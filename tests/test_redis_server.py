import asyncio
import tempfile
from pathlib import Path

import pytest
from unittest.mock import AsyncMock, Mock
from dataclasses import dataclass
from typing import List, Optional, AsyncGenerator

from app.async_TCP_redis_server import ServerConfig, RedisServer
from app.redis_data_store import RedisDataStore
from fixtures import test_config, data_store, temp_dir, redis_server, mock_writer


# Mock Response Constants
PONG_RESPONSE = b"+PONG\r\n"
OK_RESPONSE = b"+OK\r\n"
NULL_RESPONSE = b"$-1\r\n"


@dataclass
class RESPCommand:
    command: str
    args: List[str]


class MockDataStore:
    def __init__(self):
        self.data = {}

    def set(self, key: str, value: str, px: Optional[int] = None) -> bool:
        self.data[key] = value
        return True

    def get(self, key: str) -> Optional[str]:
        return self.data.get(key)

    def delete(self, key: str) -> bool:
        return bool(self.data.pop(key, None))


class MockProtocolHandler:
    def encode_simple_string(self, s: str) -> bytes:
        return f"+{s}\r\n".encode()

    def encode_error(self, err: str) -> bytes:
        return f"-ERR {err}\r\n".encode()

    def encode_integer(self, i: int) -> bytes:
        return f":{i}\r\n".encode()

    def encode_bulk_string(self, s: Optional[str]) -> bytes:
        if s is None:
            return b"$-1\r\n"
        return f"${len(s)}\r\n{s}\r\n".encode()
#
# @pytest.fixture
# def temp_dir():
#     """Create a temporary directory for test data"""
#     with tempfile.TemporaryDirectory() as tmpdirname:
#         path = Path(tmpdirname)
#         path.mkdir(exist_ok=True)
#         yield str(path)
#
# @pytest.fixture
# def test_config(temp_dir):
#     """Create a test configuration with temporary directory"""
#     return ServerConfig(
#         dir=temp_dir,
#         dbfilename="test.rdb",
#         host="localhost",
#         port=6379
#     )
#
# @pytest.fixture
# async def data_store(test_config) -> AsyncGenerator[RedisDataStore, None]:
#     """Fixture to create a fresh data store for each test"""
#     store = RedisDataStore(test_config)
#     yield store
#
#     # Cleanup tasks
#     tasks_to_cancel = []
#     if store.delete_task and not store.delete_task.done():
#         tasks_to_cancel.append(store.delete_task)
#     if store.save_task and not store.save_task.done():
#         tasks_to_cancel.append(store.save_task)
#
#     # Cancel all tasks
#     for task in tasks_to_cancel:
#         task.cancel()
#
#     # Wait for all tasks to complete
#     if tasks_to_cancel:
#         try:
#             await asyncio.gather(*tasks_to_cancel, return_exceptions=True)
#         except asyncio.CancelledError:
#             pass
# @pytest.fixture
# async def redis_server(test_config) -> AsyncGenerator[RedisServer, None]:
#     """Fixture to create a Redis server instance"""
#     # Create server instance
#     server = await RedisServer.create(config=test_config, data_store=data_store)
#
#     # Use async context manager to ensure proper setup
#     async with server as srv:
#         yield srv
#
#     # Additional cleanup if needed
#     if hasattr(server, 'monitor_task') and server.monitor_task:
#         if not server.monitor_task.done():
#             server.monitor_task.cancel()
#             try:
#                 await server.monitor_task
#             except asyncio.CancelledError:
#                 pass


@pytest.mark.asyncio
class TestRedisServer:
    async def test_ping_no_args(serf, redis_server, mock_writer):
        """Test PING command with no arguments"""
        command = RESPCommand(command='PING', args=[])
        response = await redis_server.process_command(command, mock_writer)
        assert response == PONG_RESPONSE

    async def test_ping_with_message(serf, redis_server, mock_writer):
        """Test PING command with a custom message"""
        message = "Hello, Redis!"
        command = RESPCommand(command='PING', args=[message])
        response = await redis_server.process_command(command, mock_writer)
        assert response == f"${len(message)}\r\n{message}\r\n".encode()

    async def test_ping_too_many_args(serf, redis_server, mock_writer):
        """Test PING command with too many arguments"""
        command = RESPCommand(command='PING', args=['arg1', 'arg2'])
        response = await redis_server.process_command(command, mock_writer)
        assert response.startswith(b"-ERR")
        assert b"wrong number of arguments" in response

    async def test_echo_with_message(serf, redis_server, mock_writer):
        """Test ECHO command with a message"""
        message = "Hello, Redis!"
        command = RESPCommand(command='ECHO', args=[message])
        response = await redis_server.process_command(command, mock_writer)
        assert response == f"${len(message)}\r\n{message}\r\n".encode()

    async def test_echo_no_args(serf, redis_server, mock_writer):
        """Test ECHO command with no arguments"""
        command = RESPCommand(command='ECHO', args=[])
        response = await redis_server.process_command(command, mock_writer)
        assert response.startswith(b"-ERR")
        assert b"wrong number of arguments" in response

    async def test_echo_too_many_args(serf, redis_server, mock_writer):
        """Test ECHO command with too many arguments"""
        command = RESPCommand(command='ECHO', args=['arg1', 'arg2'])
        response = await redis_server.process_command(command, mock_writer)
        assert response.startswith(b"-ERR")
        assert b"wrong number of arguments" in response

    @pytest.mark.parametrize("message", [
        "",  # Empty string
        "Hello",  # Simple string
        "Hello World",  # String with space
        "🌍🌎🌏",  # Unicode/emoji
        "!@#$%^&*()",  # Special characters
        "a" * 1000,  # Long string
    ])
    async def test_echo_various_messages(serf, redis_server, mock_writer, message):
        """Test ECHO command with various types of messages"""
        command = RESPCommand(command='ECHO', args=[message])
        response = await redis_server.process_command(command, mock_writer)
        assert response == f"${len(message)}\r\n{message}\r\n".encode()

    async def test_command_case_insensitivity(serf, redis_server, mock_writer):
        """Test that commands are case insensitive"""
        variants = ['ping', 'PING', 'Ping', 'pInG']
        for cmd in variants:
            command = RESPCommand(command=cmd, args=[])
            response = await redis_server.process_command(command, mock_writer)
            assert response == PONG_RESPONSE

    async def test_concurrent_ping_requests(serf, redis_server, mock_writer):
        """Test handling multiple PING requests concurrently"""
        commands = [
            RESPCommand(command='PING', args=[]),
            RESPCommand(command='PING', args=['msg1']),
            RESPCommand(command='PING', args=['msg2'])
        ]

        responses = await asyncio.gather(
            *[redis_server.process_command(cmd, mock_writer) for cmd in commands]
        )

        assert responses[0] == PONG_RESPONSE
        assert responses[1] == b"$4\r\nmsg1\r\n"
        assert responses[2] == b"$4\r\nmsg2\r\n"


@pytest.mark.asyncio
class TestRedisGetSet:
    """Test GET and SET commands with various scenarios including expiration"""

    async def test_basic_set_and_get(serf, redis_server, mock_writer):
        """Test basic SET followed by GET"""
        # SET command
        command = RESPCommand(command='SET', args=['mykey', 'Hello'])
        response = await redis_server.process_command(command, mock_writer)
        assert response == b"+OK\r\n"

        # GET command
        command = RESPCommand(command='GET', args=['mykey'])
        response = await redis_server.process_command(command, mock_writer)
        assert response == b"$5\r\nHello\r\n"

    async def test_get_nonexistent_key(serf, redis_server, mock_writer):
        """Test GET on a key that doesn't exist"""
        command = RESPCommand(command='GET', args=['nonexistent'])
        response = await redis_server.process_command(command, mock_writer)
        assert response == b"$-1\r\n"

    async def test_set_overwrites_existing(serf, redis_server, mock_writer):
        """Test SET overwrites existing value"""
        # Set initial value
        await redis_server.process_command(RESPCommand('SET', ['key', 'value1']), mock_writer)

        # Overwrite with new value
        await redis_server.process_command(RESPCommand('SET', ['key', 'value2']), mock_writer)

        # Verify new value
        response = await redis_server.process_command(RESPCommand('GET', ['key']), mock_writer)
        assert response == b"$6\r\nvalue2\r\n"

    async def test_get_wrong_args(serf, redis_server, mock_writer):
        """Test GET with wrong number of arguments"""
        # No arguments
        command = RESPCommand(command='GET', args=[])
        response = await redis_server.process_command(command, mock_writer)
        assert response.startswith(b"-ERR")
        assert b"wrong number of arguments" in response

        # Too many arguments
        command = RESPCommand(command='GET', args=['key1', 'key2'])
        response = await redis_server.process_command(command, mock_writer)
        assert response.startswith(b"-ERR")
        assert b"wrong number of arguments" in response

    async def test_set_wrong_args(serf, redis_server, mock_writer):
        """Test SET with wrong number of arguments"""
        # No arguments
        command = RESPCommand(command='SET', args=[])
        response = await redis_server.process_command(command, mock_writer)
        assert response.startswith(b"-ERR")
        assert b"wrong number of arguments" in response

        # Only key, no value
        command = RESPCommand(command='SET', args=['key'])
        response = await redis_server.process_command(command, mock_writer)
        assert response.startswith(b"-ERR")
        assert b"wrong number of arguments" in response

    # Expiration Tests
    async def test_set_px_basic(serf, redis_server, mock_writer):
        """Test basic SET with PX expiration"""
        # Set with 100ms expiration
        command = RESPCommand(command='SET', args=['key', 'value', 'px', '100'])
        response = await redis_server.process_command(command, mock_writer)
        assert response == b"+OK\r\n"

        # Verify value exists
        response = await redis_server.process_command(RESPCommand('GET', ['key']), mock_writer)
        assert response == b"$5\r\nvalue\r\n"
        print(redis_server.data_store.expires)

        # Wait for expiration
        await asyncio.sleep(0.15)

        # Verify value is gone
        response = await redis_server.process_command(RESPCommand('GET', ['key']), mock_writer)
        assert response == b"$-1\r\n"

    async def test_set_px_override(serf, redis_server, mock_writer):
        """Test that SET PX overrides previous expiration"""
        # Set with initial 200ms expiration
        await redis_server.process_command(
            RESPCommand('SET', ['key', 'value1', 'px', '200']), mock_writer
        )

        # Immediately override with 100ms expiration
        await redis_server.process_command(
            RESPCommand('SET', ['key', 'value2', 'px', '100']), mock_writer
        )

        # Wait 150ms - should be expired
        await asyncio.sleep(0.15)

        response = await redis_server.process_command(RESPCommand('GET', ['key']), mock_writer)
        assert response == b"$-1\r\n"

    async def test_set_removes_expiration(serf, redis_server, mock_writer):
        """Test that SET without PX removes expiration"""
        # Set with expiration
        await redis_server.process_command(
            RESPCommand('SET', ['key', 'value1', 'px', '100']), mock_writer
        )

        # Override without expiration
        await redis_server.process_command(
            RESPCommand('SET', ['key', 'value2']), mock_writer
        )

        # Wait 150ms
        await asyncio.sleep(0.15)

        # Key should still exist
        response = await redis_server.process_command(RESPCommand('GET', ['key']), mock_writer)
        assert response == b"$6\r\nvalue2\r\n"

    async def test_set_px_edge_cases(serf, redis_server, mock_writer):
        """Test edge cases for SET PX"""
        test_cases = [
            ('0', b"-ERR"),  # Zero expiration
            ('-100', b"-ERR"),  # Negative expiration
            ('abc', b"-ERR"),  # Non-numeric
            ('3.14', b"-ERR"),  # Float
            ('18446744073709551616', b"-ERR"),  # Too large
        ]

        for px_value, expected_response in test_cases:
            command = RESPCommand('SET', ['key', 'value', 'px', px_value])
            response = await redis_server.process_command(command, mock_writer)
            assert response.startswith(expected_response)

    @pytest.mark.parametrize("value", [
        "",  # Empty string
        "Hello World",  # String with space
        "!@#$%^&*()",  # Special characters
        "🌍🌎🌏",  # Unicode/emoji
        "a" * 1024,  # Large string
        "\r\n\t",  # Control characters
    ])
    async def test_set_get_various_values(serf, redis_server, mock_writer, value):
        """Test SET/GET with various types of values"""
        # SET the value
        await redis_server.process_command(
            RESPCommand('SET', ['key', value]), mock_writer
        )

        # GET and verify
        response = await redis_server.process_command(
            RESPCommand('GET', ['key']), mock_writer
        )
        expected = f"${len(value)}\r\n{value}\r\n".encode()
        assert response == expected

    async def test_concurrent_set_get(serf, redis_server, mock_writer):
        """Test concurrent SET/GET operations"""
        async def set_get_sequence(key: str, value: str, px: int):
            await redis_server.process_command(
                RESPCommand('SET', [key, value, 'px', str(px)]), mock_writer
            )
            return await redis_server.process_command(
                RESPCommand('GET', [key]), mock_writer
            )
        # Use longer expiration times to avoid timing issues
        base_expiry = 500  # 500ms base
        interval = 200  # 200ms interval

        # Run multiple SET/GET operations concurrently
        tasks = [
            set_get_sequence(f'key{i}', f'value{i}', base_expiry + i * interval)
            for i in range(5)
        ]

        # This means:
        # key0: px = 500ms
        # key1: px = 700ms
        # key2: px = 900ms
        # key3: px = 1100ms
        # key4: px = 1300ms

        responses = await asyncio.gather(*tasks)
        # Verify all initial GETs worked
        for i, response in enumerate(responses):
            expected = f"$6\r\nvalue{i}\r\n".encode()
            assert response == expected

        # Wait for first two keys to expire
        await asyncio.sleep(0.6)  # 600ms - should expire key0 only

        # Check expiration occurred as expected
        for i in range(5):
            response = await redis_server.process_command(
                RESPCommand('GET', [f'key{i}']), mock_writer
            )
            if i == 0:  # Only first key should be expired
                assert response == b"$-1\r\n", f"key{i} should be expired"
            else:
                expected = f"$6\r\nvalue{i}\r\n".encode()
                assert response == expected, f"key{i} should still be valid"

    async def test_set_get_after_expiry_race(serf, redis_server, mock_writer):
        """Test race condition between SET and expiry"""
        # Set key with very short expiration
        await redis_server.process_command(
            RESPCommand('SET', ['key', 'value', 'px', '1']), mock_writer
        )

        # Wait just long enough for potential expiry
        await asyncio.sleep(0.002)

        # Quickly SET new value before GET
        await redis_server.process_command(
            RESPCommand('SET', ['key', 'new_value']), mock_writer
        )

        # GET should return new value regardless of previous expiry
        response = await redis_server.process_command(
            RESPCommand('GET', ['key']), mock_writer
        )
        assert response == b"$9\r\nnew_value\r\n"


@pytest.mark.asyncio
class TestConfigGet:
    """Test CONFIG GET command functionality"""

    @pytest.fixture
    async def redis_server(self):
        """Create Redis server with custom config"""
        config = ServerConfig(
            dir='/tmp/redis-files',
            dbfilename='dump.rdb'
        )
        server = await RedisServer.create(config=config)
        return server

    async def test_config_get_dir(serf, redis_server, mock_writer):
        """Test CONFIG GET dir"""
        command = RESPCommand(
            command='CONFIG',
            args=['GET', 'dir']
        )
        response = await redis_server.process_command(command, mock_writer)
        expected = (
            b"*2\r\n"  # Array of 2 elements
            b"$3\r\n"  # First element length (dir)
            b"dir\r\n"  # First element value
            b"$16\r\n"  # Second element length (/tmp/redis-files)
            b"/tmp/redis-files\r\n"  # Second element value
        )
        assert response == expected

    async def test_config_get_dbfilename(serf, redis_server, mock_writer):
        """Test CONFIG GET dbfilename"""
        command = RESPCommand(
            command='CONFIG',
            args=['GET', 'dbfilename']
        )
        response = await redis_server.process_command(command, mock_writer)
        expected = (
            b"*2\r\n"  # Array of 2 elements
            b"$10\r\n"  # First element length (dbfilename is 10 chars)
            b"dbfilename\r\n"  # First element value
            b"$8\r\n"  # Second element length (dump.rdb)
            b"dump.rdb\r\n"  # Second element value
        )
        assert response == expected

    async def test_config_get_unknown_param(serf, redis_server, mock_writer):
        """Test CONFIG GET with unknown parameter"""
        command = RESPCommand(
            command='CONFIG',
            args=['GET', 'unknown']
        )
        response = await redis_server.process_command(command, mock_writer)
        expected = (
            b"*2\r\n"  # Array of 2 elements
            b"$7\r\n"  # First element length (unknown)
            b"unknown\r\n"  # First element value
            b"$-1\r\n"  # Second element (null bulk string)
        )
        assert response == expected

    async def test_config_get_case_insensitive(serf, redis_server, mock_writer):
        """Test CONFIG GET is case insensitive"""
        variants = ['DIR', 'dir', 'Dir', 'dIr']
        for variant in variants:
            command = RESPCommand(
                command='CONFIG',
                args=['GET', variant]
            )
            response = await redis_server.process_command(command, mock_writer)
            expected = (
                b"*2\r\n"
                b"$3\r\n"
                b"dir\r\n"
                b"$16\r\n"
                b"/tmp/redis-files\r\n"
            )
            assert response == expected

    async def test_config_wrong_subcommand(serf, redis_server, mock_writer):
        """Test CONFIG with wrong subcommand"""
        command = RESPCommand(
            command='CONFIG',
            args=['INVALID', 'dir']
        )
        response = await redis_server.process_command(command, mock_writer)
        assert response.startswith(b"-ERR unknown CONFIG subcommand")

    async def test_config_missing_args(serf, redis_server, mock_writer):
        """Test CONFIG with missing arguments"""
        command = RESPCommand(
            command='CONFIG',
            args=[]
        )
        response = await redis_server.process_command(command, mock_writer)
        assert response.startswith(b"-ERR wrong number of arguments")


@pytest.mark.asyncio
class TestRedisKeys:
    """Test KEYS command functionality"""

    async def test_keys_empty_db(serf, redis_server, mock_writer):
        """Test KEYS when database is empty"""
        command = RESPCommand(command='KEYS', args=['*'])
        response = await redis_server.process_command(command, mock_writer)
        expected = b"*0\r\n"  # Empty array
        assert response == expected

    async def test_keys_multiple_entries(serf, redis_server, mock_writer):
        """Test KEYS with multiple entries"""
        # Add some test data
        test_data = [
            ('key1', 'value1'),
            ('key2', 'value2'),
            ('key3', 'value3')
        ]

        for key, value in test_data:
            await redis_server.process_command(
                RESPCommand('SET', [key, value]), mock_writer
            )

        # Get all keys
        command = RESPCommand(command='KEYS', args=['*'])
        response = await redis_server.process_command(command, mock_writer)

        # Expected response with sorted keys
        expected = (
            b"*3\r\n"  # Array of 3 elements
            b"$4\r\n"  # Length of "key1"
            b"key1\r\n"
            b"$4\r\n"  # Length of "key2"
            b"key2\r\n"
            b"$4\r\n"  # Length of "key3"
            b"key3\r\n"
        )
        assert response == expected

    async def test_keys_with_expired(serf, redis_server, mock_writer):
        """Test KEYS with some expired keys"""
        # Set keys with different expirations
        await redis_server.process_command(
            RESPCommand('SET', ['key1', 'value1', 'px', '100']), mock_writer  # Will expire
        )
        await redis_server.process_command(
            RESPCommand('SET', ['key2', 'value2']), mock_writer  # No expiration
        )
        await redis_server.process_command(
            RESPCommand('SET', ['key3', 'value3', 'px', '10000']) , mock_writer # Won't expire yet
        )

        # Wait for first key to expire
        await asyncio.sleep(0.2)

        # Get all keys
        command = RESPCommand(command='KEYS', args=['*'])
        response = await redis_server.process_command(command, mock_writer)

        # Expected response (key1 should be gone)
        expected = (
            b"*2\r\n"  # Array of 2 elements
            b"$4\r\n"
            b"key2\r\n"
            b"$4\r\n"
            b"key3\r\n"
        )
        assert response == expected

    async def test_keys_wrong_args(serf, redis_server, mock_writer):
        """Test KEYS with wrong number of arguments"""
        # No arguments
        command = RESPCommand(command='KEYS', args=[])
        response = await redis_server.process_command(command, mock_writer)
        assert response.startswith(b"-ERR wrong number of arguments")

        # Too many arguments
        command = RESPCommand(command='KEYS', args=['*', 'extra'])
        response = await redis_server.process_command(command, mock_writer)
        assert response.startswith(b"-ERR wrong number of arguments")

    async def test_keys_unsupported_pattern(serf, redis_server, mock_writer):
        """Test KEYS with unsupported pattern"""
        command = RESPCommand(command='KEYS', args=['key*'])
        response = await redis_server.process_command(command, mock_writer)
        assert response.startswith(b"-ERR only \"*\" pattern is supported")

    async def test_keys_after_delete(serf, redis_server, mock_writer):
        """Test KEYS after deleting some keys"""
        # Set multiple keys
        await redis_server.process_command(RESPCommand('SET', ['key1', 'value1']), mock_writer)
        await redis_server.process_command(RESPCommand('SET', ['key2', 'value2']), mock_writer)
        await redis_server.process_command(RESPCommand('SET', ['key3', 'value3']), mock_writer)

        # Delete one key
        await redis_server.process_command(RESPCommand('DEL', ['key2']), mock_writer)

        # Get all keys
        command = RESPCommand(command='KEYS', args=['*'])
        response = await redis_server.process_command(command, mock_writer)

        # Expected response (key2 should be gone)
        expected = (
            b"*2\r\n"
            b"$4\r\n"
            b"key1\r\n"
            b"$4\r\n"
            b"key3\r\n"
        )
        assert response == expected


@pytest.mark.asyncio
class TestReplicationCommands:
    """Test replication-related Redis commands"""

    async def test_wait_command_basic(self, redis_server, mock_writer):
        """Test basic WAIT command functionality"""
        # Add a replica
        await redis_server.master.add_replica(mock_writer)

        # Calculate command bytes for SET command
        set_cmd = RESPCommand('SET', ['key', 'value'])
        await redis_server.process_command(set_cmd, mock_writer)

        # Get the current offset after SET command
        current_offset = redis_server.master.backlog_offset + len(redis_server.master.replication_backlog)

        # Test WAIT command
        command = RESPCommand('WAIT', ['1', '1000'])  # Wait for 1 replica, 1000ms timeout
        response = await redis_server.process_command(command, mock_writer)

        # Should initially return 0 as replica hasn't acknowledged
        assert response == b":0\r\n"

        # Simulate replica acknowledgment with the correct offset
        replica_id = f"{mock_writer.get_extra_info('peername')[0]}:{mock_writer.get_extra_info('peername')[1]}"
        await redis_server.master.process_ack(replica_id, str(current_offset))

        # Try WAIT again
        response = await redis_server.process_command(command, mock_writer)
        assert response == b":1\r\n"

    async def test_wait_command_timeout(self, redis_server, mock_writer):
        """Test WAIT command with timeout"""
        await redis_server.master.add_replica(mock_writer)

        # Set data and immediately try to wait with short timeout
        await redis_server.process_command(RESPCommand('SET', ['key', 'value']), mock_writer)
        command = RESPCommand('WAIT', ['1', '100'])  # 100ms timeout

        response = await redis_server.process_command(command, mock_writer)
        assert response == b":0\r\n"  # Should timeout and return 0

    async def test_replconf_ack_handling(self, redis_server, mock_writer):
        """Test handling of REPLCONF ACK command"""
        await redis_server.master.add_replica(mock_writer)

        # Simulate REPLCONF ACK command
        command = RESPCommand('REPLCONF', ['ACK', '100'])
        response = await redis_server.process_command(command, mock_writer)
        assert response == b"+OK\r\n"

        # Verify offset was updated
        replica_id = f"{mock_writer.get_extra_info('peername')[0]}:{mock_writer.get_extra_info('peername')[1]}"
        assert redis_server.master.replica_offsets[replica_id] == 100

    async def test_invalid_replconf_commands(self, redis_server, mock_writer):
        """Test invalid REPLCONF command handling"""
        test_cases = [
            (RESPCommand('REPLCONF', []), "wrong number of arguments for REPLCONF"),
            (RESPCommand('REPLCONF', ['INVALID']), "unknown REPLCONF subcommand invalid"),
            (RESPCommand('REPLCONF', ['ACK']), "wrong number of arguments for REPLCONF ACK"),
            (RESPCommand('REPLCONF', ['ACK', 'invalid']), None),  # Should handle invalid offset gracefully
        ]

        for command, error_message in test_cases:
            response = await redis_server.process_command(command, mock_writer)
            if error_message:
                assert response.startswith(b"-ERR")
                assert error_message.encode() in response