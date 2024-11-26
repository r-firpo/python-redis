import asyncio

import pytest
from unittest.mock import AsyncMock, Mock
from dataclasses import dataclass
from typing import List, Optional

from app.async_TCP_redis_server import ServerConfig, RedisServer
from app.redis_data_store import RedisDataStore

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


@pytest.fixture
async def redis_data_store(scope="function"):
    """Fixture to create a Redis server instance with mocks"""
    data_store = RedisDataStore()
    return data_store


@pytest.mark.asyncio
class TestRedisServer:
    async def test_ping_no_args(self, redis_server):
        """Test PING command with no arguments"""
        command = RESPCommand(command='PING', args=[])
        response = await redis_server.process_command(command)
        assert response == PONG_RESPONSE

    async def test_ping_with_message(self, redis_server):
        """Test PING command with a custom message"""
        message = "Hello, Redis!"
        command = RESPCommand(command='PING', args=[message])
        response = await redis_server.process_command(command)
        assert response == f"${len(message)}\r\n{message}\r\n".encode()

    async def test_ping_too_many_args(self, redis_server):
        """Test PING command with too many arguments"""
        command = RESPCommand(command='PING', args=['arg1', 'arg2'])
        response = await redis_server.process_command(command)
        assert response.startswith(b"-ERR")
        assert b"wrong number of arguments" in response

    async def test_echo_with_message(self, redis_server):
        """Test ECHO command with a message"""
        message = "Hello, Redis!"
        command = RESPCommand(command='ECHO', args=[message])
        response = await redis_server.process_command(command)
        assert response == f"${len(message)}\r\n{message}\r\n".encode()

    async def test_echo_no_args(self, redis_server):
        """Test ECHO command with no arguments"""
        command = RESPCommand(command='ECHO', args=[])
        response = await redis_server.process_command(command)
        assert response.startswith(b"-ERR")
        assert b"wrong number of arguments" in response

    async def test_echo_too_many_args(self, redis_server):
        """Test ECHO command with too many arguments"""
        command = RESPCommand(command='ECHO', args=['arg1', 'arg2'])
        response = await redis_server.process_command(command)
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
    async def test_echo_various_messages(self, redis_server, message):
        """Test ECHO command with various types of messages"""
        command = RESPCommand(command='ECHO', args=[message])
        response = await redis_server.process_command(command)
        assert response == f"${len(message)}\r\n{message}\r\n".encode()

    async def test_command_case_insensitivity(self, redis_server):
        """Test that commands are case insensitive"""
        variants = ['ping', 'PING', 'Ping', 'pInG']
        for cmd in variants:
            command = RESPCommand(command=cmd, args=[])
            response = await redis_server.process_command(command)
            assert response == PONG_RESPONSE

    async def test_concurrent_ping_requests(self, redis_server):
        """Test handling multiple PING requests concurrently"""
        commands = [
            RESPCommand(command='PING', args=[]),
            RESPCommand(command='PING', args=['msg1']),
            RESPCommand(command='PING', args=['msg2'])
        ]

        responses = await asyncio.gather(
            *[redis_server.process_command(cmd) for cmd in commands]
        )

        assert responses[0] == PONG_RESPONSE
        assert responses[1] == b"$4\r\nmsg1\r\n"
        assert responses[2] == b"$4\r\nmsg2\r\n"


@pytest.mark.asyncio
class TestRedisGetSet:
    """Test GET and SET commands with various scenarios including expiration"""

    async def test_basic_set_and_get(self, redis_server):
        """Test basic SET followed by GET"""
        # SET command
        command = RESPCommand(command='SET', args=['mykey', 'Hello'])
        response = await redis_server.process_command(command)
        assert response == b"+OK\r\n"

        # GET command
        command = RESPCommand(command='GET', args=['mykey'])
        response = await redis_server.process_command(command)
        assert response == b"$5\r\nHello\r\n"

    async def test_get_nonexistent_key(self, redis_server):
        """Test GET on a key that doesn't exist"""
        command = RESPCommand(command='GET', args=['nonexistent'])
        response = await redis_server.process_command(command)
        assert response == b"$-1\r\n"

    async def test_set_overwrites_existing(self, redis_server):
        """Test SET overwrites existing value"""
        # Set initial value
        await redis_server.process_command(RESPCommand('SET', ['key', 'value1']))

        # Overwrite with new value
        await redis_server.process_command(RESPCommand('SET', ['key', 'value2']))

        # Verify new value
        response = await redis_server.process_command(RESPCommand('GET', ['key']))
        assert response == b"$6\r\nvalue2\r\n"

    async def test_get_wrong_args(self, redis_server):
        """Test GET with wrong number of arguments"""
        # No arguments
        command = RESPCommand(command='GET', args=[])
        response = await redis_server.process_command(command)
        assert response.startswith(b"-ERR")
        assert b"wrong number of arguments" in response

        # Too many arguments
        command = RESPCommand(command='GET', args=['key1', 'key2'])
        response = await redis_server.process_command(command)
        assert response.startswith(b"-ERR")
        assert b"wrong number of arguments" in response

    async def test_set_wrong_args(self, redis_server):
        """Test SET with wrong number of arguments"""
        # No arguments
        command = RESPCommand(command='SET', args=[])
        response = await redis_server.process_command(command)
        assert response.startswith(b"-ERR")
        assert b"wrong number of arguments" in response

        # Only key, no value
        command = RESPCommand(command='SET', args=['key'])
        response = await redis_server.process_command(command)
        assert response.startswith(b"-ERR")
        assert b"wrong number of arguments" in response

    # Expiration Tests
    async def test_set_px_basic(self, redis_server):
        """Test basic SET with PX expiration"""
        # Set with 100ms expiration
        command = RESPCommand(command='SET', args=['key', 'value', 'px', '100'])
        response = await redis_server.process_command(command)
        assert response == b"+OK\r\n"

        # Verify value exists
        response = await redis_server.process_command(RESPCommand('GET', ['key']))
        assert response == b"$5\r\nvalue\r\n"
        print(redis_server.data_store.expires)

        # Wait for expiration
        await asyncio.sleep(0.15)

        # Verify value is gone
        response = await redis_server.process_command(RESPCommand('GET', ['key']))
        assert response == b"$-1\r\n"

    async def test_set_px_override(self, redis_server):
        """Test that SET PX overrides previous expiration"""
        # Set with initial 200ms expiration
        await redis_server.process_command(
            RESPCommand('SET', ['key', 'value1', 'px', '200'])
        )

        # Immediately override with 100ms expiration
        await redis_server.process_command(
            RESPCommand('SET', ['key', 'value2', 'px', '100'])
        )

        # Wait 150ms - should be expired
        await asyncio.sleep(0.15)

        response = await redis_server.process_command(RESPCommand('GET', ['key']))
        assert response == b"$-1\r\n"

    async def test_set_removes_expiration(self, redis_server):
        """Test that SET without PX removes expiration"""
        # Set with expiration
        await redis_server.process_command(
            RESPCommand('SET', ['key', 'value1', 'px', '100'])
        )

        # Override without expiration
        await redis_server.process_command(
            RESPCommand('SET', ['key', 'value2'])
        )

        # Wait 150ms
        await asyncio.sleep(0.15)

        # Key should still exist
        response = await redis_server.process_command(RESPCommand('GET', ['key']))
        assert response == b"$6\r\nvalue2\r\n"

    async def test_set_px_edge_cases(self, redis_server):
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
            response = await redis_server.process_command(command)
            assert response.startswith(expected_response)

    @pytest.mark.parametrize("value", [
        "",  # Empty string
        "Hello World",  # String with space
        "!@#$%^&*()",  # Special characters
        "🌍🌎🌏",  # Unicode/emoji
        "a" * 1024,  # Large string
        "\r\n\t",  # Control characters
    ])
    async def test_set_get_various_values(self, redis_server, value):
        """Test SET/GET with various types of values"""
        # SET the value
        await redis_server.process_command(
            RESPCommand('SET', ['key', value])
        )

        # GET and verify
        response = await redis_server.process_command(
            RESPCommand('GET', ['key'])
        )
        expected = f"${len(value)}\r\n{value}\r\n".encode()
        assert response == expected

    async def test_concurrent_set_get(self, redis_server):
        """Test concurrent SET/GET operations"""

        async def set_get_sequence(key: str, value: str, px: int):
            await redis_server.process_command(
                RESPCommand('SET', [key, value, 'px', str(px)])
            )
            return await redis_server.process_command(
                RESPCommand('GET', [key])
            )

        # Run multiple SET/GET operations concurrently
        tasks = [
            set_get_sequence(f'key{i}', f'value{i}', 100 + i * 50)
            for i in range(5)
        ]

        responses = await asyncio.gather(*tasks)

        # Verify all initial GETs worked
        for i, response in enumerate(responses):
            expected = f"$6\r\nvalue{i}\r\n".encode()
            assert response == expected

        # Wait for some keys to expire
        await asyncio.sleep(0.2)

        # Check expiration occurred as expected
        for i in range(5):
            response = await redis_server.process_command(
                RESPCommand('GET', [f'key{i}'])
            )
            # Earlier keys should be expired
            if i < 2:  # first two keys had shorter expiration
                assert response == b"$-1\r\n"
            else:
                assert response == f"$6\r\nvalue{i}\r\n".encode()

    async def test_set_get_after_expiry_race(self, redis_server):
        """Test race condition between SET and expiry"""
        # Set key with very short expiration
        await redis_server.process_command(
            RESPCommand('SET', ['key', 'value', 'px', '1'])
        )

        # Wait just long enough for potential expiry
        await asyncio.sleep(0.002)

        # Quickly SET new value before GET
        await redis_server.process_command(
            RESPCommand('SET', ['key', 'new_value'])
        )

        # GET should return new value regardless of previous expiry
        response = await redis_server.process_command(
            RESPCommand('GET', ['key'])
        )
        assert response == b"$9\r\nnew_value\r\n"