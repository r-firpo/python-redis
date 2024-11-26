import asyncio

import pytest
from unittest.mock import AsyncMock, Mock
from dataclasses import dataclass
from typing import List, Optional

from app.async_TCP_redis_server import ServerConfig, RedisServer

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

    def set(self, key: str, value: str, ex: Optional[int] = None) -> bool:
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
async def redis_server(scope="function"):
    """Fixture to create a Redis server instance with mocks"""
    data_store = MockDataStore()
    protocol_handler = MockProtocolHandler()
    parser = AsyncMock()
    connection_manager = AsyncMock()
    config = ServerConfig()

    print("Creating server...")
    server = await RedisServer.create(
        data_store=data_store,
        protocol_handler=protocol_handler,
        parser=parser,
        connection_manager=connection_manager,
        config=config,
    )
    print(f"Server created: {type(server)}")
    return server


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