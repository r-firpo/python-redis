from typing import Optional

import pytest
from unittest.mock import AsyncMock

from app.async_TCP_redis_server import ServerConfig, RedisServer
from app.redis_parser import RESPCommand


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


@pytest.mark.asyncio
async def test_redis_server():
    # Create mocks
    data_store = MockDataStore()
    protocol_handler = MockProtocolHandler()
    parser = AsyncMock()
    connection_manager = AsyncMock()
    config = ServerConfig()

    # Create server with mocks
    server = await RedisServer.create(
        data_store=data_store,
        protocol_handler=protocol_handler,
        parser=parser,
        connection_manager=connection_manager,
        config=config,
    )

    # Test SET command
    command = RESPCommand(command='SET', args=['key', 'value'])
    response = await server.process_command(command)
    assert response == b"+OK\r\n"
    assert data_store.get('key') == 'value'

    # Test GET command
    command = RESPCommand(command='GET', args=['key'])
    response = await server.process_command(command)
    assert response == b"$5\r\nvalue\r\n"