import asyncio
import tempfile
from pathlib import Path
from typing import AsyncGenerator

import pytest

from app.async_TCP_redis_server import RedisServer
from app.redis_data_store import RedisDataStore
from app.utils.config import ServerConfig


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test data"""
    with tempfile.TemporaryDirectory() as tmpdirname:
        path = Path(tmpdirname)
        path.mkdir(exist_ok=True)
        yield str(path)

@pytest.fixture
def test_config(temp_dir):
    """Create a test configuration with temporary directory"""
    return ServerConfig(
        dir=temp_dir,
        dbfilename="test.rdb",
        host="localhost",
        port=6379
    )

@pytest.fixture
async def data_store(test_config) -> AsyncGenerator[RedisDataStore, None]:
    """Fixture to create a fresh data store for each test"""
    store = RedisDataStore(test_config)
    yield store

    # Cleanup tasks
    tasks_to_cancel = []
    if store.delete_task and not store.delete_task.done():
        tasks_to_cancel.append(store.delete_task)
    if store.save_task and not store.save_task.done():
        tasks_to_cancel.append(store.save_task)

    # Cancel all tasks
    for task in tasks_to_cancel:
        task.cancel()

    # Wait for all tasks to complete
    if tasks_to_cancel:
        try:
            await asyncio.gather(*tasks_to_cancel, return_exceptions=True)
        except asyncio.CancelledError:
            pass

@pytest.fixture
async def redis_server(test_config, data_store) -> AsyncGenerator[RedisServer, None]:
    """Fixture to create a Redis server instance"""
    # Create server instance with the data_store fixture
    server = await RedisServer.create(
        config=test_config,
        data_store=data_store  # Use the fixture as an argument
    )

    # Use async context manager to ensure proper setup
    async with server as srv:
        yield srv

    # Additional cleanup if needed
    if hasattr(server, 'monitor_task') and server.monitor_task:
        if not server.monitor_task.done():
            server.monitor_task.cancel()
            try:
                await server.monitor_task
            except asyncio.CancelledError:
                pass