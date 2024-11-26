from typing import Optional

import pytest
import asyncio
from app.redis_data_store import RedisDataStore


@pytest.fixture
async def data_store():
    """Fixture to create a fresh data store for each test"""
    store = RedisDataStore()
    yield store
    # Cleanup
    if store.delete_task and not store.delete_task.done():
        store.delete_task.cancel()
        try:
            await store.delete_task
        except asyncio.CancelledError:
            pass


class TestRedisDataStoreBasic:
    """Basic SET/GET operations without expiration"""

    async def test_basic_set_get(self, data_store):
        """Test basic SET followed by GET"""
        assert data_store.set('key', 'value')
        assert data_store.get('key') == 'value'

    async def test_get_nonexistent(self, data_store):
        """Test GET on nonexistent key"""
        assert data_store.get('nonexistent') is None

    async def test_set_overwrite(self, data_store):
        """Test SET overwrites existing value"""
        data_store.set('key', 'value1')
        data_store.set('key', 'value2')
        assert data_store.get('key') == 'value2'

    async def test_delete_existing(self, data_store):
        """Test DELETE on existing key"""
        data_store.set('key', 'value')
        assert data_store.delete('key') is True
        assert data_store.get('key') is None

    async def test_delete_nonexistent(self, data_store):
        """Test DELETE on nonexistent key"""
        assert data_store.delete('nonexistent') is False

    @pytest.mark.parametrize("value", [
        "",  # Empty string
        "Hello World",  # String with space
        "!@#$%^&*()",  # Special characters
        "🌍🌎🌏",  # Unicode/emoji
        "a" * 1024,  # Large string
        "\r\n\t",  # Control characters
    ])
    async def test_set_get_various_values(self, data_store, value):
        """Test SET/GET with various value types"""
        assert data_store.set('key', value)
        assert data_store.get('key') == value


class TestRedisDataStoreExpiration:
    """Tests for expiration functionality"""

    async def test_set_with_expiration(self, data_store):
        """Test SET with expiration"""
        data_store.set('key', 'value', px=100)
        assert data_store.get('key') == 'value'
        await asyncio.sleep(0.15)  # Wait for expiration
        assert data_store.get('key') is None

    async def test_update_expiration(self, data_store):
        """Test updating expiration time"""
        # Set with initial expiration
        data_store.set('key', 'value1', px=200)
        # Update with shorter expiration
        data_store.set('key', 'value2', px=100)

        await asyncio.sleep(0.15)  # Wait longer than shorter expiration
        assert data_store.get('key') is None

    async def test_remove_expiration(self, data_store):
        """Test removing expiration with plain SET"""
        data_store.set('key', 'value', px=100)
        data_store.set('key', 'value')  # No expiration

        await asyncio.sleep(0.15)  # Wait
        assert data_store.get('key') == 'value'  # Should still exist

    async def test_multiple_expirations(self, data_store):
        """Test multiple keys with different expiration times"""
        data_store.set('key1', 'value1', px=100)
        data_store.set('key2', 'value2', px=200)
        data_store.set('key3', 'value3', px=300)

        await asyncio.sleep(0.15)  # Wait for first expiration
        assert data_store.get('key1') is None
        assert data_store.get('key2') == 'value2'
        assert data_store.get('key3') == 'value3'

        await asyncio.sleep(0.1)  # Wait for second expiration
        assert data_store.get('key2') is None
        assert data_store.get('key3') == 'value3'


class TestRedisDataStoreBackgroundCleanup:
    """Tests for background cleanup task"""

    async def test_background_cleanup(self, data_store):
        """Test background cleanup of expired keys"""
        # Set multiple keys with expiration
        data_store.set('key1', 'value1', px=100)
        data_store.set('key2', 'value2', px=100)

        # Wait for expiration and cleanup
        await asyncio.sleep(6)

        # Verify internal state
        assert 'key1' not in data_store.data
        assert 'key2' not in data_store.data
        assert 'key1' not in data_store.expires
        assert 'key2' not in data_store.expires

    async def test_cleanup_mixed_expiration(self, data_store):
        """Test cleanup with mixed expiration and non-expiration keys"""
        # Set keys with and without expiration
        data_store.set('exp_key', 'value1', px=100)
        data_store.set('permanent_key', 'value2')

        # Wait for expiration and cleanup
        await asyncio.sleep(0.15)

        # Verify only expired key was cleaned up
        assert data_store.get('exp_key') is None
        assert data_store.get('permanent_key') == 'value2'

    async def test_cleanup_task_cancellation(self, data_store):
        """Test proper cancellation of cleanup task"""
        # Cancel the task
        data_store.delete_task.cancel()
        try:
            await data_store.delete_task
        except asyncio.CancelledError:
            pass

        assert data_store.delete_task.cancelled()


class TestRedisDataStoreConcurrency:
    """Tests for concurrent operations"""

    async def test_concurrent_set_get(self, data_store):
        """Test concurrent SET/GET operations"""

        async def set_get(key: str, value: str, px: Optional[int] = None):
            data_store.set(key, value, px=px)
            return data_store.get(key)

        # Run multiple SET/GET operations concurrently
        tasks = [
            set_get(f'key{i}', f'value{i}', 100 + i * 50)
            for i in range(5)
        ]

        results = await asyncio.gather(*tasks)

        # Verify all operations succeeded
        for i, result in enumerate(results):
            assert result == f'value{i}'

    async def test_concurrent_set_delete(self, data_store):
        """Test concurrent SET and DELETE operations"""

        async def set_delete_sequence():
            data_store.set('key', 'value1')
            await asyncio.sleep(0.01)
            data_store.delete('key')
            data_store.set('key', 'value2')
            return data_store.get('key')

        results = await asyncio.gather(
            *[set_delete_sequence() for _ in range(3)]
        )

        # Last operation should win
        assert all(result == 'value2' for result in results)
