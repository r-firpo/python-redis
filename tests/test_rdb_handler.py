import os
import pytest
import tempfile
import time
from pathlib import Path
from datetime import datetime
from app.redis_rdb_handler import RDBHandler
from fixtures import temp_dir


class TestRDBHandler:
    """Test RDB file handling functionality"""

    def test_save_and_load_basic(self, temp_dir):
        """Test saving and loading basic key-value pairs without expiry"""
        handler = RDBHandler(temp_dir, "test.rdb")

        # Test data
        data = {"key1": "value1", "key2": "value2"}
        expires = {}  # No expiries

        # Save and load
        assert handler.save(data, expires)
        loaded_data, loaded_expires = handler.load()

        assert loaded_data == data
        assert loaded_expires == {}

    def test_save_and_load_with_expiry(self, temp_dir):
        """Test saving and loading key-value pairs with expiry"""
        handler = RDBHandler(temp_dir, "test.rdb")

        # Test data with expiry
        current_time = int(time.time())
        data = {"key1": "value1", "key2": "value2"}
        expires = {"key1": current_time + 5000}  # Expires in 5 seconds

        # Save and load
        assert handler.save(data, expires)
        loaded_data, loaded_expires = handler.load()

        # Validate loaded data
        assert loaded_data == data
        assert "key1" in loaded_expires
        assert loaded_expires["key1"] == expires["key1"]
        assert "key2" not in loaded_expires

    def test_load_expired_keys(self, temp_dir):
        """Test that expired keys are not loaded"""
        handler = RDBHandler(temp_dir, "test.rdb")

        # Test data with expiry
        current_time = int(time.time())
        data = {"key1": "value1", "key2": "value2"}
        expires = {"key1": current_time - 5000}  # Expired 5000 seconds ago

        # Save and load
        assert handler.save(data, expires)
        loaded_data, loaded_expires = handler.load()

        # Validate loaded data
        assert "key1" not in loaded_data
        assert "key2" in loaded_data
        assert loaded_expires == {}

    def test_save_and_load_mixed(self, temp_dir):
        """Test saving and loading a mix of expiring and non-expiring keys"""
        handler = RDBHandler(temp_dir, "test.rdb")

        # Test data
        current_time = int(time.time())
        data = {"key1": "value1", "key2": "value2", "key3": "value3"}
        expires = {"key1": current_time + 100}  # key1 expires in 100 seconds

        # Save and load
        assert handler.save(data, expires)
        loaded_data, loaded_expires = handler.load()

        # Validate loaded data
        assert "key1" in loaded_data
        assert "key2" in loaded_data
        assert "key3" in loaded_data
        assert "key1" in loaded_expires
        assert "key2" not in loaded_expires
        assert "key3" not in loaded_expires

    def test_empty_save_and_load(self, temp_dir):
        """Test saving and loading when no data is present"""
        handler = RDBHandler(temp_dir, "test.rdb")

        # Test empty data
        data = {}
        expires = {}

        # Save and load
        assert handler.save(data, expires)
        loaded_data, loaded_expires = handler.load()

        # Validate
        assert loaded_data == {}
        assert loaded_expires == {}