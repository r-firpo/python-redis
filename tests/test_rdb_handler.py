import os
import pytest
import tempfile
import time
from pathlib import Path
from app.redis_rdb_handler import RDBHandler


@pytest.fixture
def temp_dir():
    """Create a temporary directory for RDB files"""
    with tempfile.TemporaryDirectory() as tmpdirname:
        path = Path(tmpdirname)
        path.mkdir(exist_ok=True)
        yield str(path)


class TestRDBHandler:
    """Test RDB file handling functionality"""

    def test_save_and_load_basic(self, temp_dir):
        """Test basic save and load of data"""
        rdb_handler = RDBHandler(temp_dir, "test.rdb")

        # Test data
        data = {"key1": "value1", "key2": "value2"}
        expires = {}

        # Save data
        assert rdb_handler.save(data, expires) == True

        # Load data
        result = rdb_handler.load()
        assert result is not None

        loaded_data, loaded_expires = result
        assert loaded_data == data
        assert loaded_expires == expires

    def test_save_and_load_with_expiry(self, temp_dir):
        """Test save and load with expiration times"""
        rdb_handler = RDBHandler(temp_dir, "test.rdb")

        current_time = int(time.time() * 1000)
        future_time = current_time + 10000  # 10 seconds in future

        data = {"key1": "value1", "key2": "value2"}
        expires = {
            "key1": future_time,
            "key2": future_time + 1000
        }

        # Save data
        assert rdb_handler.save(data, expires) == True

        # Load data
        result = rdb_handler.load()
        assert result is not None

        loaded_data, loaded_expires = result
        assert loaded_data == data
        assert loaded_expires == expires

    def test_expired_keys_not_loaded(self, temp_dir):
        """Test that expired keys are not loaded"""
        rdb_handler = RDBHandler(temp_dir, "test.rdb")

        current_time = int(time.time() * 1000)
        past_time = current_time - 1000  # 1 second in past
        future_time = current_time + 10000  # 10 seconds in future

        data = {
            "expired_key": "value1",
            "valid_key": "value2"
        }
        expires = {
            "expired_key": past_time,
            "valid_key": future_time
        }

        # Save data
        assert rdb_handler.save(data, expires) == True

        # Load data
        result = rdb_handler.load()
        assert result is not None

        loaded_data, loaded_expires = result
        assert "expired_key" not in loaded_data
        assert "expired_key" not in loaded_expires
        assert loaded_data["valid_key"] == "value2"
        assert loaded_expires["valid_key"] == future_time

    def test_load_nonexistent_file(self, temp_dir):
        """Test loading when RDB file doesn't exist"""
        rdb_handler = RDBHandler(temp_dir, "nonexistent.rdb")
        assert rdb_handler.load() is None

    def test_save_creates_directory(self, temp_dir):
        """Test that save creates directory if it doesn't exist"""
        subdir = os.path.join(temp_dir, "subdir")
        handler = RDBHandler(subdir, "test.rdb")

        data = {"key": "value"}
        expires = {}

        assert handler.save(data, expires) == True
        assert os.path.exists(subdir)

    @pytest.mark.parametrize("test_data", [
        ({"": "empty_key"}, {}),
        ({"key": ""}, {}),
        ({"a" * 1000: "large_key"}, {}),
        ({"key": "a" * 1000}, {}),
        ({"special!@#$%": "value"}, {}),
        ({"key": "!@#$%^&*()"}, {}),
        ({"unicode🌍": "value"}, {}),
        ({"key": "value🌍"}, {})
    ])
    def test_save_load_edge_cases(self, temp_dir, test_data):
        """Test save and load with various edge cases"""
        rdb_handler = RDBHandler(temp_dir, "test.rdb")

        data, expires = test_data
        assert rdb_handler.save(data, expires) == True

        result = rdb_handler.load()
        assert result is not None

        loaded_data, loaded_expires = result
        assert loaded_data == data
        assert loaded_expires == expires

    def test_file_corruption_handling(self, temp_dir):
        """Test handling of corrupted RDB file"""
        rdb_handler = RDBHandler(temp_dir, "test.rdb")

        # Save valid data first
        data = {"key": "value"}
        assert rdb_handler.save(data, {}) == True

        # Corrupt the file
        with open(rdb_handler.full_path, 'wb') as f:
            f.write(b'CORRUPTED_DATA')

        # Attempt to load corrupted file
        assert rdb_handler.load() is None

    def test_invalid_header(self, temp_dir):
        """Test handling of invalid RDB header"""
        rdb_handler = RDBHandler(temp_dir, "test.rdb")

        # Create file with invalid header
        with open(rdb_handler.full_path, 'wb') as f:
            f.write(b'NOTREDIS0001')

        # Attempt to load file
        assert rdb_handler.load() is None