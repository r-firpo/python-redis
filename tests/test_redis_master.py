# test_redis_master.py

import pytest
from unittest.mock import AsyncMock, Mock
from app.redis_master_handler import RedisMaster
from fixtures import master, mock_writer, test_config, data_store, temp_dir


@pytest.mark.asyncio
class TestRedisMaster:
    """Test RedisMaster functionality"""

    async def test_replica_management(self, master, mock_writer):
        """Test adding and removing replicas"""
        # Add replica
        await master.add_replica(mock_writer)
        assert len(master.replicas) == 1
        assert master.config.connected_slaves == 1

        # Remove replica
        await master.remove_replica(mock_writer)
        assert len(master.replicas) == 0
        assert master.config.connected_slaves == 0

    async def test_command_propagation(self, master, mock_writer):
        """Test command propagation to replicas"""
        await master.add_replica(mock_writer)

        # Test command
        command = b"*3\r\n$3\r\nSET\r\n$4\r\nkey1\r\n$6\r\nvalue1\r\n"

        # Propagate command
        await master.propagate_write_command(command)

        # Verify command was sent to replica
        mock_writer.write.assert_called_once_with(command)
        assert len(master.replication_backlog) > 0

    async def test_backlog_size_limit(self, master):
        """Test replication backlog size limiting"""
        # Create data larger than backlog size
        large_data = b"x" * (master.max_backlog_size + 1000)

        # Add to backlog
        master.append_to_backlog(large_data)

        # Verify backlog size is limited
        assert len(master.replication_backlog) <= master.max_backlog_size
        assert master.backlog_offset > 0

    async def test_wait_for_replicas(self, master, mock_writer):
        """Test waiting for replica acknowledgments"""
        await master.add_replica(mock_writer)
        replica_id = master._get_replica_key(mock_writer)

        # Set initial offset
        current_offset = 100

        # Wait for replicas (should timeout quickly)
        count = await master.wait_for_replicas(1, current_offset, 0.1)
        assert count == 0

        # Update replica offset
        master.replica_offsets[replica_id] = current_offset + 50

        # Wait again (should succeed)
        count = await master.wait_for_replicas(1, current_offset, 0.1)
        assert count == 1