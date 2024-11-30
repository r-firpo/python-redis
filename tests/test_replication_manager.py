# test_replication_manager.py

import os
import pytest
from unittest.mock import AsyncMock, Mock, patch
from app.replication_manager import ReplicationManager
from app.async_TCP_redis_server import RESPCommand
from fixtures import test_config, data_store, temp_dir


@pytest.mark.asyncio
class TestReplicationManager:
    """Test ReplicationManager functionality"""

    @pytest.fixture
    async def replication_manager(self, test_config, data_store):
        """Create ReplicationManager instance"""
        config = test_config
        config.role = 'slave'
        config.master_host = 'localhost'
        config.master_port = 6379
        manager = ReplicationManager(config, data_store)
        yield manager

        # Clean up any background tasks
        if hasattr(manager, 'replication_task') and manager.replication_task:
            manager.replication_task.cancel()
            try:
                await manager.replication_task
            except Exception:
                pass

    async def test_initial_connection_handshake(self, replication_manager):
        """Test initial connection handshake with master"""
        with patch('asyncio.open_connection') as mock_open_connection:
            # Setup mock connection
            mock_reader = AsyncMock()
            mock_writer = AsyncMock()
            mock_open_connection.return_value = (mock_reader, mock_writer)

            # Setup response sequence
            mock_reader.readline.side_effect = [
                b"+PONG\r\n",  # Response to PING
                b"+OK\r\n",  # Response to REPLCONF listening-port
                b"+OK\r\n",  # Response to REPLCONF capa
                b"+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n"  # Response to PSYNC
            ]

            # Start connection attempt
            try:
                await replication_manager.connect_to_master()
            except Exception:
                pass  # Exception expected when mock doesn't provide full RDB data

            # Verify connection sequence
            assert mock_writer.write.call_count >= 4
            calls = mock_writer.write.call_args_list

            # Verify PING
            assert b"PING" in calls[0][0][0]

            # Verify REPLCONF listening-port
            assert b"REPLCONF" in calls[1][0][0]
            assert b"listening-port" in calls[1][0][0]

            # Verify REPLCONF capa
            assert b"REPLCONF" in calls[2][0][0]
            assert b"capa" in calls[2][0][0]

            # Verify PSYNC
            assert b"PSYNC" in calls[3][0][0]

    async def test_rdb_transfer(self, replication_manager, temp_dir):
        """Test RDB file transfer handling"""
        mock_reader = AsyncMock()
        replication_manager.master_reader = mock_reader

        # Setup mock RDB data
        rdb_data = b"REDIS0011\xfa\x00\x00\x00\x00\x00\x00\x00\x00\xff"
        mock_reader.read.side_effect = [
                                           chunk for chunk in [rdb_data[i:i + 4] for i in range(0, len(rdb_data), 4)]
                                       ] + [b""]  # EOF
        mock_reader.readline.return_value = f"${len(rdb_data)}\r\n".encode()

        # Test RDB reception
        success = await replication_manager._receive_rdb()
        assert success

        # Verify RDB file was created
        rdb_path = f"{replication_manager.config.dir}/{replication_manager.config.dbfilename}"
        assert os.path.exists(rdb_path)

        # Verify file contents
        with open(rdb_path, 'rb') as f:
            saved_data = f.read()
            assert saved_data == rdb_data

    async def test_command_processing(self, replication_manager):
        """Test processing of commands received from master"""
        # Setup test data
        test_commands = [
            RESPCommand('SET', ['key1', 'value1']),
            RESPCommand('SET', ['key2', 'value2', 'PX', '1000']),
            RESPCommand('DEL', ['key1'])
        ]

        # Process each command
        for command in test_commands:
            await replication_manager._process_master_command(command)

        # Verify results
        assert replication_manager.data_store.get('key1') is None  # Should be deleted
        assert replication_manager.data_store.get('key2') == 'value2'  # Should exist