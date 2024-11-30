# test_replication_integration.py

import asyncio
import pytest
import tempfile
import os
from pathlib import Path
from typing import List, Tuple

from app.async_TCP_redis_server import RedisServer
from app.utils.config import ServerConfig
from app.redis_parser import RESPCommand
from tests.fixtures import temp_dir


@pytest.mark.integration
class TestReplicationIntegration:
    @pytest.fixture
    async def master_server(self, temp_dir):
        """Create and start a master server"""
        master_config = ServerConfig(
            dir=os.path.join(temp_dir, "master"),
            dbfilename="master.rdb",
            host="localhost",
            port=6379,
            role="master"
        )

        server = await RedisServer.create(config=master_config)
        async with server:
            yield server

    @pytest.fixture
    async def replica_servers(self, temp_dir, master_server) -> List[RedisServer]:
        """Create and start multiple replica servers"""
        replicas = []
        base_port = 6380

        for i in range(3):  # Create 3 replicas
            replica_config = ServerConfig(
                dir=os.path.join(temp_dir, f"replica{i}"),
                dbfilename=f"replica{i}.rdb",
                host="localhost",
                port=base_port + i,
                role="slave",
                master_host="localhost",
                master_port=6379
            )

            replica = await RedisServer.create(config=replica_config)
            replicas.append(replica)

        # Start all replicas
        for replica in replicas:
            await replica.__aenter__()

        yield replicas

        # Clean up replicas
        for replica in replicas:
            await replica.__aexit__(None, None, None)

    @pytest.fixture
    async def tcp_clients(self, master_server, replica_servers) -> List[
        Tuple[asyncio.StreamReader, asyncio.StreamWriter]]:
        """Create TCP clients for all servers"""
        clients = []

        # Connect to master
        reader, writer = await asyncio.open_connection(
            host=master_server.config.host,
            port=master_server.config.port
        )
        clients.append((reader, writer))

        # Connect to replicas
        for replica in replica_servers:
            reader, writer = await asyncio.open_connection(
                host=replica.config.host,
                port=replica.config.port
            )
            clients.append((reader, writer))

        yield clients

        # Clean up connections
        for _, writer in clients:
            writer.close()
            await writer.wait_closed()

    async def send_command(self, writer: asyncio.StreamWriter, command: RESPCommand):
        """Helper to send a command to a server"""
        # Convert command to RESP format
        if command.command == "PING":
            writer.write(b"*1\r\n$4\r\nPING\r\n")
        elif command.command == "SET":
            writer.write(
                f"*3\r\n$3\r\nSET\r\n${len(command.args[0])}\r\n{command.args[0]}\r\n"
                f"${len(command.args[1])}\r\n{command.args[1]}\r\n".encode()
            )
        elif command.command == "GET":
            writer.write(
                f"*2\r\n$3\r\nGET\r\n${len(command.args[0])}\r\n{command.args[0]}\r\n".encode()
            )
        await writer.drain()

    async def read_response(self, reader: asyncio.StreamReader) -> bytes:
        """Helper to read a response from a server"""
        response = await reader.readline()
        if response.startswith(b'$'):
            length = int(response[1:].decode().strip())
            if length == -1:
                return b"$-1\r\n"
            value = await reader.readline()
            return response + value
        return response

    @pytest.mark.asyncio
    async def test_replica_connection(self, master_server, replica_servers, tcp_clients):
        """Test that replicas successfully connect to master including RDB transfer"""
        # First set some data on master that should be transferred via RDB
        master_client = tcp_clients[0]
        test_data = [
            ('key1', 'value1'),
            ('key2', 'value2'),
            ('key3', 'value3'),
        ]

        # Write data to master
        for key, value in test_data:
            await self.send_command(master_client[1], RESPCommand('SET', [key, value]))
            response = await self.read_response(master_client[0])
            assert response == b"+OK\r\n"

        # Give some time for replication setup and RDB transfer
        await asyncio.sleep(1)

        # Verify master state
        assert master_server.master is not None
        assert master_server.config.connected_slaves == len(replica_servers)

        # Verify replica states and data
        replica_clients = tcp_clients[1:]
        for replica, client in zip(replica_servers, replica_clients):
            # Verify replica configuration
            assert replica.config.role == "slave"
            assert replica.replication_manager is not None

            # Verify replica received master's replid
            assert replica.replication_manager.state.received_replid == master_server.config.master_replid

            # Verify RDB file exists
            rdb_path = os.path.join(replica.config.dir, replica.config.dbfilename)
            assert os.path.exists(rdb_path), f"RDB file not found at {rdb_path}"

            # Verify all test data was transferred via RDB
            for key, value in test_data:
                await self.send_command(client[1], RESPCommand('GET', [key]))
                response = await self.read_response(client[0])
                expected = f"${len(value)}\r\n{value}\r\n".encode()
                assert response == expected, f"Data mismatch for {key} on replica"

            # Verify replication offset is correct
            assert replica.replication_manager.state.offset > 0, "Replication offset not updated"

    @pytest.mark.asyncio
    async def test_command_propagation(self, master_server, replica_servers, tcp_clients):
        """Test that write commands from master are propagated to replicas"""
        master_client = tcp_clients[0]
        replica_clients = tcp_clients[1:]

        # Write data to master
        await self.send_command(master_client[1], RESPCommand('SET', ['key1', 'value1']))
        response = await self.read_response(master_client[0])
        assert response == b"+OK\r\n"

        # Give some time for propagation
        await asyncio.sleep(0.5)

        # Verify data is replicated to all replicas
        for replica_client in replica_clients:
            await self.send_command(replica_client[1], RESPCommand('GET', ['key1']))
            response = await self.read_response(replica_client[0])
            assert response == b"$6\r\nvalue1\r\n"

    @pytest.mark.asyncio
    async def test_wait_command(self, master_server, replica_servers, tcp_clients):
        """Test WAIT command functionality with actual replicas"""
        master_client = tcp_clients[0]

        # Write data and wait for all replicas
        await self.send_command(master_client[1], RESPCommand('SET', ['key2', 'value2']))
        response = await self.read_response(master_client[0])
        assert response == b"+OK\r\n"

        # Send WAIT command for all replicas with timeout
        writer = master_client[1]
        writer.write(f"*3\r\n$4\r\nWAIT\r\n$1\r\n{len(replica_servers)}\r\n$4\r\n1000\r\n".encode())
        await writer.drain()

        response = await self.read_response(master_client[0])
        assert response == f":{len(replica_servers)}\r\n".encode()

    @pytest.mark.asyncio
    @pytest.mark.flaky(reruns=3)
    async def test_replica_reconnection(self, master_server, replica_servers):
        """Test replica reconnection behavior"""
        # First verify initial connection
        await asyncio.sleep(1)  # Give time for initial connections
        assert master_server.config.connected_slaves == len(replica_servers)

        # First set some data and verify it propagates correctly
        master_config = master_server.config
        reader, writer = await asyncio.open_connection(
            host=master_config.host,
            port=master_config.port
        )

        try:
            # Set initial data
            writer.write(b"*3\r\n$3\r\nSET\r\n$4\r\nkey1\r\n$6\r\nvalue1\r\n")
            await writer.drain()
            response = await reader.readline()
            assert response == b"+OK\r\n"

            # Verify initial propagation
            await asyncio.sleep(1)
            for replica in replica_servers:
                r_reader, r_writer = await asyncio.open_connection(
                    host=replica.config.host,
                    port=replica.config.port
                )
                try:
                    r_writer.write(b"*2\r\n$3\r\nGET\r\n$4\r\nkey1\r\n")
                    await r_writer.drain()
                    resp = await r_reader.readline()
                    assert resp == b"$6\r\n"
                    value = await r_reader.readline()
                    assert value == b"value1\r\n"
                finally:
                    r_writer.close()
                    await r_writer.wait_closed()

            # Force disconnect replicas
            for replica in replica_servers:
                if replica.replication_manager and replica.replication_manager.master_writer:
                    replica.replication_manager.master_writer.close()
                    await replica.replication_manager.master_writer.wait_closed()

            # Wait and verify reconnection with retries
            max_retries = 5
            retry_interval = 1
            reconnected = False
            for _ in range(max_retries):
                await asyncio.sleep(retry_interval)
                if master_server.config.connected_slaves == len(replica_servers):
                    reconnected = True
                    break

            assert reconnected, f"Expected {len(replica_servers)} connected replicas, got {master_server.config.connected_slaves}"

            # Wait for replication to stabilize after reconnection
            await asyncio.sleep(2)

            # Set new data after reconnection
            writer.write(b"*3\r\n$3\r\nSET\r\n$4\r\nkey2\r\n$6\r\nvalue2\r\n")
            await writer.drain()
            response = await reader.readline()
            assert response == b"+OK\r\n"

            # Give time for propagation
            await asyncio.sleep(1)

            # Verify all replicas can still receive updates
            for replica in replica_servers:
                # First verify the replica is connected
                assert replica.replication_manager.master_writer is not None, \
                    f"Replica on port {replica.config.port} failed to reconnect"

                # Verify offset is being tracked
                assert replica.replication_manager.state.offset >= 0, \
                    f"Replica on port {replica.config.port} has invalid offset"

                # Check both keys exist in replica
                r_reader, r_writer = await asyncio.open_connection(
                    host=replica.config.host,
                    port=replica.config.port
                )
                try:
                    # Check both pre-disconnect and post-disconnect keys
                    for key, expected_value in [("key1", "value1"), ("key2", "value2")]:
                        r_writer.write(f"*2\r\n$3\r\nGET\r\n${len(key)}\r\n{key}\r\n".encode())
                        await r_writer.drain()
                        resp = await r_reader.readline()
                        assert resp == f"${len(expected_value)}\r\n".encode(), \
                            f"Wrong response length for key {key} on replica {replica.config.port}"
                        value = await r_reader.readline()
                        assert value == f"{expected_value}\r\n".encode(), \
                            f"Wrong value for key {key} on replica {replica.config.port}"
                finally:
                    r_writer.close()
                    await r_writer.wait_closed()

        finally:
            writer.close()
            await writer.wait_closed()
