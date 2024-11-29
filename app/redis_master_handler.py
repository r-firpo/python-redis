import os
import asyncio
import logging
from typing import Set, Dict, Optional
from dataclasses import dataclass

from app.redis_data_store import RedisDataStore
from app.utils.config import ServerConfig

logger = logging.getLogger(__name__)


@dataclass
class ReplicaInfo:
    """Information about a connected replica"""
    writer: asyncio.StreamWriter
    offset: int = 0


class RedisMaster:
    """Handles master-side replication functionality"""

    def __init__(self, config: ServerConfig, data_store: RedisDataStore):
        self.config = config
        self.data_store = data_store
        self.replicas: Dict[str, ReplicaInfo] = {}
        self.replication_backlog = bytearray()
        self.backlog_offset = 0
        self.max_backlog_size = config.repl_backlog_size
        self.ack_check_task: Optional[asyncio.Task] = None

    async def start_ack_checker(self):
        """Start periodic task to check replica offsets"""
        self.ack_check_task = asyncio.create_task(self._check_replica_acks())

    async def _check_replica_acks(self):
        """Periodically send REPLCONF GETACK to replicas"""
        try:
            while True:
                for replica_key, replica_info in list(self.replicas.items()):
                    try:
                        # Send REPLCONF GETACK command
                        getack_cmd = (
                            b"*3\r\n"
                            b"$8\r\nREPLCONF\r\n"
                            b"$6\r\nGETACK\r\n"
                            b"$0\r\n\r\n"
                        )
                        replica_info.writer.write(getack_cmd)
                        await replica_info.writer.drain()
                    except Exception as e:
                        logger.error(f"Error sending GETACK to replica {replica_key}: {e}")
                        await self.remove_replica(replica_info.writer)

                await asyncio.sleep(1)  # Check every second
        except asyncio.CancelledError:
            logger.info("ACK checker task cancelled")
            raise

    async def stop(self):
        """Stop the master's background tasks"""
        if self.ack_check_task:
            self.ack_check_task.cancel()
            try:
                await self.ack_check_task
            except asyncio.CancelledError:
                pass

    def _get_replica_key(self, writer: asyncio.StreamWriter) -> str:
        """Get unique key for a replica based on its connection info"""
        addr = writer.get_extra_info('peername')
        return f"{addr[0]}:{addr[1]}"

    async def add_replica(self, writer: asyncio.StreamWriter) -> None:
        """Register a new replica"""
        key = self._get_replica_key(writer)
        self.replicas[key] = ReplicaInfo(writer=writer)
        self.config.connected_slaves += 1

    async def remove_replica(self, writer: asyncio.StreamWriter) -> None:
        """Remove a replica"""
        key = self._get_replica_key(writer)
        if key in self.replicas:
            del self.replicas[key]
            self.config.connected_slaves -= 1

    async def send_rdb(self, writer: asyncio.StreamWriter) -> None:
        """Send RDB file to replica"""
        try:
            # Save current dataset to RDB
            self.data_store.save_to_rdb()
            rdb_path = os.path.join(self.config.dir, self.config.dbfilename)

            # Get file size
            file_size = os.path.getsize(rdb_path)

            # Send file size in bulk string format (without trailing CRLF)
            size_header = f"${file_size}\r\n".encode()
            writer.write(size_header)
            await writer.drain()

            # Send file contents in chunks
            chunk_size = 8192  # 8KB chunks
            with open(rdb_path, 'rb') as f:
                while True:
                    chunk = f.read(chunk_size)
                    if not chunk:
                        break
                    writer.write(chunk)
                    await writer.drain()

            logger.info(f"Sent RDB file ({file_size} bytes)")

        except Exception as e:
            logger.error(f"Error sending RDB file: {e}")
            raise

    def append_to_backlog(self, data: bytes) -> None:
        """Append command to replication backlog"""
        self.replication_backlog.extend(data)
        if len(self.replication_backlog) > self.max_backlog_size:
            # Remove oldest data when backlog is full
            excess = len(self.replication_backlog) - self.max_backlog_size
            self.replication_backlog = self.replication_backlog[excess:]
            self.backlog_offset += excess

    async def propagate_write_command(self, command_bytes: bytes) -> None:
        """Propagate write command to all replicas"""
        self.append_to_backlog(command_bytes)

        # Send to all replicas
        for replica_key, replica_info in list(self.replicas.items()):
            try:
                replica_info.writer.write(command_bytes)
                await replica_info.writer.drain()
                replica_info.offset += len(command_bytes)
            except Exception as e:
                logger.error(f"Error propagating command to replica {replica_key}: {e}")
                await self.remove_replica(replica_info.writer)