import asyncio
import logging
from typing import Optional
from dataclasses import dataclass

from app.redis_parser import RedisProtocolHandler, RESPParser
from app.utils.config import ServerConfig

logger = logging.getLogger(__name__)


@dataclass
class ReplicationState:
    """Stores the current state of replication"""
    master_link_status: str = "down"
    master_sync_in_progress: bool = False
    master_last_io_seconds: int = -1
    master_link_down_since: float = 0


class ReplicationManager:
    def __init__(self, config: ServerConfig):
        self.config = config
        self.state = ReplicationState()
        self.protocol = RedisProtocolHandler()
        self.parser = RESPParser()
        self.master_reader: Optional[asyncio.StreamReader] = None
        self.master_writer: Optional[asyncio.StreamWriter] = None

    async def connect_to_master(self):
        """Establish connection to master and perform initial handshake"""
        if self.config.role != 'slave':
            return

        try:
            # Connect to master
            self.master_reader, self.master_writer = await asyncio.open_connection(
                self.config.master_host,
                self.config.master_port
            )
            logger.info(f"Connected to master at {self.config.master_host}:{self.config.master_port}")

            # Step 1: Send PING
            logger.info("Sending PING to master")
            self.master_writer.write(b"*1\r\n$4\r\nPING\r\n")
            await self.master_writer.drain()

            # Wait for PONG
            response = await self.master_reader.readline()
            if not response.startswith(b"+PONG"):
                raise Exception(f"Unexpected response to PING: {response}")
            logger.info("Received PONG from master")

            # Step 2: Send REPLCONF listening-port
            logger.info(f"Sending REPLCONF listening-port {self.config.port}")
            port_str = str(self.config.port)
            replconf_port = (
                    b"*3\r\n"  # Array of 3 elements
                    b"$8\r\n"  # Length of "REPLCONF"
                    b"REPLCONF\r\n"  # Command
                    b"$13\r\n"  # Length of "listening-port"
                    b"listening-port\r\n"  # First argument
                    b"$4\r\n"  # Length of port number (assuming 4 digits)
                    + port_str.encode() +  # Port number
                    b"\r\n"  # Final CRLF
            )
            self.master_writer.write(replconf_port)
            await self.master_writer.drain()

            # Wait for OK
            response = await self.master_reader.readline()
            if not response.startswith(b"+OK"):
                raise Exception(f"Unexpected response to REPLCONF listening-port: {response}")

            # Step 3: Send REPLCONF capa psync2
            logger.info("Sending REPLCONF capa psync2")
            self.master_writer.write(
                b"*3\r\n"  # Array of 3 elements
                b"$8\r\n"  # Length of "REPLCONF"
                b"REPLCONF\r\n"  # Command
                b"$4\r\n"  # Length of "capa"
                b"capa\r\n"  # First argument
                b"$6\r\n"  # Length of "psync2"
                b"psync2\r\n"  # Second argument
            )
            await self.master_writer.drain()

            # Wait for OK
            response = await self.master_reader.readline()
            if not response.startswith(b"+OK"):
                raise Exception(f"Unexpected response to REPLCONF capa: {response}")

            # Step 4: Send PSYNC
            logger.info("Sending PSYNC ? -1")
            self.master_writer.write(
                b"*3\r\n"  # Array of 3 elements
                b"$5\r\n"  # Length of "PSYNC"
                b"PSYNC\r\n"  # Command
                b"$1\r\n"  # Length of "?"
                b"?\r\n"  # First argument
                b"$2\r\n"  # Length of "-1"
                b"-1\r\n"  # Second argument
            )
            await self.master_writer.drain()

            # Update replication state
            self.state.master_link_status = "up"
            self.state.master_last_io_seconds = 0

            logger.info("Initial replication handshake completed")

        except Exception as e:
            logger.error(f"Error during replication handshake: {e}")
            if self.master_writer:
                self.master_writer.close()
                await self.master_writer.wait_closed()
            self.master_reader = None
            self.master_writer = None
            self.state.master_link_status = "down"
            raise

    async def start_replication(self):
        """Start the replication process"""
        if self.config.role != 'slave':
            return

        while True:
            try:
                await self.connect_to_master()
                # TODO: Implement full replication logic here
                # This includes handling the master's response to PSYNC
                # and managing the replication stream
                await asyncio.sleep(1)  # Prevent tight loop
            except Exception as e:
                logger.error(f"Replication error: {e}")
                await asyncio.sleep(5)  # Wait before retrying