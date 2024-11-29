import asyncio
import logging
import os
from typing import Optional
from dataclasses import dataclass

from app.redis_data_store import RedisDataStore
from app.redis_parser import RedisProtocolHandler, RESPParser, RESPCommand
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
    """Handles secondary-side replication of data"""
    def __init__(self, config: ServerConfig, data_store: RedisDataStore):
        self.config = config
        self.data_store = data_store
        self.state = ReplicationState()
        self.protocol = RedisProtocolHandler()
        self.parser = RESPParser()
        self.master_reader: Optional[asyncio.StreamReader] = None
        self.master_writer: Optional[asyncio.StreamWriter] = None

    def _encode_array_command(self, *args: str) -> bytes:
        """Encode a command and its arguments as a RESP array"""
        items = []
        for arg in args:
            items.append(self.protocol.encode_bulk_string(arg))
        return self.protocol.encode_array(items)

    async def _read_length(self) -> int:
        """Read bulk string length from master"""
        try:
            length_line = await self.master_reader.readline()
            if not length_line.startswith(b'$'):
                raise ValueError(f"Expected $length, got: {length_line}")
            return int(length_line[1:-2])  # Skip $ and \r\n
        except (ValueError, TypeError) as e:
            raise ValueError(f"Invalid length format: {e}")

    async def _receive_rdb(self) -> bool:
        """Receive and process RDB file from master"""
        try:
            logger.info("Starting RDB transfer")
            self.state.master_sync_in_progress = True

            # Read the RDB file length
            length = await self._read_length()
            logger.info(f"Expecting RDB file of size {length} bytes")

            # Create a temporary file to store the RDB data
            temp_rdb_path = os.path.join(self.config.dir, f"temp_{self.config.dbfilename}")
            final_rdb_path = os.path.join(self.config.dir, self.config.dbfilename)

            # Ensure directory exists
            os.makedirs(self.config.dir, exist_ok=True)

            bytes_received = 0
            with open(temp_rdb_path, 'wb') as f:
                while bytes_received < length:
                    chunk_size = min(8192, length - bytes_received)
                    chunk = await self.master_reader.read(chunk_size)
                    if not chunk:
                        raise EOFError("Connection closed during RDB transfer")
                    f.write(chunk)
                    bytes_received += len(chunk)
                    logger.info(f"RDB transfer progress: {bytes_received}/{length} bytes")

            # Rename temp file to final file
            os.replace(temp_rdb_path, final_rdb_path)

            # Load the new RDB file
            logger.info("Loading received RDB file")
            self.data_store._load_rdb()

            logger.info("RDB transfer completed successfully")
            return True

        except Exception as e:
            logger.error(f"Error receiving RDB file: {e}")
            return False
        finally:
            self.state.master_sync_in_progress = False

    async def _process_master_command(self, command: RESPCommand) -> None:
        """Process a command received from master"""
        try:
            # Update offset based on the full command size
            # Note: This is a simplification, in real Redis the offset
            # would be based on the exact bytes received
            self.state.offset += 1

            # Execute the command locally
            cmd = command.command.upper()
            if cmd == 'SET':
                if len(command.args) >= 2:
                    key, value = command.args[0], command.args[1]
                    px = None
                    if len(command.args) > 2 and command.args[2].upper() == 'PX':
                        px = int(command.args[3])
                    self.data_store.set(key, value, px=px)
            elif cmd == 'DEL':
                if len(command.args) >= 1:
                    self.data_store.delete(command.args[0])
            # Add other commands as needed...

        except Exception as e:
            logger.error(f"Error processing master command: {e}")

    async def _process_master_stream(self) -> None:
        """Process the continuous stream of commands from master"""
        try:
            while True:
                command = await self.parser.parse_stream(self.master_reader)
                if not command:
                    break

                logger.info(f"Received command from master: {command}")

                # Handle REPLCONF GETACK command
                if command.command.upper() == 'REPLCONF' and len(command.args) > 0 and command.args[
                    0].upper() == 'GETACK':
                    # Send ACK response with current offset
                    ack_response = self._encode_array_command(
                        "REPLCONF",
                        "ACK",
                        str(self.state.offset)
                    )
                    self.master_writer.write(ack_response)
                    await self.master_writer.drain()
                    continue

                # Process normal command
                await self._process_master_command(command)

        except Exception as e:
            logger.error(f"Error processing master stream: {e}")
            raise

    async def connect_to_master(self):
        """Establish connection to master and perform initial handshake"""
        if self.config.role != 'slave':
            return

        try:
            # Initial connection
            self.master_reader, self.master_writer = await asyncio.open_connection(
                self.config.master_host,
                self.config.master_port
            )
            logger.info(f"Connected to master at {self.config.master_host}:{self.config.master_port}")

            # Step 1: Send PING
            logger.info("Sending PING to master")
            ping_cmd = self._encode_array_command("PING")
            self.master_writer.write(ping_cmd)
            await self.master_writer.drain()

            # Wait for PONG
            response = await self.master_reader.readline()
            if not response.startswith(b"+PONG"):
                raise Exception(f"Unexpected response to PING: {response}")
            logger.info("Received PONG from master")

            # Step 2: Send REPLCONF listening-port
            logger.info(f"Sending REPLCONF listening-port {self.config.port}")
            replconf_port = self._encode_array_command(
                "REPLCONF",
                "listening-port",
                str(self.config.port)
            )
            self.master_writer.write(replconf_port)
            await self.master_writer.drain()

            # Wait for OK
            response = await self.master_reader.readline()
            if not response.startswith(b"+OK"):
                raise Exception(f"Unexpected response to REPLCONF listening-port: {response}")

            # Step 3: Send REPLCONF capa psync2
            logger.info("Sending REPLCONF capa psync2")
            replconf_capa = self._encode_array_command(
                "REPLCONF",
                "capa",
                "psync2"
            )
            self.master_writer.write(replconf_capa)
            await self.master_writer.drain()

            # Wait for OK
            response = await self.master_reader.readline()
            if not response.startswith(b"+OK"):
                raise Exception(f"Unexpected response to REPLCONF capa: {response}")

            # Step 4: Send PSYNC
            logger.info("Sending PSYNC ? -1")
            psync_cmd = self._encode_array_command(
                "PSYNC",
                "?",
                "-1"
            )
            self.master_writer.write(psync_cmd)
            await self.master_writer.drain()

            # Update replication state
            self.state.master_link_status = "up"
            self.state.master_last_io_seconds = 0

            logger.info("Initial replication handshake completed")

            # Wait for FULLRESYNC response
            response = await self.master_reader.readline()
            if not response.startswith(b'+FULLRESYNC'):
                raise Exception(f"Unexpected response to PSYNC: {response}")

            # Parse replication ID and offset
            parts = response.decode().strip()[1:].split()
            if len(parts) == 3:  # FULLRESYNC <replid> <offset>
                self.state.received_replid = parts[1]
                self.state.offset = int(parts[2])

            logger.info(f"Starting FULLRESYNC with ID: {self.state.received_replid}, offset: {self.state.offset}")

            # Receive and load RDB file
            if not await self._receive_rdb():
                raise Exception("Failed to receive RDB file")

            # Start processing the command stream
            await self._process_master_stream()

        except Exception as e:
            logger.error(f"Error during replication: {e}")
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
                await asyncio.sleep(1)  # Prevent tight loop
            except Exception as e:
                logger.error(f"Replication error: {e}")
                await asyncio.sleep(5)  # Wait before retrying