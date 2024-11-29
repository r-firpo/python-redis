import asyncio
import logging
from types import TracebackType
from typing import Optional, Type, Protocol, List

from app.connection_manager import ConnectionManager, DefaultConnectionManager
from app.redis_data_store import RedisDataStore
from app.redis_master_handler import RedisMaster
from app.redis_parser import RESPCommand, RedisProtocolHandler, RESPParser
from app.replication_manager import ReplicationManager

from app.utils.config import ServerConfig

### Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(funcName)s:%(lineno)d - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# protocols/interfaces for our dependencies
class DataStore(Protocol):
    def set(self, key: str, value: str, px: Optional[int] = None) -> bool: ...

    def get(self, key: str) -> Optional[str]: ...

    def delete(self, key: str) -> bool: ...

class ProtocolHandler(Protocol):
    def encode_simple_string(self, s: str) -> bytes: ...

    def encode_error(self, err: str) -> bytes: ...

    def encode_integer(self, i: int) -> bytes: ...

    def encode_bulk_string(self, s: Optional[str]) -> bytes: ...

    def encode_array(self, items: List[bytes]) -> bytes: ...

class Parser(Protocol):
    async def parse_stream(self, reader: asyncio.StreamReader) -> Optional[RESPCommand]: ...


class RedisServer:
    """
    A custom async TCP redis server that supports RESP protocol and provides monitoring of active connections
    """

    @classmethod
    async def create(cls,
                     data_store: Optional[DataStore] = None,
                     protocol_handler: Optional[ProtocolHandler] = None,
                     parser: Optional[Parser] = None,
                     connection_manager: Optional[ConnectionManager] = None,
                     config: Optional[ServerConfig] = None) -> 'RedisServer':
        """Factory method to create a RedisServer instance with default or custom dependencies"""
        return cls(
            data_store=data_store or RedisDataStore(config or ServerConfig()),
            protocol_handler=protocol_handler or RedisProtocolHandler(),
            parser=parser or RESPParser(),
            connection_manager=connection_manager or DefaultConnectionManager(),
            config=config or ServerConfig(),
        )

    def __init__(self,
                 data_store: DataStore,
                 protocol_handler: ProtocolHandler,
                 parser: Parser,
                 connection_manager: ConnectionManager,
                 config: ServerConfig):
        self.data_store = data_store
        self.protocol = protocol_handler
        self.parser = parser
        self.connection_manager = connection_manager
        self.config = config
        self.replication_manager = ReplicationManager(config, data_store) if config.role == 'slave' else None
        self.master = RedisMaster(config, data_store) if config.role == 'master' else None

        self.server = None
        self.monitor_task: Optional[asyncio.Task] = None
        self.replication_task: Optional[asyncio.Task] = None

    async def __aenter__(self):
        """Start server and monitoring when entering context"""
        self.server = await asyncio.start_server(
            self.handle_client,
            self.config.host,
            self.config.port,
            backlog=self.config.backlog,
            limit=self.config.buffer_limit
        )
        self.monitor_task = asyncio.create_task(self.monitor_connections())

        # Start replication if we're a slave
        if self.config.role == 'slave':
            self.replication_task = asyncio.create_task(self.replication_manager.start_replication())

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Clean up all resources when exiting context"""
        if self.monitor_task:
            self.monitor_task.cancel()
            try:
                await self.monitor_task
            except asyncio.CancelledError:
                pass

        if self.replication_task:
            self.replication_task.cancel()
            try:
                await self.replication_task
            except asyncio.CancelledError:
                pass

        if self.server:
            self.server.close()
            await self.server.wait_closed()
            logger.info("Server shutdown complete")

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """Handle individual client connection"""
        peer_name = writer.get_extra_info('peername')
        logger.info(f"New connection from {peer_name}")

        try:
            await self.connection_manager.add_connection(writer)
            while True:
                command = await self.parser.parse_stream(reader)
                if not command:
                    break

                logger.info(f"Received from {peer_name}: {command}")
                response = await self.process_command(command, writer)
                if response:  # Only write response if we have one
                    writer.write(response)
                    await writer.drain()

        except Exception as e:
            logger.error(f"Error handling client {peer_name}: {e}")
        finally:
            if self.config.role == 'master':
                await self.master.remove_replica(writer)
            await self.connection_manager.remove_connection(writer)
            writer.close()
            await writer.wait_closed()

    async def process_command(self, command: RESPCommand, writer: asyncio.StreamWriter) -> bytes:
        """Process Redis command and return RESP response"""
        cmd = command.command.upper()
        args = command.args

        try:
            # Handle replication commands when acting as master
            if self.config.role == 'master':
                if cmd == 'REPLCONF':
                    if not args:
                        return self.protocol.encode_error('wrong number of arguments for REPLCONF')

                    subcmd = args[0].lower()
                    if subcmd == 'listening-port':
                        if len(args) != 2:
                            return self.protocol.encode_error('wrong number of arguments for REPLCONF listening-port')
                        return self.protocol.encode_simple_string('OK')

                    elif subcmd == 'capa':
                        if len(args) != 2:
                            return self.protocol.encode_error('wrong number of arguments for REPLCONF capa')
                        return self.protocol.encode_simple_string('OK')

                    else:
                        return self.protocol.encode_error(f'unknown REPLCONF subcommand {subcmd}')

                elif cmd == 'PSYNC':
                    if len(args) != 2 or not writer:
                        return self.protocol.encode_error('wrong number of arguments for PSYNC')

                    try:
                        # Register new replica
                        await self.master.add_replica(writer)

                        # Send FULLRESYNC response
                        response = f"FULLRESYNC {self.config.master_replid} 0"
                        writer.write(self.protocol.encode_simple_string(response))
                        await writer.drain()

                        # Send RDB file
                        await self.master.send_rdb(writer)

                        # Return empty response since we've already sent everything
                        return b""

                    except Exception as e:
                        logger.error(f"Error handling PSYNC: {e}")
                        await self.master.remove_replica(writer)
                        return self.protocol.encode_error("replication error")

            # Handle write commands
            response = None
            is_write_command = cmd in {'SET', 'DEL', 'EXPIRE', 'INCR', 'DECR', 'RPUSH', 'LPUSH', 'SADD',
                                       'ZADD'}

            if is_write_command and self.config.role == 'master':
                # Get the exact bytes that were received for this command
                command_bytes = self.protocol.encode_array([
                    self.protocol.encode_bulk_string(cmd),
                    *(self.protocol.encode_bulk_string(arg) for arg in args)
                ])
                # Propagate to replicas before executing
                await self.master.propagate_write_command(command_bytes)

            if cmd == 'PING':
                if len(args) > 1:
                    return self.protocol.encode_error('wrong number of arguments for PING')
                return (self.protocol.encode_bulk_string(args[0]) if args
                        else self.protocol.encode_simple_string('PONG'))

            elif cmd == 'ECHO':
                if len(args) != 1:
                    return self.protocol.encode_error('wrong number of arguments for ECHO')
                return self.protocol.encode_bulk_string(args[0])

            elif cmd == 'SET':
                if len(args) < 2:
                    return self.protocol.encode_error('wrong number of arguments for SET')
                key, value = args[0], args[1]
                px = None
                # Parse options
                i = 2
                while i < len(args):
                    option = args[i].upper()  # case doesn't matter
                    if option == 'PX':
                        if i + 1 >= len(args):
                            return self.protocol.encode_error('value is required for PX option')
                        try:
                            px = int(args[i + 1])
                            # Validate PX value
                            if px <= 0:
                                return self.protocol.encode_error('PX value must be positive')
                            if px > 999999999999999:  # Maximum reasonable milliseconds
                                return self.protocol.encode_error('PX value is too large')
                        except ValueError:
                            return self.protocol.encode_error('PX value must be an integer')
                        i += 2
                    else:
                        return self.protocol.encode_error(f'unknown option {option}')
                success = self.data_store.set(key, value, px=px)
                if not success:
                    return self.protocol.encode_error('operation failed')
                return self.protocol.encode_simple_string('OK')

            elif cmd == 'GET':
                if len(args) != 1:
                    return self.protocol.encode_error('wrong number of arguments for GET')
                value = self.data_store.get(args[0])
                return self.protocol.encode_bulk_string(value)

            elif cmd == 'DEL':
                if len(args) != 1:
                    return self.protocol.encode_error('wrong number of arguments for DEL')
                success = self.data_store.delete(args[0])
                return self.protocol.encode_integer(1 if success else 0)

            elif cmd == 'KEYS':
                if len(args) != 1:
                    return self.protocol.encode_error('wrong number of arguments for KEYS command')

                pattern = args[0]
                if pattern != "*":
                    return self.protocol.encode_error('only "*" pattern is supported')

                # Get all keys from data store
                keys = self.data_store.get_keys()

                # Convert keys to bulk strings
                key_strings = [
                    self.protocol.encode_bulk_string(key)
                    for key in keys
                ]

                # Return as array
                return self.protocol.encode_array(key_strings)

            elif cmd == 'CONFIG':
                if len(args) < 2:
                    return self.protocol.encode_error('wrong number of arguments for CONFIG command')
                if args[0].upper() == 'GET':
                    param = args[1].lower()
                    config_value = None
                    if param == 'dir':
                        config_value = self.config.dir
                    elif param == 'dbfilename':
                        config_value = self.config.dbfilename
                    # Create array response with parameter name and value
                    return self.protocol.encode_array([
                        self.protocol.encode_bulk_string(param),
                        self.protocol.encode_bulk_string(
                            config_value) if config_value else self.protocol.encode_bulk_string(None)
                    ])
                else:
                    return self.protocol.encode_error(f'unknown CONFIG subcommand {args[0]}')

            if cmd == 'INFO':
                if len(args) > 1:
                    return self.protocol.encode_error('wrong number of arguments for INFO')

                section = args[0].lower() if args else None
                if section is None or section == 'replication':
                    # Build replication info
                    info_lines = [
                        "# Replication",
                        f"role:{self.config.role}",
                        f"connected_slaves:{self.config.connected_slaves}"
                    ]
                    if self.config.role == 'master':
                        info_lines.extend([
                            f"master_replid:{self.config.master_replid}",
                            f"master_repl_offset:{self.config.master_repl_offset}",
                            "second_repl_offset:-1",
                            "repl_backlog_active:0",
                            f"repl_backlog_size:{self.config.repl_backlog_size}",
                            "repl_backlog_first_byte_offset:0",
                            "repl_backlog_histlen:0"
                        ])
                    elif self.config.role == 'slave':
                        info_lines.extend([
                            f"master_host:{self.config.master_host}",
                            f"master_port:{self.config.master_port}",
                            "master_link_status:down",  # update this when we implement replication
                            "master_last_io_seconds_ago:-1",
                            f"master_sync_in_progress:0",
                            "slave_repl_offset:0",
                            "slave_priority:100",
                            "slave_read_only:1"
                        ])
                    # Join lines with \r\n for RESP protocol
                    info_str = '\r\n'.join(info_lines)
                    return self.protocol.encode_bulk_string(info_str)
                else:
                    return self.protocol.encode_error(f'Invalid section name {section}')

        except Exception as e:
            return self.protocol.encode_error(str(e))

    async def monitor_connections(self):
        """Monitor active connections and their status"""
        try:
            while True:
                info = await self.connection_manager.get_connection_info()
                logger.info(info)
                await asyncio.sleep(self.config.monitoring_interval)
        except asyncio.CancelledError:
            logger.info("Connection monitoring stopped")
            raise

    async def start(self):
        """Start the server"""
        async with self.server:
            addr = self.server.sockets[0].getsockname()
            logger.info(f'Serving on {addr}')
            await self.server.serve_forever()


# Example usage
async def run(config: Optional[ServerConfig] = None):
    # Create server with default dependencies
    server = await RedisServer.create(config=config)
    async with server:
        await server.start()