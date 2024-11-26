import asyncio
import logging
from types import TracebackType
from typing import Optional, Type, Protocol, List

from app.connection_manager import ConnectionManager, DefaultConnectionManager
from app.redis_data_store import RedisDataStore
from app.redis_parser import RESPCommand, RedisProtocolHandler, RESPParser

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

        self.server = None
        self.monitor_task: Optional[asyncio.Task] = None

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
        return self

    async def __aexit__(
            self,
            exc_type: Optional[Type[BaseException]],
            exc_val: Optional[BaseException],
            exc_tb: Optional[TracebackType]
    ):
        """Clean up all resources when exiting context"""
        if self.monitor_task:
            self.monitor_task.cancel()
            try:
                await self.monitor_task
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
                response = await self.process_command(command)
                writer.write(response)
                await writer.drain()

        except Exception as e:
            logger.error(f"Error handling client {peer_name}: {e}")
        finally:
            await self.connection_manager.remove_connection(writer)
            writer.close()
            await writer.wait_closed()

    async def process_command(self, command: RESPCommand) -> bytes:
        """Process Redis command and return RESP response"""
        cmd = command.command.upper()
        args = command.args

        try:
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
            else:
                return self.protocol.encode_error(f'unknown command {cmd}')

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