import asyncio
import logging
from types import TracebackType
from typing import Optional, Type, Protocol

from app.connection_manager import ConnectionManager, DefaultConnectionManager
from app.redis_data_store import RedisDataStore
from app.redis_parser import RESPCommand, RedisProtocolHandler, RESPParser
from dataclasses import dataclass


### Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(funcName)s:%(lineno)d - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# First, let's define protocols/interfaces for our dependencies
class DataStore(Protocol):
    def set(self, key: str, value: str, ex: Optional[int] = None) -> bool: ...

    def get(self, key: str) -> Optional[str]: ...

    def delete(self, key: str) -> bool: ...


class ProtocolHandler(Protocol):
    def encode_simple_string(self, s: str) -> bytes: ...

    def encode_error(self, err: str) -> bytes: ...

    def encode_integer(self, i: int) -> bytes: ...

    def encode_bulk_string(self, s: Optional[str]) -> bytes: ...


class Parser(Protocol):
    async def parse_stream(self, reader: asyncio.StreamReader) -> Optional[RESPCommand]: ...


@dataclass
class ServerConfig:
    """Configuration for Redis Server"""
    host: str = 'localhost'
    port: int = 6379
    backlog: int = 100
    buffer_limit: int = 65536
    monitoring_interval: int = 5


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
            data_store=data_store or RedisDataStore(),
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

                self.logger.info(f"Received from {peer_name}: {command}")
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
                return self.protocol.encode_simple_string('PONG')

            elif cmd == 'SET':
                if len(args) < 2:
                    return self.protocol.encode_error('wrong number of arguments for SET')

                key, value = args[0], args[1]
                ex = None
                if len(args) > 2 and args[2].upper() == 'EX' and len(args) > 3:
                    ex = int(args[3])

                self.data_store.set(key, value, ex)
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
async def run():
    # Create server with default dependencies
    server = await RedisServer.create()
    async with server:
        await server.start()