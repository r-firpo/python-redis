import asyncio
import logging
from types import TracebackType
from typing import Optional, Type, Set



### Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(funcName)s:%(lineno)d - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

class AsyncServer:
    """
    A custom async TCP redis server that supports RESP protocol and provides monitoring of active connections
    """
    def __init__(self, host='localhost', port=6379):
        self.host = host
        self.port = port
        self.server = None
        self.active_connections: Set[asyncio.StreamWriter] = set()
        self.monitor_task: Optional[asyncio.Task] = None

    async def __aenter__(self):
        """Start server and monitoring when entering context"""
        self.server = await asyncio.start_server(
            self.handle_client,
            self.host,
            self.port,
            backlog=100,  # Allow many pending connections
            limit=65536  # StreamReader buffer limit
        )

        # Start monitoring task
        self.monitor_task = asyncio.create_task(self.monitor_connections())
        return self

    async def __aexit__(
            self,
            exc_type: Optional[Type[BaseException]],
            exc_val: Optional[BaseException],
            exc_tb: Optional[TracebackType]
    ):
        """Clean up all resources when exiting context"""
        # Cancel monitoring
        if self.monitor_task:
            self.monitor_task.cancel()
            try:
                await self.monitor_task
            except asyncio.CancelledError:
                pass

        # Close all client connections
        for writer in self.active_connections:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception as e:
                logger.info(f"Error closing client connection: {e}")
        self.active_connections.clear()

        # Close server
        if self.server:
            self.server.close()
            await self.server.wait_closed()
            logger.info("Server shutdown complete")

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """Handle individual client connection"""

        class ClientConnection:
            def __init__(self, server, writer):
                self.server = server
                self.writer = writer

            async def __aenter__(self):
                self.server.active_connections.add(self.writer)
                return self

            async def __aexit__(self, exc_type, exc_val, exc_tb):
                self.server.active_connections.remove(self.writer)
                self.writer.close()
                await self.writer.wait_closed()

        peer_name = writer.get_extra_info('peername')
        logger.info(f"New connection from {peer_name}")

        async with ClientConnection(self, writer):
            try:
                while True:
                    # Wait for data
                    data = await reader.read(1024)
                    if not data:
                        logger.info(f"Client {peer_name} disconnected")
                        break

                    # Process message
                    message = data.decode('utf-8')
                    logger.info(f"Received from {peer_name}: {message}")

                    # Send response
                    response = b"+PONG\r\n"
                    writer.write(response)
                    await writer.drain()

            except Exception as e:
                logger.info(f"Error handling client {peer_name}: {e}")

    async def monitor_connections(self):
        """Monitor active connections and their status"""
        try:
            while True:
                logger.info(f"\nActive connections: {len(self.active_connections)}")

                for writer in self.active_connections:
                    peer = writer.get_extra_info('peername')
                    buffer_size = writer.transport.get_write_buffer_size()
                    logger.info(f"Connection {peer}: Write buffer size={buffer_size}")

                await asyncio.sleep(5)  # Update every 5 seconds

        except asyncio.CancelledError:
            logger.info("Connection monitoring stopped")
            raise

    async def start(self):
        """Start the server"""
        async with self.server:
            addr = self.server.sockets[0].getsockname()
            logger.info(f'Serving on {addr}')
            await self.server.serve_forever()


async def run():
    async with AsyncServer() as server:
        await server.start()