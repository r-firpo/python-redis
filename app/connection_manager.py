import asyncio, logging
from abc import ABC, abstractmethod
from typing import Set

### Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(funcName)s:%(lineno)d - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

class ConnectionManager(ABC):
    """Abstract base class for connection management"""

    @abstractmethod
    async def add_connection(self, writer: asyncio.StreamWriter) -> None: ...

    @abstractmethod
    async def remove_connection(self, writer: asyncio.StreamWriter) -> None: ...

    @abstractmethod
    async def get_connection_info(self) -> str: ...


class DefaultConnectionManager(ConnectionManager):
    """Default implementation of connection management"""

    def __init__(self):
        self.active_connections: Set[asyncio.StreamWriter] = set()

    async def add_connection(self, writer: asyncio.StreamWriter) -> None:
        self.active_connections.add(writer)

    async def remove_connection(self, writer: asyncio.StreamWriter) -> None:
        self.active_connections.remove(writer)

    async def get_connection_info(self) -> str:
        info = [f"Active connections: {len(self.active_connections)}"]
        for writer in self.active_connections:
            peer = writer.get_extra_info('peername')
            buffer_size = writer.transport.get_write_buffer_size()
            info.append(f"Connection {peer}: Write buffer size={buffer_size}")
        return "\n".join(info)