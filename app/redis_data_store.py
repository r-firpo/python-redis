import asyncio
from dataclasses import dataclass
from typing import Dict, Any, Optional
import time, logging
from app.utils.config import ServerConfig


### Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(funcName)s:%(lineno)d - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

@dataclass
class ExpirationInfo:
    """Stores expiration information for a key"""
    expire_at: float  # Unix timestamp in milliseconds when key expires
class RedisDataStore:
    def __init__(self):
        self.data: Dict[str, Any] = {}
        self.expires: Dict[str, ExpirationInfo] = {}
        self.delete_task = asyncio.create_task(self.delete_task())



    def set(self, key: str, value: str, px: Optional[int] = None) -> bool:
        """
        Set key to value with optional expiration in milliseconds
        Args:
            key: The key to set
            value: The value to set
            px: Optional expiration time in milliseconds
        """
        self.data[key] = value

        if px is not None:
            # Convert current time to milliseconds and add px
            expire_at = (time.time() * 1000) + px
            self.expires[key] = ExpirationInfo(expire_at=expire_at)
        elif key in self.expires:
            # Remove any existing expiration
            del self.expires[key]

        return True

    def get(self, key: str) -> Optional[str]:
        """Get value for key, considering expiration"""
        if key in self.expires:
            # Convert current time to milliseconds for comparison
            current_time_ms = time.time() * 1000
            if current_time_ms > self.expires[key].expire_at:
                # Key has expired
                del self.data[key]
                del self.expires[key]
                return None

        return self.data.get(key)

    def delete(self, key: str) -> bool:
        """Delete a key and its expiration info if it exists"""
        existed = False
        if key in self.data:
            del self.data[key]
            existed = True
        if key in self.expires:
            del self.expires[key]
        return existed

    async def cleanup_expired(self) -> None:
        """Periodically cleanup expired keys"""
        current_time_ms = time.time() * 1000
        expired_keys = [
            key for key, exp in self.expires.items()
            if current_time_ms > exp.expire_at
        ]

        for key in expired_keys:
            self.data.pop(key, None)
            self.expires.pop(key, None)

    async def delete_task(self) -> None:
        """Delete expired keys"""
        try:
            while True:
                await self.cleanup_expired()
                await asyncio.sleep(ServerConfig.monitoring_interval)
        except asyncio.CancelledError:
            logger.info("Delete task stopped")
            raise

