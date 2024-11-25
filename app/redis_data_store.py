from typing import Dict, Any, Optional
import time, logging

### Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(funcName)s:%(lineno)d - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

class RedisDataStore:
    """Simple in-memory Redis-like data store"""

    def __init__(self):
        self.data: Dict[str, Any] = {}
        self.expires: Dict[str, float] = {}

    def set(self, key: str, value: str, ex: Optional[int] = None) -> bool:
        """Set key to value with optional expiration in seconds"""
        self.data[key] = value
        if ex is not None:
            self.expires[key] = time.time() + ex
        return True

    def get(self, key: str) -> Optional[str]:
        """Get value for key, considering expiration"""
        if key in self.expires:
            if time.time() > self.expires[key]:
                # Key has expired
                del self.data[key]
                del self.expires[key]
                return None
        return self.data.get(key)

    def delete(self, key: str) -> bool:
        """Delete a key"""
        if key in self.data:
            del self.data[key]
            if key in self.expires:
                del self.expires[key]
            return True
        return False
