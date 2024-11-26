from dataclasses import dataclass
from typing import Optional


@dataclass
class ServerConfig:
    """Configuration for Redis Server"""
    host: str = 'localhost'
    port: int = 6379
    backlog: int = 100
    buffer_limit: int = 65536
    monitoring_interval: int = 5

    # RDB configuration
    dir: str = '/tmp/redis-files-rdb'  # default directory for RDB files
    dbfilename: str = 'dump.rdb'  # default RDB filename

    @classmethod
    def from_args(cls, args: Optional[list[str]] = None) -> 'ServerConfig':
        """Create config from command line arguments"""
        config = cls()

        if not args:
            return config

        i = 0
        while i < len(args):
            if args[i] == '--dir' and i + 1 < len(args):
                config.dir = args[i + 1]
                i += 2
            elif args[i] == '--dbfilename' and i + 1 < len(args):
                config.dbfilename = args[i + 1]
                i += 2
            else:
                i += 1

        return config