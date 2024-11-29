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

    # Replication configuration
    role: str = 'master'  # either 'master' or 'slave'
    master_host: Optional[str] = None
    master_port: Optional[int] = None
    master_replid: str = '8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb'  # random replid for now
    master_repl_offset: int = 0
    repl_backlog_size: int = 1048576
    connected_slaves: int = 0

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
            elif args[i] == '--port' and i + 1 < len(args):
                try:
                    config.port = int(args[i + 1])
                except ValueError:
                    raise ValueError(f"Invalid port number: {args[i + 1]}")
                i += 2
            elif args[i] == '--replicaof' and i + 2 < len(args):
                config.role = 'slave'
                config.master_host = args[i + 1]
                try:
                    config.master_port = int(args[i + 2])
                except ValueError:
                    raise ValueError(f"Invalid master port number: {args[i + 2]}")
                i += 3
            else:
                i += 1

        return config