from dataclasses import dataclass


@dataclass
class ServerConfig:
    """Configuration for Redis Server"""
    host: str = 'localhost'
    port: int = 6379
    backlog: int = 100
    buffer_limit: int = 65536
    monitoring_interval: int = 5