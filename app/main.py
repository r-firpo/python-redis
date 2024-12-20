import socket  # noqa: F401
import logging
import sys

from app.async_TCP_redis_server import run
import asyncio

from app.utils.config import ServerConfig

### Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(funcName)s:%(lineno)d - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# def main():
#     # You can use print statements as follows for debugging, they'll be visible when running tests.
#     print("Logs from your program will appear here!")
#     logger.info("Hello World!")
#
#     server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
#     connection, _ = server_socket.accept()
#     while connection:
#         connection.sendall(b"+PONG\r\n")



if __name__ == "__main__":
    config = ServerConfig.from_args(sys.argv[1:])
    logger.info(f"server config: {config}")
    asyncio.run(run(config=config))
