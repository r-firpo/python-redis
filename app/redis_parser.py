import asyncio
import logging
from typing import List, Optional
from dataclasses import dataclass
from enum import Enum


### Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(funcName)s:%(lineno)d - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


class RESPType(Enum):
    """RESP Protocol Types"""
    SIMPLE_STRING = '+'  # +OK\r\n
    ERROR = '-'  # -Error message\r\n
    INTEGER = ':'  # :1000\r\n
    BULK_STRING = '$'  # $4\r\ntest\r\n
    ARRAY = '*'  # *2\r\n$4\r\ntest\r\n$4\r\ntest\r\n


@dataclass
class RESPCommand:
    """Represents a parsed Redis command"""
    command: str
    args: List[str]


class RESPParser:
    """Parser for Redis RESP protocol"""

    def __init__(self):
        self.buffer = ""

    async def parse_stream(self, reader: asyncio.StreamReader) -> Optional[RESPCommand]:
        """Parse incoming Redis commands from a stream"""
        try:
            # Read the first line to determine the type
            first_byte = await reader.read(1)
            if not first_byte:  # EOF
                return None

            if first_byte == b'*':  # Array (typical command format)
                # Read array length
                array_length = await self._read_integer(reader)
                if array_length < 1:
                    raise ValueError("Invalid array length")

                # Parse each array element
                elements = []
                for _ in range(array_length):
                    # Each element should be a bulk string
                    element_type = await reader.read(1)
                    if element_type != b'$':
                        raise ValueError(f"Expected bulk string, got {element_type}")

                    # Read string length
                    str_length = await self._read_integer(reader)
                    if str_length < 0:
                        raise ValueError("Invalid string length")

                    # Read the string content
                    content = await reader.read(str_length)
                    if len(content) != str_length:
                        raise ValueError("Incomplete string")

                    # Read and verify CRLF
                    if await reader.read(2) != b'\r\n':
                        raise ValueError("Missing CRLF")

                    elements.append(content.decode('utf-8'))

                # First element is the command, rest are arguments
                return RESPCommand(
                    command=elements[0].upper(),
                    args=elements[1:]
                )

            else:
                raise ValueError(f"Unexpected message type: {first_byte}")

        except Exception as e:
            logging.error(f"Error parsing RESP: {e}")
            return None

    async def _read_integer(self, reader: asyncio.StreamReader) -> int:
        """Read an integer from the stream until CRLF"""
        line = await reader.readline()
        if not line.endswith(b'\r\n'):
            raise ValueError("Missing CRLF")
        return int(line[:-2])


class RedisProtocolHandler:
    """Handles Redis protocol responses"""

    @staticmethod
    def encode_simple_string(s: str) -> bytes:
        return f"+{s}\r\n".encode('utf-8')

    @staticmethod
    def encode_error(err: str) -> bytes:
        return f"-ERR {err}\r\n".encode('utf-8')

    @staticmethod
    def encode_integer(i: int) -> bytes:
        return f":{i}\r\n".encode('utf-8')

    @staticmethod
    def encode_bulk_string(s: Optional[str]) -> bytes:
        if s is None:
            return b"$-1\r\n"
        return f"${len(s)}\r\n{s}\r\n".encode('utf-8')

    @staticmethod
    def encode_array(items: List[bytes]) -> bytes:
        """Encode an array of RESP-encoded items"""
        if items is None:
            return b"*-1\r\n"
        array_string = f"*{len(items)}\r\n".encode('utf-8')
        return array_string + b"".join(items)
