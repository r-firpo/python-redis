import os
import struct
import time
import logging
from typing import Dict, Optional, BinaryIO, Tuple

### Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(funcName)s:%(lineno)d - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

class RDBHandler:
    """Handles RDB file persistence for Redis compatible server"""

    REDIS_RDB_VERSION = 6  # Using version 6 format

    # RDB file format constants
    REDIS_RDB_OPCODE_EOF = 255
    REDIS_RDB_OPCODE_SELECTDB = 254
    REDIS_RDB_OPCODE_EXPIRETIME_MS = 252
    REDIS_RDB_TYPE_STRING = 0

    def __init__(self, dir_path: str, filename: str):
        self.dir_path = dir_path
        self.filename = filename
        self.full_path = os.path.join(dir_path, filename)

        # Ensure directory exists
        os.makedirs(dir_path, exist_ok=True)

    def save(self, data: Dict[str, str], expires: Dict[str, float]) -> bool:
        """
        Save the current dataset to RDB file
        Returns True if successful, False otherwise
        """
        try:
            with open(self.full_path, 'wb') as f:
                # Write header
                self._write_header(f)

                # Write database selector (using 0 as default)
                f.write(struct.pack('BB', self.REDIS_RDB_OPCODE_SELECTDB, 0))

                # Write key-value pairs
                current_time = int(time.time() * 1000)
                for key, value in data.items():
                    # Check if key has a valid non-expired timestamp
                    if key in expires and expires[key] <= current_time:
                        continue

                    # Write expiry if exists
                    if key in expires:
                        f.write(struct.pack('B', self.REDIS_RDB_OPCODE_EXPIRETIME_MS))
                        f.write(struct.pack('<Q', int(expires[key])))

                    # Write type
                    f.write(struct.pack('B', self.REDIS_RDB_TYPE_STRING))

                    # Write key-value
                    self._write_string(f, key)
                    self._write_string(f, value)

                # Write EOF marker
                f.write(struct.pack('B', self.REDIS_RDB_OPCODE_EOF))

            return True

        except Exception as e:
            logging.error(f"Error saving RDB file: {e}")
            return False

    def load(self) -> Optional[Tuple[Dict[str, str], Dict[str, float]]]:
        """
        Load dataset from RDB file
        Returns tuple of (data, expires) if successful, None if file doesn't exist or is invalid
        """
        if not os.path.exists(self.full_path):
            return None

        try:
            with open(self.full_path, 'rb') as f:
                if not self._verify_header(f):
                    raise ValueError("Invalid RDB file format")

                data = {}
                expires = {}

                while True:
                    # Read type byte
                    type_byte = f.read(1)
                    if not type_byte:  # EOF
                        break
                    type_byte = type_byte[0]

                    if type_byte == self.REDIS_RDB_OPCODE_EOF:
                        break
                    elif type_byte == self.REDIS_RDB_OPCODE_SELECTDB:
                        # Skip database selector
                        f.read(1)
                    elif type_byte == self.REDIS_RDB_OPCODE_EXPIRETIME_MS:
                        # Read expiry
                        expire_time = struct.unpack('<Q', f.read(8))[0]

                        # Read type byte for the actual value
                        value_type = f.read(1)[0]
                        if value_type != self.REDIS_RDB_TYPE_STRING:
                            raise ValueError(f"Unexpected value type: {value_type}")

                        # Read key-value
                        key = self._read_string(f)
                        value = self._read_string(f)

                        # Only add if not expired
                        current_time = int(time.time() * 1000)
                        if expire_time > current_time:
                            data[key] = value
                            expires[key] = expire_time
                    elif type_byte == self.REDIS_RDB_TYPE_STRING:
                        # Regular key-value
                        key = self._read_string(f)
                        value = self._read_string(f)
                        data[key] = value
                    else:
                        raise ValueError(f"Unexpected RDB type byte: {type_byte}")

                return data, expires

        except Exception as e:
            logging.error(f"Error loading RDB file: {e}")
            return None

    def _write_header(self, f: BinaryIO) -> None:
        """Write RDB file header"""
        f.write(b'REDIS')
        f.write(str(self.REDIS_RDB_VERSION).zfill(4).encode())

    def _verify_header(self, f: BinaryIO) -> bool:
        """Verify RDB file header"""
        magic = f.read(5)
        if magic != b'REDIS':
            return False
        version = f.read(4)
        try:
            version_num = int(version.decode())
            return version_num <= self.REDIS_RDB_VERSION
        except (ValueError, UnicodeDecodeError):
            return False

    def _write_string(self, f: BinaryIO, s: str) -> None:
        """Write length prefixed string"""
        encoded = s.encode('utf-8')
        f.write(struct.pack('<I', len(encoded)))
        f.write(encoded)

    def _read_string(self, f: BinaryIO) -> str:
        """Read length prefixed string"""
        length = struct.unpack('<I', f.read(4))[0]
        return f.read(length).decode('utf-8')