import os
import struct
import time
import logging
from typing import Dict, Optional, BinaryIO, Tuple

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(funcName)s:%(lineno)d - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


class RDBHandler:
    """Handles RDB file persistence for Redis compatible server"""

    REDIS_RDB_VERSION = 11

    # RDB file format constants
    REDIS_RDB_OPCODE_EOF = 255
    REDIS_RDB_OPCODE_SELECTDB = 254
    REDIS_RDB_OPCODE_EXPIRETIME_MS = 252
    REDIS_RDB_OPCODE_AUX = 250
    REDIS_RDB_TYPE_STRING = 0
    REDIS_RDB_TYPE_STRING_ENCODED = 251

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

                # Write aux fields (Redis version info)
                self._write_aux_field(f, "redis-ver", "7.2.0")
                self._write_aux_field(f, "redis-bits", "64")

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

                    # Write type (using encoded string type for compatibility)
                    f.write(struct.pack('B', self.REDIS_RDB_TYPE_STRING_ENCODED))

                    # Write key-value with length encoding
                    self._write_length_encoded_string(f, key)
                    self._write_length_encoded_string(f, value)

                # Write EOF marker
                f.write(struct.pack('B', self.REDIS_RDB_OPCODE_EOF))

            return True

        except Exception as e:
            logging.error(f"Error saving RDB file: {e}")
            return False

    def _write_aux_field(self, f: BinaryIO, key: str, value: str) -> None:
        """Write auxiliary field"""
        f.write(struct.pack('B', self.REDIS_RDB_OPCODE_AUX))
        self._write_length_encoded_string(f, key)
        self._write_length_encoded_string(f, value)

    def _write_length_encoded_string(self, f: BinaryIO, s: str) -> None:
        """Write a length-encoded string"""
        encoded = s.encode('utf-8')
        length = len(encoded)

        # Write length using the same encoding as Redis
        if length < 64:
            f.write(struct.pack('B', length))
        elif length < 16384:
            f.write(struct.pack('>H', length | 0x4000))
        else:
            f.write(struct.pack('B', 0x80))
            f.write(struct.pack('>I', length)[1:])  # Write last 3 bytes

        # Write the actual string
        f.write(encoded)

    def _read_length_encoded(self, f: BinaryIO) -> int:
        """Read a length-encoded integer"""
        first_byte = f.read(1)[0]
        if (first_byte & 0xC0) == 0:
            return first_byte & 0x3F
        elif (first_byte & 0xC0) == 0x40:
            next_byte = f.read(1)[0]
            return ((first_byte & 0x3F) << 8) | next_byte
        elif (first_byte & 0xC0) == 0x80:
            length = 0
            for _ in range(3):
                length = (length << 8) | f.read(1)[0]
            return length
        elif first_byte == 0xC0:
            length = 0
            for _ in range(4):
                length = (length << 8) | f.read(1)[0]
            return length
        else:
            raise ValueError(f"Invalid length encoding byte: {first_byte}")

    def load(self) -> Optional[Tuple[Dict[str, str], Dict[str, float]]]:
        """Load dataset from RDB file"""
        if not os.path.exists(self.full_path):
            return None

        try:
            with open(self.full_path, 'rb') as f:
                if not self._verify_header(f):
                    raise ValueError("Invalid RDB file format")

                data = {}
                expires = {}

                while True:
                    type_byte = f.read(1)
                    if not type_byte:  # EOF
                        break
                    type_byte = type_byte[0]

                    if type_byte == self.REDIS_RDB_OPCODE_EOF:
                        break
                    elif type_byte == self.REDIS_RDB_OPCODE_SELECTDB:
                        db_num = self._read_length_encoded(f)
                        logging.info(f"Selected DB {db_num}")
                    elif type_byte == self.REDIS_RDB_OPCODE_AUX:
                        # For aux fields, need to read both key and value as length-encoded strings
                        aux_key = self._read_length_encoded_string(f)
                        if aux_key == "redis-bits":
                            # Handle redis-bits specially - it's encoded as a 64-bit value
                            f.read(2)  # Skip the encoding bytes (c0 40)
                        else:
                            aux_value = self._read_length_encoded_string(f)
                            logging.info(f"Aux field: {aux_key}={aux_value}")
                    elif type_byte == self.REDIS_RDB_TYPE_STRING_ENCODED:
                        key = self._read_length_encoded_string(f)
                        value = self._read_length_encoded_string(f)
                        data[key] = value
                    elif type_byte == self.REDIS_RDB_OPCODE_EXPIRETIME_MS:
                        expire_time = struct.unpack('<Q', f.read(8))[0]
                        key_type = f.read(1)[0]
                        if key_type == self.REDIS_RDB_TYPE_STRING_ENCODED:
                            key = self._read_length_encoded_string(f)
                            value = self._read_length_encoded_string(f)
                            current_time = int(time.time() * 1000)
                            if expire_time > current_time:
                                data[key] = value
                                expires[key] = expire_time
                    else:
                        raise ValueError(f"Unexpected RDB type byte: {type_byte}")

                logging.info(f"Loaded {len(data)} keys from RDB file")
                return data, expires

        except Exception as e:
            logging.error(f"Error loading RDB file: {e}")
            return None

    def _read_length_encoded_string(self, f: BinaryIO) -> str:
        """Read a length-encoded string"""
        try:
            length = self._read_length_encoded(f)
            return f.read(length).decode('utf-8')
        except UnicodeDecodeError as e:
            logging.error(f"Failed to decode string at position {f.tell()}: {e}")
            raise

    def _verify_header(self, f: BinaryIO) -> bool:
        """Verify RDB file header"""
        magic = f.read(5)
        if magic != b'REDIS':
            return False
        version = f.read(4)
        try:
            version_num = int(version.decode())
            # Accept any version up to our supported version
            return version_num <= self.REDIS_RDB_VERSION
        except (ValueError, UnicodeDecodeError):
            return False

    def _write_header(self, f: BinaryIO) -> None:
        """Write RDB file header"""
        f.write(b'REDIS')
        f.write(str(self.REDIS_RDB_VERSION).zfill(4).encode())
