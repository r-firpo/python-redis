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
        """Save the current dataset to RDB file"""
        try:
            with open(self.full_path, 'wb') as f:
                # Write header
                self._write_header(f)

                # Write aux fields
                self._write_aux_field(f, "redis-ver", "7.2.0")

                # Write redis-bits aux field
                f.write(struct.pack('B', self.REDIS_RDB_OPCODE_AUX))
                self._write_length_encoded_string(f, "redis-bits")
                f.write(b'\xc0\x40')

                # Write database selector
                f.write(struct.pack('BB', self.REDIS_RDB_OPCODE_SELECTDB, 0))

                # Write string encoded marker and count of pairs
                f.write(struct.pack('BB', self.REDIS_RDB_TYPE_STRING_ENCODED, len(data)))

                # Write key-value pairs
                current_time = int(time.time() * 1000)
                for key, value in data.items():
                    # Skip expired keys
                    if key in expires and expires[key] <= current_time:
                        continue

                    # Write expiration if exists
                    if key in expires:
                        # Write expiration marker and timestamp
                        f.write(b'\xfc\x00')  # Expiration marker
                        f.write(struct.pack('<Q', int(expires[key])))  # 8-byte timestamp
                        f.write(b'\x00')  # Extra byte as seen in format
                    else:
                        # Write bytes indicating no expiration
                        f.write(b'\x04\xfc')  # Non-expiring marker

                    # Write key and value
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

        # Write length prefix
        if length < 64:
            f.write(struct.pack('B', length))
        elif length < 16384:
            f.write(struct.pack('>H', length | 0x4000))
        else:
            f.write(b'\x80')
            f.write(struct.pack('>I', length)[1:])  # Write last 3 bytes

        # Write string data
        f.write(encoded)

    def _write_aux_field(self, f: BinaryIO, key: str, value: str) -> None:
        """Write auxiliary field"""
        f.write(struct.pack('B', self.REDIS_RDB_OPCODE_AUX))
        self._write_length_encoded_string(f, key)
        self._write_length_encoded_string(f, value)

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
                    logging.info(f"Processing type byte: {type_byte}")

                    if type_byte == self.REDIS_RDB_OPCODE_EOF:
                        break
                    elif type_byte == self.REDIS_RDB_OPCODE_SELECTDB:
                        db_num = self._read_length_encoded(f)
                        logging.info(f"Selected DB {db_num}")
                    elif type_byte == self.REDIS_RDB_OPCODE_AUX:
                        aux_key = self._read_length_encoded_string(f)
                        if aux_key == "redis-bits":
                            f.read(2)  # Skip redis-bits value
                        else:
                            aux_value = self._read_length_encoded_string(f)
                            logging.info(f"Aux field: {aux_key}={aux_value}")
                    elif type_byte == self.REDIS_RDB_TYPE_STRING_ENCODED:
                        # Read count
                        count = f.read(1)[0]
                        f.read(1)  # Skip duplicate count
                        logging.info(f"Reading {count} entries")

                        # Process each key-value pair
                        for i in range(count):
                            # Read expiry marker and timestamp
                            expiry_bytes = f.read(10)  # fc 00 + 8 bytes timestamp
                            if expiry_bytes[0:2] != b'\xfc\x00':
                                raise ValueError(f"Invalid expiry marker: {expiry_bytes[0:2].hex()}")

                            # Convert expiry timestamp (last 8 bytes)
                            expire_at = int.from_bytes(expiry_bytes[2:], byteorder='little')

                            # Read key length and key
                            key_len = f.read(1)[0]
                            key_bytes = f.read(key_len)
                            key = key_bytes.decode('utf-8')

                            # Read value length and value
                            value_len = f.read(1)[0]
                            value_bytes = f.read(value_len)
                            value = value_bytes.decode('utf-8')

                            current_time = int(time.time())
                            logging.info(f"Key: {key}, Value: {value}, Expires: {expire_at}, Current: {current_time}")

                            # The timestamp in RDB is in milliseconds
                            if expire_at > current_time:
                                data[key] = value
                                expires[key] = expire_at
                                logging.info(f"Stored key-value pair: {key}={value}")
                            else:
                                logging.info(f"Skipped expired key: {key}")

                logging.info(f"Final data: {data}")
                return data, expires

        except Exception as e:
            logging.error(f"Error loading RDB file: {e}")
            logging.error(f"Exception details:", exc_info=True)
            return None

    def _read_length_encoded_string(self, f: BinaryIO) -> str:
        """Read a length-encoded string"""
        length = f.read(1)[0]
        return f.read(length).decode('utf-8')

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
        else:
            return struct.unpack('>I', f.read(4))[0]


    def _write_header(self, f: BinaryIO) -> None:
        """Write RDB file header"""
        f.write(b'REDIS')
        f.write(str(self.REDIS_RDB_VERSION).zfill(4).encode())
