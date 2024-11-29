import os
import struct
import time
import logging
from datetime import datetime
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
    BYTE_FE = REDIS_RDB_OPCODE_SELECTDB = 254  # FE
    REDIS_RDB_OPCODE_EXPIRETIME_MS = 252
    REDIS_RDB_OPCODE_AUX = 250
    REDIS_RDB_TYPE_STRING = 0
    BYTE_FB = REDIS_RDB_TYPE_STRING_ENCODED = 251  # FB

    def __init__(self, dir_path: str, filename: str):
        self.dir_path = dir_path
        self.filename = filename
        self.full_path = os.path.join(dir_path, filename)

        # Ensure directory exists
        os.makedirs(dir_path, exist_ok=True)

    def save(self, data: Dict[str, str], expires: Dict[str, float]) -> bool:
        """Save the current dataset to RDB file with detailed logging"""
        try:
            with open(self.full_path, 'wb') as f:
                # Write header
                self._write_header(f)
                logging.info(f"Header written: REDIS{self.REDIS_RDB_VERSION}")

                # Write auxiliary fields
                self._write_aux_field(f, "redis-ver", "7.2.0")
                logging.info(f"Aux field written: redis-ver=7.2.0")
                self._write_aux_field(f, "redis-bits", "64")
                logging.info(f"Aux field written: redis-bits=64")

                # Write database selector
                f.write(struct.pack('BB', self.BYTE_FE, 0))
                logging.info("Database selector written: SELECTDB 0")

                # prune expired keys before comitting the length of expired


                # Write key-value pairs
                f.write(struct.pack('B', self.BYTE_FB))  # Marker for string encoded section
                f.write(struct.pack('B', len(data)))  # Number of key-value pairs
                f.write(struct.pack('B', len(expires)))  # length of expired DB

                logging.info(f"Key-value pair section marker written with {len(data)} entries and {len(expires)} expirations")

                for key, value in data.items():

                    if key in expires:
                        # Write expiration marker
                        f.write(struct.pack('B', self.REDIS_RDB_OPCODE_EXPIRETIME_MS))  # Expiry marker (0xFC)
                        expiry_timestamp = int(expires[key])
                        f.write(struct.pack('<Q', expiry_timestamp))  # 8-byte timestamp in little-endian
                        logging.info(f"Expiry marker written for key {key}: {expiry_timestamp}")

                    # Write type byte for non-expiring strings or value type
                    f.write(struct.pack('B', self.REDIS_RDB_TYPE_STRING))  # String type marker
                    logging.info(f"Type marker for key {key} written: {self.REDIS_RDB_TYPE_STRING}")

                    # Write key
                    self._write_length_encoded_string(f, key)
                    logging.info(f"Key written: {key}")

                    # Write value
                    self._write_length_encoded_string(f, value)
                    logging.info(f"Value written: {value}")

                # Write EOF marker
                f.write(struct.pack('B', self.REDIS_RDB_OPCODE_EOF))
                logging.info("EOF marker written")
                # TODO add checksum validation

                return True

        except Exception as e:
            logging.error(f"Error saving RDB file: {e}")
            return False

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
        #TODO add checksum validation to ensure valid RDB file?
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
                        try:
                            # Read count
                            count = f.read(1)[0]
                            second_byte = f.read(1)[0]  # Skip expiry count (num of items in expiry db)
                            logging.info(f"Reading {count} entries (second byte: {second_byte})")

                            # Process each key-value pair
                            for i in range(count):
                                try:
                                    # Read and log current position
                                    pos_before = f.tell()
                                    next_byte = f.read(1)
                                    logging.info(
                                        f"Reading entry {i} at position {pos_before}, next byte: {next_byte.hex()}")
                                    f.seek(pos_before)  # Go back to start

                                    # Check for expiry marker
                                    marker = f.read(1)[0]
                                    if marker == self.REDIS_RDB_OPCODE_EXPIRETIME_MS:  # 0xFC
                                        logger.info(f"Found MS expiry marker: {marker}")
                                        # Read 8-byte timestamp
                                        expire_at = int.from_bytes(f.read(8), byteorder='little')

                                        # Read value type
                                        value_type = f.read(1)[0]
                                        if value_type != self.REDIS_RDB_TYPE_STRING:
                                            raise ValueError(f"Unsupported value type: {value_type}")

                                    else:
                                        # No expiry - marker was the value type
                                        value_type = marker
                                        expire_at = None
                                        if value_type != self.REDIS_RDB_TYPE_STRING:
                                            raise ValueError(f"Unsupported value type: {value_type}")

                                    # Read key length and key
                                    key_len = f.read(1)[0]
                                    key = f.read(key_len).decode('utf-8')

                                    # Read value length and value
                                    value_len = f.read(1)[0]
                                    value = f.read(value_len).decode('utf-8')

                                    current_time = int(time.time())
                                    logging.info(
                                        f"Entry {i}: Key: {key}, Value: {value}, " +
                                        f"Expires: {expire_at}, Current: {current_time}"
                                    )

                                    if expire_at is None or current_time < expire_at:
                                        data[key] = value
                                        if expire_at is not None:
                                            expires[key] = expire_at
                                        logging.info(f"Stored key {key}")
                                    else:
                                        logging.info(f"Skipped expired key {key}")

                                except Exception as e:
                                    logging.error(f"Error processing entry {i}: {e}")
                                    raise

                        except Exception as e:
                            logging.error(f"Error processing string encoded section: {e}")
                            raise

                logging.info(f"Final data: {data}")
                return data, expires

        except Exception as e:
            logging.error(f"Error loading RDB file: {e}")
            logging.error("Exception details:", exc_info=True)
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
