# Python Redis Server Implementation

A robust, asynchronous Redis server implementation in Python that supports core Redis functionality and replication. This server implements the Redis Serialization Protocol (RESP) and includes RDB persistence with version 11 compatibility.

## Features

- **Asynchronous Implementation**: Built using Python's `asyncio` library for high-performance I/O operations
- **RESP Protocol Support**: Full implementation of the Redis Serialization Protocol
- **RDB Persistence**: Compatible with Redis RDB version 11
  - Supports saving and loading of data
  - Automatic periodic saves
  - Configurable save location
- **Master-Replica Replication**:
  - Support for master-replica topology
  - Replica synchronization with RDB transfer
  - PSYNC implementation
  - Replication backlog management
  - WAIT command for write acknowledgment
- **Connection Management**:
  - Concurrent client handling
  - Connection monitoring
  - Automatic cleanup of inactive connections

### Supported Commands

- Basic Operations:
  - `PING` - Test connection
  - `ECHO` - Echo input message
  - `SET` - Set key-value pair (with PX option for expiration)
  - `GET` - Get value by key
  - `DEL` - Delete key
  - `KEYS` - List all keys (currently only supports '*' pattern)
- Configuration:
  - `CONFIG GET` - Get configuration parameters
- Replication:
  - `REPLCONF` - Replication configuration
  - `PSYNC` - Replication synchronization
  - `WAIT` - Wait for write acknowledgment from replicas
- Info:
  - `INFO replication` - Get replication status information

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd python-redis-server
```

2. Install requirements:
```bash
pip install -r requirements.txt
```

## Usage

### Starting the Server

Basic usage:
```bash
python -m app.main
```

With custom configuration:
```bash
python -m app.main --port 6380 --dir /path/to/rdb --dbfilename dump.rdb
```

Starting as a replica:
```bash
python -m app.main --replicaof "localhost 6379"
```

### Configuration Options

- `--port`: Server port (default: 6379)
- `--dir`: Directory for RDB files (default: /tmp/redis-files-rdb)
- `--dbfilename`: RDB filename (default: dump.rdb)
- `--replicaof`: Configure as replica (format: "host port")

### Testing with redis-cli

1. Start the server:
```bash
python -m app.main --port 6379
```

2. Connect using redis-cli:
```bash
redis-cli -p 6379
```

Example commands:
```redis
PING
SET mykey "Hello World"
GET mykey
SET mykey "Hello" PX 5000
KEYS *
DEL mykey
```

### Replication Example

1. Start master instance:
```bash
python -m app.main --port 6379
```

2. Start replica instance:
```bash
python -m app.main --port 6380 --replicaof "localhost 6379"
```

3. Test replication:
```bash
# Connect to master
redis-cli -p 6379
> SET key1 "value1"
OK

# Connect to replica
redis-cli -p 6380
> GET key1
"value1"
```

## Running Tests

The project includes comprehensive test coverage:

```bash
# Run all tests
pytest

# Run specific test file
pytest tests/test_redis_server.py

# Run with verbose output
pytest -v

# Run with logging
pytest --log-cli-level=INFO
```

## Development Notes

- The server uses asynchronous programming with Python's `asyncio` library
- All Redis commands are implemented following RESP protocol specifications
- RDB persistence follows Redis RDB version 11 format
- Connection handling supports concurrent clients
- Replication follows Redis replication protocol

## Project Structure

- `async_TCP_redis_server.py`: Main server implementation
- `connection_manager.py`: Client connection management
- `redis_data_store.py`: Data storage and persistence
- `redis_parser.py`: RESP protocol parser
- `redis_master_handler.py`: Master-side replication logic
- `replication_manager.py`: Replica-side replication logic
- `redis_rdb_handler.py`: RDB file handling

## Error Handling

The server implements robust error handling for:
- Invalid commands
- Malformed RESP protocol messages
- Connection issues
- Replication failures
- RDB persistence errors

## Contributing

Contributions are welcome! Please ensure:
1. Tests are added for new features
2. Documentation is updated
3. Code follows project style guidelines
4. All tests pass before submitting PR

