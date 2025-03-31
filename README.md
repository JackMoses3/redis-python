# Redis Replica Project

This is a simplified, custom implementation of a Redis-like key-value store in Python. It includes basic support for key-value storage with expiry, a subset of the Redis command set (such as `PING`, `ECHO`, `GET`, `SET`, `DEL`, `CONFIG GET`, and `KEYS`), and rudimentary replication functionality that mimics some of Redis’ master–replica features.

## Features

- **Key-Value Store:**  
  Supports storing string values with optional expiry times. Expired keys are automatically skipped/removed.

- **Basic Commands:**  
  Implements commands like:
  - `PING` / `ECHO` for connectivity checks.
  - `GET` / `SET` for retrieving and updating keys.
  - `DEL` to remove keys.
  - `CONFIG GET` to retrieve configuration parameters.
  - `KEYS *` to list all non-expired keys.

- **RDB File Loading:**  
  On startup, the server loads key-value pairs (with expiry support) from an RDB file if it exists in the specified directory.

- **Replication Support:**  
  When run as a replica (using the `--replicaof` argument), the server connects to a master instance, receives write commands (such as `SET` and `DEL`), and sends acknowledgments using a simplified version of the Redis replication protocol. It also supports the `PSYNC` command for initial synchronization.

- **RESP Protocol:**  
  The project uses the Redis Serialization Protocol (RESP) for command parsing and formatting responses.

## Prerequisites

- **Python 3.6+**  
  This project uses only Python’s standard libraries such as `socket`, `threading`, `struct`, `time`, `os`, and `sys`.

## Installation

1. **Clone the repository** (if applicable) or copy the source file into your working directory.

2. **Ensure you have Python 3 installed.**

## Usage

Run the project using the Python interpreter. The server accepts several optional command-line arguments:

```bash
python redis_project.py [--dir <directory>] [--dbfilename <filename>] [--port <port>] [--replicaof <master_host> <master_port>]
