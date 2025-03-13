import socket
import threading
import time
import sys
import os

store = {}  # Key-value storage with expiry support
config = {
    "dir": "/tmp",  # Default values
    "dbfilename": "dump.rdb"
}

def parse_args():
    """Parses command-line arguments for --dir and --dbfilename."""
    args = sys.argv[1:]
    for i in range(len(args) - 1):
        if args[i] == "--dir":
            config["dir"] = args[i + 1]
        elif args[i] == "--dbfilename":
            config["dbfilename"] = args[i + 1]

def parse_resp(command: str) -> list[str]:
    """Parses a RESP-encoded command string into a list of arguments."""
    parts = command.strip().split("\r\n")
    if not parts or not parts[0].startswith("*"):
        return []

    num_args = int(parts[0][1:])
    args = []
    i = 1
    while i < len(parts) and len(args) < num_args:
        if parts[i].startswith("$"):
            arg_len = int(parts[i][1:])
            if i + 1 < len(parts):
                args.append(parts[i + 1])
            i += 2
        else:
            i += 1  # Skip malformed input

    return args

def load_rdb_file():
    """Loads a single key from the RDB file if it exists."""
    rdb_path = os.path.join(config["dir"], config["dbfilename"])
    if not os.path.exists(rdb_path):
        return  # No RDB file, treat as empty database

    with open(rdb_path, "rb") as f:
        data = f.read()

    # A minimal RDB parser to extract a single key
    try:
        key = extract_key_from_rdb(data)
        if key and key not in store:
            store[key] = (None, None)  # Store key with no value or expiry
    except Exception as e:
        print(f"Failed to load RDB file: {e}")

def extract_key_from_rdb(data):
    """Parses RDB binary format to extract a single key."""
    try:
        key_start = data.find(b"\xFE") + 2  # Example: Looking for Redis key marker
        key_end = data.find(b"\xFF", key_start)
        key = data[key_start:key_end].decode("utf-8")
        return key
    except Exception:
        return None

def connect(connection: socket.socket) -> None:
    global store, config
    with connection:
        buffer = ""
        while True:
            try:
                data = connection.recv(1024).decode()
                if not data:
                    break

                buffer += data

                while "\r\n" in buffer:
                    print(f"received - {buffer.strip()}")

                    args = parse_resp(buffer)
                    if not args:
                        buffer = ""
                        continue  # Ignore invalid commands

                    cmd = args[0].upper()  # Case-insensitive command handling
                    response: str = ""

                    if cmd == "PING":
                        response = "+PONG\r\n"
                    elif cmd == "ECHO" and len(args) > 1:
                        msg = args[1]
                        response = f"${len(msg)}\r\n{msg}\r\n"
                    elif cmd == "SET" and len(args) > 2:
                        key, value = args[1], args[2]
                        expiry = None
                        if len(args) > 4 and args[3].upper() == "PX":
                            try:
                                expiry = int(args[4])
                                expiry = time.time() * 1000 + expiry  # Convert to absolute expiry time
                            except ValueError:
                                response = "-ERR PX value must be an integer\r\n"

                        store[key] = (value, expiry)
                        response = "+OK\r\n"
                    elif cmd == "GET" and len(args) > 1:
                        key = args[1]
                        if key in store:
                            value, expiry = store[key]
                            if expiry and time.time() * 1000 > expiry:
                                del store[key]  # Remove expired key
                                response = "$-1\r\n"
                            else:
                                response = f"${len(value)}\r\n{value}\r\n"
                        else:
                            response = "$-1\r\n"  # Null bulk string for missing keys
                    elif cmd == "CONFIG" and len(args) == 3 and args[1].upper() == "GET":
                        param = args[2]
                        if param in config:
                            value = config[param]
                            response = f"*2\r\n${len(param)}\r\n{param}\r\n${len(value)}\r\n{value}\r\n"
                        else:
                            response = "$-1\r\n"  # Null response for unknown parameters
                    else:
                        response = "-ERR unknown command\r\n"

                    if response:
                        print(f"responding with - {response.strip()}")
                        connection.sendall(response.encode())

                    buffer = ""  # Clear buffer after processing a full command

            except Exception as e:
                print(f"Error handling request: {e}")
                break

def main() -> None:
    parse_args()  # Parse CLI arguments
    load_rdb_file()  # Load data from RDB file
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    while True:
        connection: socket.socket
        address: tuple[str, int]
        connection, address = server_socket.accept()
        print(f"accepted connection - {address[0]}:{str(address[1])}")
        thread: threading.Thread = threading.Thread(target=connect, args=[connection])
        thread.start()

if __name__ == "__main__":
    main()