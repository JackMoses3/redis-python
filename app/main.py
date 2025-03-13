import socket
import threading
import time
import sys
import os
import struct

store = {}  # Key-value storage with expiry support
config = {
    "dir": "/tmp",  # Default values
    "dbfilename": "dump.rdb"
}

def load_rdb_file():
    """Loads a single key from the RDB file if it exists."""
    rdb_path = os.path.join(config["dir"], config["dbfilename"])
    if not os.path.exists(rdb_path):
        print("No RDB file found, treating database as empty.")
        return  # No RDB file, treat as empty database

    with open(rdb_path, "rb") as f:
        data = f.read()

    try:
        key = extract_key_from_rdb(data)
        if key:
            print(f"Extracted key from RDB: {key}")
            store[key] = (None, None)  # Store key with no value or expiry
        else:
            print("No valid key found in RDB file.")
    except Exception as e:
        print(f"Failed to load RDB file: {e}")

def extract_key_from_rdb(data):
    """Parses an RDB file and extracts the first key."""
    try:
        pos = 9  # Skip the RDB header (first 9 bytes)
        
        while pos < len(data):
            byte = data[pos]
            pos += 1

            if byte == 0xFE:  # FE marks a database selector
                db_number = data[pos]
                pos += 1
                continue

            if byte == 0xFD:  # FD marks an expiry time (seconds)
                pos += 4  # Skip 4-byte expiry time
                continue

            if byte == 0xFC:  # FC marks an expiry time (milliseconds)
                pos += 8  # Skip 8-byte expiry time
                continue

            if 0x00 <= byte <= 0xFA:  # This is an encoded object type
                key_length = byte
            elif byte == 0xFB:  # FB marks an 8-bit length
                key_length = data[pos]
                pos += 1
            elif byte == 0xFC:  # FC marks a 16-bit length
                key_length = struct.unpack(">H", data[pos:pos+2])[0]
                pos += 2
            elif byte == 0xFD:  # FD marks a 32-bit length
                key_length = struct.unpack(">I", data[pos:pos+4])[0]
                pos += 4
            else:
                continue  # Skip unknown byte

            # Extract the key
            key = data[pos:pos+key_length].decode("utf-8", errors="ignore")
            pos += key_length
            return key

    except Exception as e:
        print(f"Error extracting key from RDB: {e}")
        return None

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
                    elif cmd == "KEYS" and len(args) == 2 and args[1] == "*":
                        keys = list(store.keys())
                        print(f"Stored keys: {keys}")  # Debugging log to check stored keys
                        response = f"*{len(keys)}\r\n" + "".join(f"${len(k)}\r\n{k}\r\n" for k in keys)
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
    load_rdb_file()  # Load RDB file on startup
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