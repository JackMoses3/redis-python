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
    """Loads a single key-value pair from the RDB file if it exists."""
    rdb_path = os.path.join(config["dir"], config["dbfilename"])
    if not os.path.exists(rdb_path):
        print("No RDB file found, treating database as empty.")
        return

    with open(rdb_path, "rb") as f:
        data = f.read()

    try:
        pos = 9  # Skip 'REDIS' + version header (9 bytes)
        found_key = False

        while pos < len(data):
            byte = data[pos]
            pos += 1

            if byte == 0xFE:  # Database selector (marks key-value start)
                pos += 1  # Skip database number
                continue

            if byte in {0xFD, 0xFC}:  # Expiry times (seconds/milliseconds)
                pos += 4 if byte == 0xFD else 8
                continue

            if 0x00 <= byte <= 0x06:  # Object type (string, list, etc.)
                if pos >= len(data):
                    break

                # Extract key length and key
                key_length, key_length_size = parse_length_encoding(data, pos)
                pos += key_length_size
                if pos + key_length > len(data):
                    break

                key = data[pos:pos+key_length].decode("utf-8", errors="ignore").strip()
                pos += key_length

                # Extract value length and value
                value_length, value_length_size = parse_length_encoding(data, pos)
                pos += value_length_size
                if pos + value_length > len(data):
                    break

                value = data[pos:pos+value_length].decode("utf-8", errors="ignore").strip()
                pos += value_length

                if key and value and key.isprintable():
                    print(f"Extracted key-value from RDB: {key} -> {value}")
                    store[key] = (value, None)  # Store value without expiry
                    found_key = True
                    return  # Return after storing the first valid key

        if not found_key:
            print("No valid key-value found in RDB file.")
            store.clear()  # Ensure no stale data is left

    except Exception as e:
        print(f"Error reading RDB file: {e}")
        store.clear()

def parse_length_encoding(data, pos):
    """Parses the length encoding in an RDB file."""
    first_byte = data[pos]

    if first_byte < 0x80:  # 7-bit integer
        return first_byte, 1
    elif first_byte == 0x81:  # 8-bit length
        return data[pos + 1], 2
    elif first_byte == 0x82:  # 16-bit length (Big-Endian)
        return struct.unpack(">H", data[pos+1:pos+3])[0], 3
    elif first_byte == 0x83:  # 32-bit length (Big-Endian)
        return struct.unpack(">I", data[pos+1:pos+5])[0], 5
    elif first_byte == 0x80:  # 32-bit length (Big-Endian for strings)
        return struct.unpack(">I", data[pos+1:pos+5])[0], 5
    elif first_byte == 0x81:  # 64-bit length (Big-Endian for strings)
        return struct.unpack(">Q", data[pos+1:pos+9])[0], 9
    else:
        return 0, 1  # Default case (unexpected data)

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