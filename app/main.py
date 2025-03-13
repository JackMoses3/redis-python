import socket
import threading
import time
import sys
import os
import struct

store = {}  # Key-value storage with expiry support
config = {
    "dir": "/tmp",  # Default values
    "dbfilename": "dump.rdb",
    "port": 6379  # Default port
}

def load_rdb_file():
    """Loads key-value pairs from the RDB file if it exists."""
    rdb_path = os.path.join(config["dir"], config["dbfilename"])
    if not os.path.exists(rdb_path):
        print("No RDB file found, treating database as empty.")
        return

    with open(rdb_path, "rb") as f:
        data = f.read()

    try:
        pos = 9  # Skip 'REDIS' + version header (9 bytes)
        expiry = None

        while pos < len(data):
            byte = data[pos]
            pos += 1

            if byte == 0xFA:  # Auxiliary fields (skip them)
                aux_key_length, key_size = parse_length_encoding(data, pos)
                pos += key_size + aux_key_length + parse_length_encoding(data, pos + key_size)[1]
                continue

            if byte == 0xFE:  # Database selector
                db_number, db_size = parse_length_encoding(data, pos)
                pos += db_size
                print(f"Selecting database {db_number}")
                continue

            if byte in [0xFD, 0xFC]:  # Expiry time (Little Endian Fix)
                expiry_size = 8 if byte == 0xFC else 4
                if pos + expiry_size > len(data):
                    print("Error: Not enough data for expiry time.")
                    break

                if expiry_size == 8:  # Millisecond precision expiry
                    expiry_value = struct.unpack("<Q", data[pos:pos+8])[0]  # Little-endian millis
                else:  # Second precision expiry, convert to millis
                    expiry_value = struct.unpack("<I", data[pos:pos+4])[0] * 1000  # Little-endian seconds to millis
                pos += expiry_size
                expiry = expiry_value
                continue

            if byte == 0xFB:  # RESIZEDB opcode
                main_size, main_size_bytes = parse_length_encoding(data, pos)
                pos += main_size_bytes
                expiry_size, expiry_size_bytes = parse_length_encoding(data, pos)
                pos += expiry_size_bytes
                continue

            if byte == 0xFF:  # End of RDB file
                print("End of RDB file reached.")
                break

            if 0x00 <= byte <= 0x06:  # Object type (String, List, etc.)
                key_length, key_length_size = parse_length_encoding(data, pos)
                pos += key_length_size

                if pos + key_length > len(data):
                    print(f"Error: Key length exceeds data size at position {pos}.")
                    break

                key = data[pos:pos+key_length].decode("utf-8", errors="ignore").strip()
                pos += key_length

                value_length, value_length_size = parse_length_encoding(data, pos)
                pos += value_length_size

                if pos + value_length > len(data):
                    print(f"Error: Value length exceeds data size at position {pos}.")
                    break

                value = data[pos:pos+value_length].decode("utf-8", errors="ignore").strip()
                pos += value_length

                if expiry is not None and expiry < int(time.time() * 1000):  # Expired
                    print(f"Skipping expired key: {key}")
                    continue
                else:
                    store[key] = (value, expiry)
                    print(f"Loaded key-value pair: {key} -> {value} with expiry {expiry}")
                expiry = None
                continue

            print(f"Encountered unknown byte: {byte}. Skipping...")
            continue

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
    """Parses command-line arguments for --dir, --dbfilename, --port, and --replicaof."""
    args = sys.argv[1:]
    for i in range(len(args) - 1):
        if args[i] == "--dir":
            config["dir"] = args[i + 1]
        elif args[i] == "--dbfilename":
            config["dbfilename"] = args[i + 1]
        elif args[i] == "--port":
            try:
                config["port"] = int(args[i + 1])
            except ValueError:
                print("Invalid port number. Using default port 6379.")
                config["port"] = 6379
        elif args[i] == "--replicaof":
            config["replicaof"] = args[i + 1].split()
    if "port" not in config:
        config["port"] = 6379  # Default to 6379 if not set

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
                    elif cmd == "INFO" and len(args) == 2 and args[1].upper() == "REPLICATION":
                        role = "slave" if "replicaof" in config else "master"
                        master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"  # Hardcoded replication ID
                        master_repl_offset = 0  # Initial offset
 
                        info_response = (
                            f"role:{role}\r\n"
                            f"master_replid:{master_replid}\r\n"
                            f"master_repl_offset:{master_repl_offset}\r\n"
                        )
                        info_bytes = info_response.encode("utf-8")
                        response = f"${len(info_response)}\r\n{info_response}"
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
                        current_time = int(time.time() * 1000)
                        
                        if key in store:
                            value, expiry = store[key]
                            print(f"Checking expiry for key {key}: expiry={expiry}, current_time={current_time}")
                            if expiry is not None and current_time >= expiry:
                                print(f"Key {key} has expired. Removing from store.")
                                del store[key]
                                response = "$-1\r\n"  # Null bulk string for expired keys
                            else:
                                response = f"${len(value)}\r\n{value}\r\n"  # Return bulk string
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
                        valid_keys = []
                        for k in list(store.keys()):
                            value, expiry = store[k]
                            if expiry and time.time() * 1000 > expiry:
                                del store[k]
                            else:
                                valid_keys.append(k)
                        print(f"Stored keys (non-expired): {valid_keys}")  # Debugging log
                        response = f"*{len(valid_keys)}\r\n" + "".join(f"${len(k)}\r\n{k}\r\n" for k in valid_keys)
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
    server_socket = socket.create_server(("localhost", config["port"]), reuse_port=True)
    print(f"Server started on port {config['port']}")  # Debugging log
    while True:
        connection: socket.socket
        address: tuple[str, int]
        connection, address = server_socket.accept()
        print(f"Accepted connection - {address[0]}:{str(address[1])}")
        thread: threading.Thread = threading.Thread(target=connect, args=[connection])
        thread.start()

if __name__ == "__main__":
    main()