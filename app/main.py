import socket
import threading
import time
import sys
import os
import struct

replication_offset = 0
replica_sockets = []
replica_ack_offsets = {}  # Tracks latest acknowledged replication offsets

store = {}  # Key-value storage with expiry support
replica_connection = None  # Replication connection for propagating write commands
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

def receive_commands_from_master(replica_socket):
    """Continuously listen for commands from the master and process them."""
    global replication_offset, store, replica_ack_offsets
    buffer = b""  
    rdb_received = False

    while True:
        try:
            data = replica_socket.recv(4096)
            if not data:
                break
            buffer += data

            if not rdb_received:
                # Handle FULLRESYNC and RDB file loading
                if buffer.startswith(b"+FULLRESYNC"):
                    newline_index = buffer.find(b"\r\n")
                    if newline_index == -1:
                        continue  

                    fullresync_response = buffer[:newline_index].decode().strip()
                    buffer = buffer[newline_index + 2:]  

                    parts = fullresync_response.split()
                    if len(parts) == 3 and parts[0] == "FULLRESYNC":
                        master_repl_id = parts[1]
                        master_repl_offset = int(parts[2])
                        print(f"Received FULLRESYNC: {master_repl_id}, offset {master_repl_offset}")
                    else:
                        print(f"Error parsing FULLRESYNC response: {fullresync_response}")
                        continue

                if buffer.startswith(b"$"):
                    rdb_length_end = buffer.find(b"\r\n")
                    if rdb_length_end != -1:
                        try:
                            rdb_length = int(buffer[1:rdb_length_end])
                            print(f"RDB file detected, length: {rdb_length} bytes. Reading...")

                            if len(buffer) < rdb_length_end + 2 + rdb_length:
                                continue  

                            buffer = buffer[rdb_length_end + 2 + rdb_length:]  
                            rdb_received = True
                            print("RDB file processing completed.")
                        except ValueError:
                            print("Error parsing RDB file length")
                            continue
                    else:
                        continue  

            if not rdb_received:
                continue  

            while buffer:
                try:
                    parts = buffer.split(b"\r\n")
                    if len(parts) < 3:
                        break  

                    num_args = int(parts[0][1:])
                    expected_lines = 1 + num_args * 2
                    if len(parts) < expected_lines + 1:
                        break  

                    command_lines = parts[:expected_lines]
                    command_str = "\r\n".join(line.decode("utf-8", errors="ignore") for line in command_lines) + "\r\n"
                    consumed = b"\r\n".join(parts[:expected_lines]) + b"\r\n"
                    buffer = buffer[len(consumed):]

                    pre_offset = replication_offset
                    processed_bytes = len(consumed)
                    replication_offset += processed_bytes

                    args = parse_resp(command_str)
                    if not args:
                        continue

                    cmd = args[0].upper()
                    print(f"Received command from master: {args}")
                    
                    if cmd == "REPLCONF" and len(args) == 3 and args[1].upper() == "GETACK":
                        ack_command = f"*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${len(str(replication_offset))}\r\n{replication_offset}\r\n"
                        replica_socket.sendall(ack_command.encode())
                        print(f"Replica sent ACK for offset {replication_offset}")
                        continue
                    
                    if cmd == "REPLCONF" and len(args) == 3 and args[1].upper() == "ACK":
                        try:
                            ack_offset = int(args[2])
                            replica_ack_offsets[replica_socket] = ack_offset
                            print(f"Replica acknowledged offset: {ack_offset}")
                        except ValueError:
                            print("Invalid ACK offset received")
                        continue

                    if cmd == "SET" and len(args) > 2:
                        key, value = args[1], args[2]
                        store[key] = (value, None)
                        print(f"Replicated SET command: {key} -> {value}")
                        ack_command = f"*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${len(str(replication_offset))}\r\n{replication_offset}\r\n"
                        replica_socket.sendall(ack_command.encode())
                        print(f"Sent ACK for offset {replication_offset} to master")

                    elif cmd == "DEL" and len(args) > 1:
                        key = args[1]
                        store.pop(key, None)
                        print(f"Replicated DEL command: {key}")
                        ack_command = f"*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${len(str(replication_offset))}\r\n{replication_offset}\r\n"
                        replica_socket.sendall(ack_command.encode())
                        print(f"Sent ACK for offset {replication_offset} to master")

                    # (ACK already sent for SET/DEL commands)

                except UnicodeDecodeError:
                    continue
        except Exception as e:
            print(f"Error receiving commands from master: {e}")
            break

def connect(connection: socket.socket) -> None:
    global store, config
    global replica_connection
    global replica_ack_offsets
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
                            f"master_repl_offset:{master_repl_offset}"
                        )  # No trailing \r\n to avoid length miscalculation
                        
                        response = f"${len(info_response.encode('utf-8'))}\r\n{info_response}\r\n"  # Append \r\n separately
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
                        # Propagate SET command to all replicas
                        command_to_replicate = f"*{len(args)}\r\n" + "".join(f"${len(arg)}\r\n{arg}\r\n" for arg in args)
                        for replica in replica_sockets:
                            try:
                                replica.sendall(command_to_replicate.encode())
                            except Exception as e:
                                print(f"Error propagating to replica: {e}")
                                replica_sockets.remove(replica)  # Remove dead connections

                        # Request acknowledgment from replicas after a write operation
                        getack_command = "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n"
                        for replica in replica_sockets:
                            try:
                                replica.sendall(getack_command.encode())
                            except Exception as e:
                                print(f"Error sending GETACK request to replica: {e}")
                                replica_sockets.remove(replica)  # Remove failed connections

                        global replication_offset
                        replication_offset += len(command_to_replicate)  # Increment replication offset
                    elif cmd == "DEL" and len(args) >= 2:
                        key = args[1]
                        if key in store:
                            del store[key]
                            response = ":1\r\n"  # Successful deletion
                        else:
                            response = ":0\r\n"

                        # Propagate DEL command to all replicas
                        command_to_replicate = f"*{len(args)}\r\n" + "".join(f"${len(arg)}\r\n{arg}\r\n" for arg in args)
                        for replica in replica_sockets:
                            try:
                                replica.sendall(command_to_replicate.encode())
                            except Exception as e:
                                print(f"Error propagating to replica: {e}")
                                replica_sockets.remove(replica)

                        # Request acknowledgment from replicas after a write operation
                        getack_command = "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n"
                        for replica in replica_sockets:
                            try:
                                replica.sendall(getack_command.encode())
                            except Exception as e:
                                print(f"Error sending GETACK request to replica: {e}")
                                replica_sockets.remove(replica)

                        replication_offset += len(command_to_replicate)
                    elif cmd == "GET" and len(args) > 1:
                        key = args[1]
                        print(f"Store contents before GET for key '{key}': {store}")
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
                    elif cmd == "REPLCONF" and len(args) >= 2:
                        if len(args) == 3 and args[1].upper() == "GETACK":
                            ack_command = f"*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${len(str(replication_offset))}\r\n{replication_offset}\r\n"
                            connection.sendall(ack_command.encode())
                            print(f"Sent ACK for offset {replication_offset} upon GETACK request")
                        else:
                            response = "+OK\r\n"
                    elif cmd == "PSYNC" and len(args) == 3 and args[1] == "?" and args[2] == "-1":
                        fullresync_response = f"+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb {replication_offset}\r\n"
                        connection.sendall(fullresync_response.encode())
                        
                        empty_rdb_hex = bytes.fromhex(
                            "524544495330303131"  # "REDIS0011" - version 11
                            "fa0972656469732d76657205372e322e30"
                            "fa0a72656469732d62697473c040"
                            "fa056374696d65c26d08bc65"
                            "fa08757365642d6d656dc2b0c41000"
                            "fa08616f662d62617365c000ff"
                            "f06e3bfec0ff5aa2"
                        )
                        empty_rdb_response = f"${len(empty_rdb_hex)}\r\n".encode() + empty_rdb_hex
                        connection.sendall(empty_rdb_response)
                        print("Sent corrected empty RDB file to replica")
                    
                        # Store replica connection for later use
                        replica_sockets.append(connection)
                    elif cmd == "WAIT" and len(args) == 3:
                        try:
                            expected_replicas = int(args[1])
                            timeout = int(args[2]) / 1000  # Convert milliseconds to seconds
                            start_time = time.time()

                            while time.time() - start_time < timeout:
                                acknowledged_replicas = sum(1 for ack in replica_ack_offsets.values() if ack >= replication_offset)
                                if acknowledged_replicas >= expected_replicas:
                                    break
                                time.sleep(0.01)  # Sleep briefly to avoid busy-waiting

                            print(f"WAIT completed: expected={expected_replicas}, acknowledged={acknowledged_replicas}, latest_offset={replication_offset}")
                            response = f":{acknowledged_replicas}\r\n"
                        except ValueError:
                            response = "-ERR Invalid WAIT arguments\r\n"
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
    if "replicaof" in config:
        master_host, master_port = config["replicaof"]
        master_port = int(master_port)
        try:
            replica_socket = socket.create_connection((master_host, master_port))
            print(f"Connected to master at {master_host}:{master_port}")
            ping_command = b"*1\r\n$4\r\nPING\r\n"
            replica_socket.sendall(ping_command)
            print("Sent PING to master")
            pong_response = replica_socket.recv(1024)
            if not pong_response.startswith(b"+PONG"):
                print("Did not receive PONG from master")
                return
            print("Received PONG from master")
            
            # Send REPLCONF listening-port <PORT>
            listening_port_command = f"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n${len(str(config['port']))}\r\n{config['port']}\r\n".encode()
            replica_socket.sendall(listening_port_command)
            print(f"Sent REPLCONF listening-port {config['port']} to master")
            ok_response = replica_socket.recv(1024)
            if not ok_response.startswith(b"+OK"):
                print("Did not receive OK for REPLCONF listening-port")
                return
            print("Received OK for REPLCONF listening-port")
            
            # Send REPLCONF capa psync2
            capa_command = b"*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"
            replica_socket.sendall(capa_command)
            print("Sent REPLCONF capa psync2 to master")
            ok_response = replica_socket.recv(1024)
            if not ok_response.startswith(b"+OK"):
                print("Did not receive OK for REPLCONF capa psync2")
                return
            print("Received OK for REPLCONF capa psync2")
            # Send PSYNC command as RESP array
            psync_command = b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"
            replica_socket.sendall(psync_command)
            print("Sent PSYNC ? -1 to master")
            replica_connection = replica_socket  # Store the replication connection
            threading.Thread(target=receive_commands_from_master, args=(replica_socket,)).start()
        except Exception as e:
            print(f"Failed to connect to master: {e}")
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