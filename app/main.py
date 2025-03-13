import socket
import threading

def parse_resp(command: str) -> list[str]:
    """Parses a RESP-encoded command string into a list of arguments."""
    parts = command.strip().split("\r\n")
    if len(parts) < 4 or not parts[0].startswith("*"):
        return []
    
    num_args = int(parts[0][1:])
    args = []
    i = 1
    while i < len(parts) and len(args) < num_args:
        if parts[i].startswith("$"):
            arg_len = int(parts[i][1:]) # lrngth of the string, this is used to tell how long the response is
            args.append(parts[i + 1])  # The actual argument
            i += 2
        else:
            i += 1  # Skip malformed input

    return args

def connect(connection: socket.socket) -> None:
    with connection:
        buffer = ""
        while True:
            try:
                data = connection.recv(1024).decode()
                if not data:
                    break

                buffer += data
                if "\r\n" not in buffer:
                    continue  # Wait for the full RESP command

                print(f"received - {buffer.strip()}")

                args = parse_resp(buffer)
                buffer = ""  # Reset buffer after processing

                if not args:
                    continue  # Ignore invalid commands

                cmd = args[0].upper()  # Case-insensitive command handling
                response: str = ""

                if cmd == "PING":
                    response = "+PONG\r\n"
                elif cmd == "ECHO" and len(args) > 1:
                    msg = args[1]
                    response = f"${len(msg)}\r\n{msg}\r\n"

                if response:
                    print(f"responding with - {response.strip()}")
                    connection.sendall(response.encode())

            except Exception as e:
                print(f"Error handling request: {e}")
                break

def main() -> None:
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