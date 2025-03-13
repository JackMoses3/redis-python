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
        while True:
            command: str = connection.recv(1024).decode()
            if not command:
                break
            
            print(f"received - {command}")
            
            args = parse_resp(command)
            if not args:
                continue  # Ignore invalid commands

            cmd = args[0].upper()  # Case-insensitive command handling
            response: str = ""

            if cmd == "PING":
                response = "+PONG\r\n"
            elif cmd == "ECHO" and len(args) > 1: # if it is an echo you are going to return the length of the string and the string itself
                msg = args[1]
                response = f"${len(msg)}\r\n{msg}\r\n"
            
            print(f"responding with - {response}")
            connection.sendall(response.encode())

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