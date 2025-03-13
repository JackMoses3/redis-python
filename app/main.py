import socket
import threading

def main() -> None:
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    while True:
        connection: socket.socket
        address: tuple[str, int]
        connection, address = server_socket.accept()
        print(f"accepted connection - {address[0]}:{str(address[1])}")
        thread: threading.Thread = threading.Thread(target=connect, args=[connection])
        thread.start()

        
def connect(connection: socket.socket) -> None:
    with connection:
        connected: bool = True
        while connected:
            command: str = connection.recv(1024).decode()
            print(f"recieved - {command}")
            connected = bool(command)
            response: str
            match command:
                case "*1\r\n$4\r\nPING\r\n":
                    response = "+PONG\r\n"
            print(f"responding with - {response}")
            connection.sendall(response.encode())
if __name__ == "__main__":
    main()