import socket  # noqa: F401


def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    conn, addr = server_socket.accept()  # wait for client
    with conn:
        print("Connected by", addr)
        while True:
            data = conn.recv(1024)
            print("data: %s" % data)
            if data == b"*1\r\n$4\r\nping\r\n":
                conn.sendall(b"+PONG\r\n")
            elif b"ping" in data:
                conn.sendall(b"+PONG\r\n")
            else:
                conn.sendall(b"error\r\n")

if __name__ == "__main__":
    main()
