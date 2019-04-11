import socket
import sys

server_addresses = [
    ('127.0.0.1', 1200),
    ('127.0.0.1', 1201),
    ('127.0.0.1', 1202),
    ('127.0.0.1', 1203),
    ('127.0.0.1', 1204),
]

N = 5

def process(data, connection):
    message_type = data.split('|')[-1]
    print(message_type)


if __name__ == "__main__":
    while True:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        command = raw_input('> ')
        command_type = command.split(' ')[0]
        
        if command_type == 'read':
            _, server_idx, key = command.split(' ')
            server_idx = int(server_idx)
            sock.connect(server_addresses[server_idx])
            sock.sendall(key+'|user_read')
            data = sock.recv(1000)
            value, _ = data.split('|')
            print(value)
            sock.close()
        
        else:
            _, server_idx, key, value = command.split(' ')
            server_idx = int(server_idx)
            sock.connect(server_addresses[server_idx])
            sock.sendall(key+'|'+value+'|user_write')
            data = sock.recv(1000)
            _ = data.split('|')
            sock.close()
    



