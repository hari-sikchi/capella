import socket
import sys
server_addresses = [
    ('10.146.137.215', 12050),
    ('10.146.137.215', 1201),
    ('10.5.16.220', 1202),
    ('10.5.16.220', 1203),
    ('10.145.195.203', 1204),
    ('10.145.195.203', 1205)
]

N = 6

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

        elif command_type == 's':
            _, server_idx, key = command.split(' ')
            server_idx = int(server_idx)
            sock.connect(server_addresses[server_idx])
            sock.sendall(key + '|start_recovery')
            data = sock.recv(1000)
            _ = data.split('|')
            sock.close()

        else:
            _, server_idx, key, value = command.split(' ')
            server_idx = int(server_idx)
            sock.connect(server_addresses[server_idx])
            sock.sendall(key+'|'+value+'|user_write')
            data = sock.recv(1000)
            _ = data.split('|')
            sock.close()
