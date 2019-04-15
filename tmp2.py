import socket
import sys
import random
import time
# from _thread import *
import threading

server_addresses = [
    ('127.0.0.1', 1200),
    ('127.0.0.1', 1201),
    ('127.0.0.1', 1202),
    ('127.0.0.1', 1203),
    ('127.0.0.1', 1204),
]

N = 5
R = 3
Q_r = 2
Q_w = 2

database = {}
database_version = {}
locks = {}

server_idx = int(sys.argv[1])

# Message types -
#
#   user_read = 'key|user_read'
#   user_read_reply = 'value|user_read_reply'
#   user_write = 'key|value|user_write'
#   user_write_reply = 'user_write_reply'
#   coordinator_read = 'key|coordinator_read'
#   coordinator_read_reply = 'value|version|coordinator_read_reply'
#   coordinator_write = 'key|value|version|coordinator_write'
#   coordinator_write_reply = 'coordinator_write_reply'
#   lock = 'key|lock'
#   lock_reply = '0/1|version|lock_reply'
#   lock_release = 'key|lock_release'

def process(data, connection):
    message_type = data.split('|')[-1]

    if message_type == 'user_read':
        key,_ = data.split('|')

        highest_version = -1
        highest_version_value = ''

        nodes = random.sample([(hash(key)+i)%N for i in range(R)], Q_r)
        for node in nodes:
            if node == server_idx:
                value, version = database[key], database_version[key]
            else:
                tmp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                tmp_sock.connect(server_addresses[node])
                tmp_sock.sendall(key+'|coordinator_read')
                data = tmp_sock.recv(1000)
                value, version, _ = data.split('|')
                tmp_sock.close()

            version = int(version)
            if version > highest_version:
                highest_version = version
                highest_version_value = value

        connection.sendall(highest_version_value+'|user_read_reply')

    elif message_type == 'coordinator_read':
        key, _ = data.split('|')
        connection.sendall(database[key]+'|'+database_version[key]+'|'+'coordinator_read_reply')

    elif message_type == 'user_write':
        key, value,_ = data.split('|')
        sleep_time = 0
        write_successfull = False

        while True:
            time.sleep(sleep_time)
            if sleep_time == 0:
                sleep_time += 0.1
            else:
                sleep_time *= 2

            locked_nodes = []
            highest_version = -1
            nodes = [(hash(key)+i)%N for i in range(R)]

            print(nodes)

            # try locking
            for node in nodes:
                if node == server_idx:
                    if key not in locks:
                        locks[key] = False
                        database_version[key] = str(0)

                    if locks[key] == False:
                        locks[key] = True
                        locked_nodes.append(node)
                        highest_version = max(int(database_version[key]), highest_version)
                else:
                    tmp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    tmp_sock.connect(server_addresses[node])
                    tmp_sock.sendall(key+'|lock')
                    data = tmp_sock.recv(1000)
                    success,version,_ = data.split('|')
                    tmp_sock.close()

                    if int(success):
                        locked_nodes.append(node)
                        highest_version = max(int(version), highest_version)

            # if successfully locked write quorum, write
            if(len(locked_nodes)>=Q_w):
                write_successfull = True
                for node in locked_nodes:
                    if node == server_idx:
                        database[key] = value
                        database_version[key] = str(highest_version+1)
                    else:
                        tmp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        tmp_sock.connect(server_addresses[node])
                        tmp_sock.sendall(key+'|'+value+'|'+str(highest_version+1)+'|coordinator_write')
                        data = tmp_sock.recv(1000)
                        _ = data.split('|')
                        tmp_sock.close()

            # release locks
            for node in locked_nodes:
                if node == server_idx:
                    locks[key] = False
                else:
                    tmp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    tmp_sock.connect(server_addresses[node])
                    tmp_sock.sendall(key+'|lock_release')
                    tmp_sock.close()

            if write_successfull:
                break

        connection.sendall('coordinator_write_reply')

    elif message_type == 'lock':
        key,_ = data.split('|')
        if key not in locks:
            locks[key] = False
            database_version[key] = str(0)

        if locks[key] == True:
            connection.sendall('0|_|lock_reply')
        else:
            locks[key] = True
            if key in database_version:
                connection.sendall('1|'+database_version[key]+'|lock_reply')

    elif message_type == 'coordinator_write':
        key,value,version,_ = data.split('|')
        database[key] = value
        database_version[key] = version
        connection.sendall('coordinator_write_reply')

    elif message_type == 'lock_release':
        key,_ = data.split('|')
        locks[key] = False

    print("database: %s\nversion:%s\nlocks:%s\n"%(database, database_version, locks))
    print("closing this socket")
    connection.close()
    return


if __name__ == "__main__":

    print "Starting server at %s" % (server_addresses[server_idx],)

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
    sock.bind(server_addresses[server_idx])
    sock.listen(10)

    while True:
        connection, client_address = sock.accept()
        data = connection.recv(1000)
        if data:
            # message_type = data.split('|')[-1]
            t1 = threading.Thread(target=process, args=(data, connection, ))
            t1.start()

            # process(data, connection)
        # sock.close()
