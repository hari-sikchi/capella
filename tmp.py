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

def func(data, connection):
    connection.send("Hello")
    print connection
    connection.close()

if __name__ == "__main__":

    print "Starting server at %s" % (server_addresses[server_idx],)

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
    sock.bind(server_addresses[server_idx])
    sock.listen(10)

    for i in range(2):
        connection, client_address = sock.accept()
        data = ("hello")
        if data:
            # message_type = data.split('|')[-1]
            t1 = threading.Thread(target=func, args=(data, connection, ))
            t1.start()

            # process(data, connection)
        # sock.close()
