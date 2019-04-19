import socket
import sys
import random
import time
# from _thread import *
import threading
import json
import cPickle as pickle

server_addresses = [
    ('10.146.137.215', 12050),
    ('10.146.137.215', 1201),
    ('10.145.195.203', 1202),
    ('10.145.195.203', 1203),
    ('10.145.195.203', 1204),
]

server_hb_addresses = [
    ('10.146.137.215', 12100),
    ('10.146.137.215', 1211),
    ('10.145.195.203', 1212),
    ('10.145.195.203', 1213),
    ('10.145.195.203', 1214),
]

N = 5
R = 3
Q_r = 2
Q_w = 2


database = {}
database_version = {}
locks = {}
failed_nodes = []

server_idx = int(sys.argv[1])

# Required for system recovery_done

ownership = [server_idx]
nxt = (server_idx+1)%N
prv = (server_idx + N-1)%N

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
#   start_recovery = 'node|start_recovery' - node detects crash and becomes coordinator for that recovery
#   recover_data = 'add_to|recover_data'
#   send_ownership
#   send_database
#   send_database_version

def rcv_heartbeats():
    time.sleep(10)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(10)
    sock.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
    sock.bind(server_hb_addresses[server_idx])
    print('Listening to heartbeart at', server_hb_addresses[server_idx])
    sock.listen(10)
    own = ""

    while True:
        # sock.settimeout(1)
        # connection, client_address = sock.accept()
        try:
            connection, client_address = sock.accept()
            # print(client_address)
            data = connection.recv(1000)
            # print('data', data)
            # print ("Received hearbeat")
            if data:
                # print (own)
                own = data.split('|')[1]
                # print ("Data", data)
        except socket.timeout: # fail after 1 second of no activity
            print("Didn't receive data! [Timeout]: start recovery")
            tmp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            tmp_sock.connect(server_addresses[server_idx])
            # print ("recovery messsage sent")
            tmp_sock.sendall(str(prv)+'|'+own+'|start_recovery')
            # data = tmp_sock.recv(1000)
            # print("received ack")
            tmp_sock.close()
            break


def send_hb():
    sleep_time=1
    while True:
        tmp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            # print("try heartbeat to", server_hb_addresses[nxt])
            tmp_sock.connect(server_hb_addresses[nxt])
            # print("Sending heartbeat to", server_hb_addresses[nxt])
            # print ("hearbeat sent")
            # print("sending", pickle.loads(pickle.dumps(ownership)))
            tmp_sock.sendall(str(server_idx)+'|'+str(pickle.dumps(ownership))+'|heartbeat')
            # print ("hearbeat sent")
            tmp_sock.close()
            time.sleep(sleep_time)
        except:
            continue


def get_add_to_nodes(crash):

    lst = []
    ctr = 1
    i = 0
    while len(lst) != 3:

        n = (crash + ctr) % N;

        if n not in failed_nodes:
            lst.append(n)
            i = i+1

        ctr = ctr + 1

    return lst
def get_next_live_inc(node, sz):
    lst = []
    ctr = 0
    i = 0
    while len(lst) != sz:

        n = (node + ctr) % N;

        if n not in failed_nodes:
            lst.append(n)
            i = i+1

        ctr = ctr + 1

    return lst

def get_prev(node, sz):

    lst = []
    ctr = 1
    i = 0
    while len(lst) != sz:

        n = (node + N - ctr) % N;

        if n not in failed_nodes:
            lst.append(n)
            i = i+1

        ctr = ctr + 1
    lst.reverse()
    return lst

def get_copy_key_nodes(crash):

    lst = []
    ctr = 2
    i = 0
    while len(lst) != 2:

        n = (crash + N - ctr) % N;

        if n not in failed_nodes:
            lst.append(n)

        ctr = ctr - 1

    lst.append(crash)
    return lst

def get_copy_from_list(copy_key):

    lst = []
    i = copy_key

    print(failed_nodes)

    while len(lst) !=2 :

        if i not in failed_nodes:
            lst.append(i)

        i = (i + 1)%N

    # return lst
    return lst

def recover(add_to, copy_from):
    global database ,database_version, locks, failed_nodes, ownership, nxt, prv
    # copy_from = get_copy_from_list(copy_key)

    print("copy_from_list" , copy_from)

    for i in range(len(copy_from)):
        node = copy_from[i]
        if i == 0 :
            tmp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            tmp_sock.connect(server_addresses[node])
            tmp_sock.sendall('send_ownership')
            own = tmp_sock.recv(10000)
            tmp_sock.close()
            own = pickle.loads(own)
            print("received ownership: %s"%(own))

        tmp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tmp_sock.connect(server_addresses[node])
        tmp_sock.sendall('send_database')
        data = tmp_sock.recv(10000)
        tmp_sock.close()

        tmp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tmp_sock.connect(server_addresses[node])
        tmp_sock.sendall('send_database_version')
        data_version = tmp_sock.recv(10000)
        tmp_sock.close()



        data = pickle.loads(data)
        data_version = pickle.loads(data_version)

        print("received database: %s"%(data))
        print("received database version: %s"%(data_version))
        print("received own: %s"%(own))

        for k in data:
            print("dd", hash(k)%N, own)
            if hash(k)%N in own:
                print("Adding database of ", k)
                if k not in database:
                    database[k] = data[k]
                    database_version[k] = data_version[k]
                elif database_version[k] < data_version[k]:
                    database[k] = data[k]
                    database_version[k] = data_version[k]


def process(data, connection):

    global database ,database_version, locks, failed_nodes, ownership, nxt, prv

    message_type = data.split('|')[-1]

    if message_type == 'user_read':
        key,_ = data.split('|')

        highest_version = -1
        highest_version_value = ''

        # nodes = random.sample([(hash(key)+i)%N for i in range(R)], Q_r)
        nodes = random.sample(get_next_live_inc(hash(key), R), Q_r)

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
            nodes = get_next_live_inc(hash(key), R)

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

    elif message_type == 'start_recovery':
        #             tmp_sock.sendall(str(prv)+'|'+own+'|start_recovery')
        print(data)
        crash,own, _ = data.split('|')
        own = pickle.loads(own)
        crash = int(crash)
        failed_nodes.append(crash)

        ownership.extend(own)
        print(' changing prv from ', prv, "to ")
        prv = get_prev(server_idx, 1) [0]
        print(prv)

        tmp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tmp_sock.connect(server_addresses[prv])
        tmp_sock.sendall(str(server_idx) + '|' + 'update_nxt')
        data = tmp_sock.recv(1000)
        print(data)
        tmp_sock.close()

        print("coord - recovery started")

        for node in server_addresses:
            if (node in failed_nodes):
                continue

            if node == server_addresses[server_idx]:
                continue

            try:
                tmp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                tmp_sock.connect(node)
                tmp_sock.sendall(str(crash) + '|update_failed_nodes')
                print("crash update sent to " + str(node))
                data = tmp_sock.recv(1000)
                print(data)
                tmp_sock.close()

            except:
	        	continue


        add_to_list = get_add_to_nodes(crash) #returns 3 nodes where data will be added

        print("add_to_list" , add_to_list)

        for i in range(3):

            add_to = add_to_list[i]

            if add_to == server_idx:
                recover(add_to,get_prev(server_idx, 2))
                print(str(add_to) + ' recovery_done')

            else:
                tmp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                tmp_sock.connect(server_addresses[add_to])
                tmp_sock.sendall(str(add_to) + '|' + 'recover_data')
                data = tmp_sock.recv(1000)
                print(data)
                tmp_sock.close()

        print('recovery_completed')
        print('Listening to hearbeats now')
        t1 = threading.Thread(target=rcv_heartbeats)
        t1.start()

    elif message_type == 'recover_data':
        add_to , _ = data.split('|')
        add_to = int(add_to)
        copy_from = get_prev(server_idx, 2)
        print("recovering data into  " + str(add_to) +" "+ str(copy_from[0]) + " and "+ str(copy_from[1]))
        recover(add_to,copy_from)
        print(str(add_to) +" from prev 2 live nodes" + ' recovery_done')
        connection.sendall(str(add_to) + ' recovery_done')

    elif message_type == 'send_ownership':
        connection.sendall(pickle.dumps(ownership))

    elif message_type == 'send_database':
        connection.sendall(pickle.dumps(database))

    elif message_type == 'send_database_version':
        connection.sendall(pickle.dumps(database_version))

    elif message_type == 'update_failed_nodes':
        crash , _ = data.split('|')
        failed_nodes.append(int(crash))
        connection.sendall("Updated" + str(server_idx))

    elif message_type == 'update_nxt':
    	temp,_ = data.split('|')
    	nxt = int(temp)
    	connection.sendall("Updated nxt")

    print("database: %s\nversion:%s\nlocks:%s\nfailed_nodes%s\nownership%s\n"%(database, database_version, locks, failed_nodes, ownership))
    # print("closing this socket")
    connection.close()
    return


if __name__ == "__main__":

    print "Starting server at %s" % (server_addresses[server_idx],)

    t1 = threading.Thread(target=send_hb)
    t1.start()
    t1 = threading.Thread(target=rcv_heartbeats)
    t1.start()

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
