import socket
import sys
import numpy as np 
# Create a TCP/IP socket
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)

my_port = int(sys.argv[1])
server_address = ('localhost', my_port)
print >>sys.stderr, 'starting up on %s port %s' % server_address
sock.bind(server_address)
replication_factor = 3
qr= 2
sock.listen(1)




network = [10001,10002,10003,10004,10005]

n_nodes = len(network)
Qr = n_nodes/2+1
Qw =n_nodes/2+1
busy = False

my_database = {}


def read_data(key,mode = 'read'):
    global qr,sock1,busy
    node_number = hash(key)%5
    nodes_to_write = [network[node_number]]
    for i in range(replication_factor-1):
        nodes_to_write.append(network[(node_number+1+i)%5])



    nodes_to_read = nodes_to_write
    #nodes_to_read = np.random.choice(np.asarray(nodes_to_write),qr,replace=False)
    print("Nodes to read: {}".format(nodes_to_read))
    replies = []
    latest_version = -1
    quorum_received = 0
    servers_locked =[]
    for node in nodes_to_read:

        if(mode=='write'):
            sock1.close()
            sock1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

            if(node==my_port):
                if busy==True:
                    return 
                else:
                    quorum_received+=1
                    replies.append(my_database.get(key))
                    servers_locked.append(my_port)
                    busy = True
            else:
                server_address = ('localhost', node)
                print >>sys.stderr, 'connecting to %s port %s' % server_address
                
                sock1.connect(server_address)
                message = key+'|'+'_'+'|coordinator_read'
                print >>sys.stderr, 'sending "%s"' % message
                sock1.sendall(message)

                data = sock1.recv(1000)
                print(data)
                key_,value,_option = data.split('|')
                
                if(_option!="rejected"):
                    quorum_received+=1
                    replies.append(value)
                    servers_locked.append(int(key_))
                sock1.close()


        else:


            sock1.close()
            sock1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)


            if(node==my_port):
                replies.append(my_database[key])
            else:
                server_address = ('localhost', node)
                print >>sys.stderr, 'connecting to %s port %s' % server_address
                
                sock1.connect(server_address)
                message = key+'|'+'_'+'|coordinator_read'
                print >>sys.stderr, 'sending "%s"' % message
                sock1.sendall(message)

                data = sock1.recv(1000)
                print(data)
                
                key_,value,_ = data.split('|')
                replies.append(value)
                sock1.close()


    return replies[np.random.randint(0,len(replies))],quorum_received, servers_locked



def write_data(key,value):
    global replication_factor,sock1, busy

    node_number = hash(key)%5
    
    nodes_to_write = [network[node_number]]

    # First obtain the permission from read quorum
    quorum_received=0

    while(quorum_received<Qw):
        replies, quorum_received,servers_locked = read_data(key,mode="write")

        print("Quorun received is: {}".format(quorum_received))
        # Release quorum if not required
        if(quorum_received<Qw):
            for node in servers_locked:
                sock1.close()
                sock1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)


                if(node==my_port):
                    busy = False
                else:
                    server_address = ('localhost', node)
                    print >>sys.stderr, 'connecting to %s port %s' % server_address
                    
                    sock1.connect(server_address)
                    message = key+'|'+value+'|coordinator_write_release'
                    print >>sys.stderr, 'sending "%s"' % message
                    sock1.sendall(message)
                    sock1.close()





    for i in range(replication_factor-1):
        nodes_to_write.append(network[(node_number+1+i)%5])

    #print("Nodes to write: {}".format(nodes_to_write))

    print("Obtained Quorum for write")
    print("Servers locked are: {}".format(servers_locked))

    for node in servers_locked:
        sock1.close()
        sock1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)


        if(node==my_port):
            busy = False
            my_database[key]=value
        else:
            server_address = ('localhost', node)
            print >>sys.stderr, 'connecting to %s port %s' % server_address
            
            sock1.connect(server_address)
            message = key+'|'+value+'|coordinator_write'
            print >>sys.stderr, 'sending "%s"' % message
            sock1.sendall(message)
            sock1.close()



def process_data(data,connection):
    global sock, busy
    print(str(data).split('|'))
    key,value,_type= data.split('|')
    if _type=="coordinator_write":
        if(busy==True):
            my_database[key]=value
            busy = False
        else:
            pass
    elif _type=="coordinator_write_release":
        busy = False        
        
    elif _type=="coordinator_read":
        if(busy==False):
            print("My database has: {}".format(my_database.get(key)))
            print(str(my_port)+"|"+str(my_database.get(key))+"|"+"read_reply")
            connection.sendall(str(my_port)+"|"+str(my_database.get(key))+"|"+"read_reply")
            busy = True    
        else:
            print("I am busy, rejecting an incoming connection")
            connection.sendall(key+"|"+my_database[key]+"|"+"rejected")
    elif _type == "user_read":
        reply,_,_ = read_data(key)
        connection.sendall(reply)
    else:  
        print(key,value)
        write_data(key,value)

    
    print(my_database)




while True:
    # Wait for a connection
    print >>sys.stderr, 'waiting for a connection'
    

    while True:
        connection, client_address = sock.accept()
        data = connection.recv(1000)
        if data:
            process_data(data,connection)
        # connection.close()

    print >>sys.stderr, 'connection from', client_address

            
    # finally:
    #     # Clean up the connection
    #     connection.close()