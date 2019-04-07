



import socket
import sys

# Create a TCP/IP socket
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# Connect the socket to the port where the server is listening
port_to_connect = int(sys.argv[1])
key=sys.argv[2]
value=sys.argv[3]
server_address = ('localhost', port_to_connect)
print >>sys.stderr, 'connecting to %s port %s' % server_address
sock.connect(server_address)



try:
    
    # Write data

    message=key+'|'+value+"|user_write"
    print >>sys.stderr, 'sending "%s"' % message
    sock.sendall(message)



    # Read data

    # message = 'harshit|_|user_read'
    # print >>sys.stderr, 'sending "%s"' % message
    # sock.sendall(message)

    # msg = sock.recv(1000)
    # print("Client received value {} for key {}".format(msg,message))



finally:
    print >>sys.stderr, 'closing socket'
    sock.close()