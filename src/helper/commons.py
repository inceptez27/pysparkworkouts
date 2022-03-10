import socket

def getdiscount(age):
    if age < 10:
        return 5
    elif age < 20:
        return 10
    elif age  < 40:
        return 15
    elif age < 60:
        return 20
    else:
        return 25


def getfullname(firstname,lastname):
    return firstname + " -" + lastname

def writetosocket(msg):
    clientsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    clientsocket.connect(('localhost', 9999))
    
    #Send our result to the server.
    clientsocket.send(bytes(str(msg),'utf-8'))
    
    
writetosocket("This is a test message\n")    
    
    
