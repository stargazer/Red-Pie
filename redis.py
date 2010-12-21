import socket
from mini_redis.utils import comm

class Redis():
    def __init__(self, hostname = "localhost", port = 6379):
        self.sock = None
        self.connect(hostname, port)

    def connect(self, hostname = "localhost", port = 6379):
        """ Sets up a TCP connection to the redis server
            Socket sock can be used to send to the server (bytes)
            File Descriptor readFD can be used to read from the server (utf-8
            strings)
        """
        if self.sock:
            return

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((hostname, port))
        self.sock = sock                # The socket will be user to write (as bytes) 
        #self.readFD = sock.makefile("r", encoding="utf8")    # For reading we'll use a fake fd. Enables us to read directly utf8

    def set(self,key,value):           
        """ Set the string value as value for the key
            Gives status code.
            Returns True if succeeded, False otherwise
        """
        self.connect()
        self.sock.sendall(constructMessage("SET", [key,value]))
        return self.handleResponse()

    def get(self, key):
        """ Get the value of the specified key
            Gives bulk response.
            Returns the value of the key, or False if the key does not exist
        """ 
        self.connect()
        self.sock.sendall(constructMessage("GET", [key]))
        return self.handleResponse()

    def hset(self, key, field, value):
        """ Sets the specified field of the key, to the specified value.
            Gives Integer response (0 if overwritten, 1 if new field was
            added)
            Returns True if succeeded, False otherwise
        """  
        self.connect()
        self.sock.sendall(constructMessage("HSET", [key, field, value]))
        return self.handleResponse()
    
    def hget(self, key, field):
        """ Gest the specified field from the given key.
            Gives bulk reply.
            Returns True if succeeded, or False if the key or field do not
            exist
        """ 
        self.connect()
        self.sock.sendall(constructMessage("HGET", [key,field]))
        return self.handleResponse()

    def hkeys(self, key):
        """ Returns all the fields contained into a key
            Gives multi-bulk reply.
            Returns a list of all the fields into the key.
            If the key does not exist, returns an empty list.
            If the key exists, but is not a hashtable, returns False
        """
        self.connect()
        self.sock.sendall(constructMessage("HKEYS", [key]))
        return self.handleResponse()

    def hgetall(self, key):
        """ Returns both the fields and values contained in a hash.
            Gives multi-bulk reply.
            Returns a dictionary of field:value for all the fields in the key.
            If the key does not exist, returns an empty dic.
            If the key exists, but is not a hashtable, returns an empty dic.
        """
        self.connect()
        self.sock.sendall(constructMessage("HGETALL", [key]))
        # Returns a list
        response = self.handleResponse()
        # Which I transform to a dictionary
        dic = {} 
        if isinstance(response, list):
            for i in range(0, len(response), 2):
                dic[response[i]] = response[i+1]
            return dic
        return False

    def save(self):
        """ Saves the database on disk
        """
        self.connect()
        self.sock.sendall(constructMessage("SAVE"))
        return self.handleResponse()

    def flushdb(self):
        """ Remove all the keys from the currently selected database
        """
        self.connect()
        self.sock.sendall(constructMessage("FLUSHDB"))
        return self.handleResponse()

    
    def handleResponse(self):
        """ Handles the response returned from the redis server
        """
        line = b""
        byte = self.sock.recv(1)
        
        # Bulk reply
        if byte == b"$":
            # Construct the first line of reply.
            # It is either -1 or a number, indicating the length(in bytes)
            # of the actual response
            response = comm.getLine(self.sock)
            
            # line is still in bytes. I don't even need
            # to decode to ascii, since I will only interpret
            # what i read as an int. The conversion from bytes
            # to int is done automatically in int(line)
            length = int(response)
            if length == -1:
                return False
            else:
                # We need to read length bytes
                value = self.sock.recv(length)
                
                #consume the 2 remaning bytes(\r\n)
                self.sock.recv(2)
                return value.decode("utf8")

        # Single line reply
        # Status code
        elif byte == b"+":
            response = comm.getLine(self.sock)
            return response.decode("ascii")

        # Single line reply
        # Error Code
        elif byte == b"-":
            response = comm.getLine(self.sock)
            return response.decode("ascii")

        # Single line reply
        # Integer Reply
        elif byte == b":":
            response = comm.getLine(self.sock)
            return response.decode("ascii")

        # Multibulk reply
        elif byte == b"*":
            response = comm.getLine(self.sock)
            num_res = int(response)     # number of results
            if num_res != -1:
                results = []
                for i in range(num_res):
                    length = int(comm.getLine(self.sock)[1:])    # Length of result in bytes
                    results.append( self.sock.recv(length).decode("utf8") )
                    self.sock.recv(2)                            # Consume the \r\n
                return results
            else:
                return None

        return False

def constructMessage(command, args = []):
    """ command: Name of Redis command
        args: List of arguments
        Constructs the appropriate message, 
        that will be send to the redis server.
        The message represent a database query

        Messages are of the form:
        *<num arguments>\r\n
        $<length of command>\r\n
        command\r\n
        $<length of arg>\r\n
        argument\r\n
        $length of arg>\r\n
        argument\r\n
        ...
        ...\r\n
    """
    message1 = [ \
        "*" + str(len(args) + 1), \
        "$" + str(len(command.encode("utf8"))), \
        command \
    ]
    
    message2 = []
    for arg in args:
        arg = str(arg)
        message2.append("$" + str(len(arg.encode("utf8"))))
        message2.append(arg)

    message = "\r\n".join(message1 + message2) + "\r\n"
    return message.encode("utf8")
    #"\r\n".join(message) + "\r\n"
