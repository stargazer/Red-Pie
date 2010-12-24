import socket
from red.utils import comm

class Redis():
    def __init__(self, hostname = "localhost", port = 6379):
        self.sock = None
        self.connect(hostname, port)

    def connect(self, hostname = "localhost", port = 6379):
        """ Sets up a TCP connection to the redis server
        """
        if self.sock:
            return

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((hostname, port))
        self.sock = sock  

    def set(self,key,value):           
        """ Set the string value of a key. If key already holds its 
            value, its overwritten, regardless of its type
            
            @return: Status code reply. 
            Always OK
        """
        self.connect()
        self.sock.sendall(comm.constructMessage("SET", [key,value]))
        return self.handleResponse()

    def get(self, key):
        """ Get the value of a key. If the key does not exist, the special 
            value nil is returned. 

            @return: Bulk reply. 
            The value of key, or None when key does not exist.
        """ 
        self.connect()
        self.sock.sendall(comm.constructMessage("GET", [key]))
        return self.handleResponse()

    def hset(self, key, field, value):
        """  Sets the field in the hash stored at key, to value.

            @return: Integer reply. 
            1 if field is new and the value was set.
            0 if field already exists in the hash and the value was updated.
        """  
        self.connect()
        self.sock.sendall(comm.constructMessage("HSET", [key, field, value]))
        return self.handleResponse()
    
    def hget(self, key, field):
        """ Returns the value associated with field, in the hash at key.

            @return: Bulk reply.
            The value associated with field, or None if field is not present or
            key does not exist
        """ 
        self.connect()
        self.sock.sendall(comm.constructMessage("HGET", [key,field]))
        return self.handleResponse()

    def hkeys(self, key):
        """ Returns all field names in the hash stored at key.

            @return: Multi-bulk reply.
            List of fields in the hash, or an empty list when key does not
            exist.
        """
        self.connect()
        self.sock.sendall(comm.constructMessage("HKEYS", [key]))
        return self.handleResponse()

    def hgetall(self, key):
        """ Returns all fields and values of the hash stored at key
        
            @return: Multi-bulk reply.
            A dictionary of field:value pairs stored at key.
        """
        self.connect()
        self.sock.sendall(comm.constructMessage("HGETALL", [key]))
        # Returns a list
        response = self.handleResponse()
        # Which I transform to a dictionary
        dic = {} 
        if isinstance(response, list):
            for i in range(0, len(response), 2):
                dic[response[i]] = response[i+1]
            return dic
        return False

    def select(self, index):
        """ Selects the DB which has the specified zero-based
            numeric index.

            @return: Status code reply. OK if connection was successful
        """
        self.connect()
        self.sock.sendall(comm.constructMessage("SELECT",[index]))
        return self.handleResponse()

    def save(self):
        """ Synchronously save the dataset to disk.
        """
        self.connect()
        self.sock.sendall(comm.constructMessage("SAVE"))
        return self.handleResponse()

    def flushdb(self):
        """ Remove all keys from the current database.

            @return: Status code reply.
        """
        self.connect()
        self.sock.sendall(comm.constructMessage("FLUSHDB"))
        return self.handleResponse()

    def dbsize(self):
        """ Returns the number of keys in the currently selected database
        
            @return: Integer reply            
        """
        self.connect()
        self.sock.sendall(comm.constructMessage("DBSIZE"))
        return self.handleResponse()

    def quit(self):
        """ Asks the server to close the connection.

            @return: Status code reply. Always returns OK
        """               
        self.connect()
        self.sock.sendall(comm.constructMessage("QUIT"))
        self.sock = None    # The socket has closed the connection. None
                            # is used in connect() to symbolize this.
        
        #del self           # del, only removes the local reference to the
                            # object. So if I had placed "del object" here,
                            # the object would just unbind from the self var.
                            # It would still exist on the calling program's 
                            # namespace, since there still would be a reference
                            # there
                           
        return "OK"



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
                return None
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

