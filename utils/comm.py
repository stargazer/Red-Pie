

def getLine(sock):
    """ Gets a sock communicating with the redis server
        Reads the response of the server, byte per byte,
        and returns a line of the server response
        
        The line returned is in byte format, not in 
        any encoded.

        In the end, the socket points to the start of the next line
    """
    line = b""
    while True:
        next_byte = sock.recv(1)  # read a byte
        if next_byte == b"\r":    # if it's end of line, break
            break                  
        line += next_byte         # otherwise, stick it with the rest
    sock.recv(1)                  # Consume the remaining \n character
    return line
    

def constructMessage(command, args = []):
    """ Constructs the appropriate message, 
        that will be send to the redis server.
        The message represents a database query
        
        @param command: Database command to execute
        @param: List of arguments for command

        Messages are in the form:
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
    part_1 = [ \
        "*" + str(len(args) + 1), \
        "$" + str(len(command.encode("utf8"))), \
        command \
    ]
    
    part_2 = []
    for arg in args:
        arg = str(arg)
        part_2.append("$" + str(len(arg.encode("utf8"))))
        part_2.append(arg)

    message = "\r\n".join(part_1 + part_2) + "\r\n"
    return message.encode("utf8")
    #"\r\n".join(message) + "\r\n"
