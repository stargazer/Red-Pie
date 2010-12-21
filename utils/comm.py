

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
    
