import time
from red import pie                  
#import redis

server = pie.Redis()
server.select(5)

times = []
for j in range(5):
    start = time.clock()
    for i in range(100000):
        server.hset("key","field","value")
        #server.hset("word","field","value")
        #server.hset("word","fielr","value")
    elapsed = time.clock() - start
    times.append(elapsed)
    print(elapsed)
