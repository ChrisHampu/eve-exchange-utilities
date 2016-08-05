import redis
import time

'''
t = time.time()

re = redis.StrictRedis(host='localhost', port=6379, db=0)

re.delete("order_cache")

re.hmset("order_cache", {1: t, 2: t})

re.hdel("order_cache", *[2])

print(re.hkeys("order_cache"))
'''
l =  [1, 2, 3]

s = set(l)

print(s)

#data = [int(k) for k in re.hgetall("order_cache")]

#print(data)
#for k in data:


  #print((int(k), float(data[k])))

#print(data)