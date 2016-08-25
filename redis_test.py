import redis
import time

re = redis.StrictRedis(host='localhost', port=6379, db=0)


allkeys = []

idx = 0
first = True

while idx != 0 or first == True:
  keys = re.scan(match='dly:*', cursor=idx)

  #print(keys)
  idx = keys[0]
  allkeys.extend(keys[1])

  first = False

print(len(allkeys))

t = time.perf_counter()

i = 0

for k in allkeys:
  doc = re.hmget(k, ['type', 'spread', 'tradeVolume', 'buyFifthPercentile'])
  i += 1

print(i)
print(time.perf_counter() - t)

t = time.perf_counter()

pip = re.pipeline()

i = 0

print(allkeys[0])

for k in allkeys:
  pip.hmget(k, ['type', 'spread', 'tradeVolume', 'buyFifthPercentile'])

docs = pip.execute()

print(docs[0])

print(i)
print(time.perf_counter() - t)