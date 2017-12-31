import redis
r = redis.Redis(decode_responses=True, db=1)

addresses = r.hgetall('bitcoin')

with open('part.addresses', 'w') as ff:
    ff.write('\n'.join(list(addresses.keys())))
