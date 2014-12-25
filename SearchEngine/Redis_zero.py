import redis

#redis_187 = redis.Redis(host='219.239.89.187', port=7777)
#redis_zero = redis.Redis(host='219.239.89.187', port=7777)
redis_zero = redis.Redis(port=7777, unix_socket_path='/tmp/redis.sock')