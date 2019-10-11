import redis
from proxypool.error import PoolEmptyError
from proxypool.setting import HOST, PORT, PASSWORD


class RedisClient(object):
    def __init__(self, host=HOST, port=PORT):
        if PASSWORD:
            # 创建一个redis的操作对象
            self._db = redis.Redis(host=host, port=port, password=PASSWORD)
        else:
            self._db = redis.Redis(host=host, port=port)

    def get(self, count=1):
        """
        get proxies from redis
        """
        # 获取代理数据,从左边第一个取
        proxies = self._db.lrange("proxies", 0, count - 1)
        # 去过之后,把这个代理从代理队列中剔除
        self._db.ltrim("proxies", count, -1)
        # 返回取出的代理
        return proxies

    def lput(self,proxy):
        self._db.lpush("proxies",proxy)


    def put(self, proxy):
        """
        add proxy to right top
        """
        # 从右侧添加代理到代理队列
        self._db.rpush("proxies", proxy)

    def pop(self):
        """
        get proxy from right.
        """
        try:
            # 从右侧最后一个取出一个代理,并把它从代理队列中删除
            return self._db.rpop("proxies").decode('utf-8')
        except:
            # 如果没有的,捕获代理池为空的异常
            raise PoolEmptyError

    @property
    def queue_len(self):
        """
        get length from queue.
        """
        # 获取列表长度
        return self._db.llen("proxies")

    def flush(self):
        """
        flush db
        """
        # 刷新(删除)代理池
        self._db.flushall()


if __name__ == '__main__':
    conn = RedisClient()
    print(conn.pop())
