import time
from multiprocessing import Process
import asyncio
import aiohttp
try:
    from aiohttp.errors import ProxyConnectionError,ServerDisconnectedError,ClientResponseError,ClientConnectorError
except:
    from aiohttp import ClientProxyConnectionError as ProxyConnectionError,ServerDisconnectedError,ClientResponseError,ClientConnectorError
from proxypool.db import RedisClient
from proxypool.error import ResourceDepletionError
from proxypool.getter import FreeProxyGetter
from proxypool.setting import *
from asyncio import TimeoutError


class ValidityTester(object):
    test_api = TEST_API

    def __init__(self):
        self._raw_proxies = None  # 原始代理
        self._usable_proxies = []  # 可用的代理

    def set_raw_proxies(self, proxies):
        self._raw_proxies = proxies  # 把接收到的proxies赋值给实例属性让内部使用
        self._conn = RedisClient()  # 创建redis连接

    # 测试单个代理,async设置为异步函数(协程)
    async def test_single_proxy(self, proxy):
        """
        text one proxy, if valid, put them to usable_proxies.
        """
        try:
            # 创建一个会话对象session发请求
            async with aiohttp.ClientSession() as session:
                try:
                    # 设置代理为utf-8
                    if isinstance(proxy, bytes):
                        proxy = proxy.decode('utf-8')
                    # 拼接代理地址
                    real_proxy = 'http://' + proxy
                    print('Testing', proxy)
                    # 用session发送get请求,用百度测试,设置代理检测
                    async with session.get(self.test_api, proxy=real_proxy, timeout=get_proxy_timeout) as response:
                        # 如果响应状态码是200,说明代理可用,加入到代理队列右边
                        if response.status == 200:
                            self._conn.put(proxy)
                            print('Valid proxy', proxy)
                # 如果出现未连接成功,超时,值错误,那么就打印提示信息
                except (ProxyConnectionError, TimeoutError, ValueError):
                    print('Invalid proxy', proxy)
        # 捕获里面异常
        except (ServerDisconnectedError, ClientResponseError,ClientConnectorError) as s:
            print(s)
            pass
    # 大量检测proxy
    def test(self):
        """
        aio test all proxies.
        """
        print('ValidityTester is working')
        try:
            # 协程不能直接运行,需要创建一个事件循环
            loop = asyncio.get_event_loop()
            # 任务对象列表,把每个代理交给检测方法,生成一个任务对象,把这些任务对象放到一个任务对象列表中
            tasks = [self.test_single_proxy(proxy) for proxy in self._raw_proxies]
            # 调用run_until_complete方法,将协程注册到事件循环中,并启动事件循环
            loop.run_until_complete(asyncio.wait(tasks))
        except ValueError:
            print('Async Error')


class PoolAdder(object):
    """
    add proxy to pool
    """

    def __init__(self, threshold):
        self._threshold = threshold  # 代理数量的上线
        self._conn = RedisClient()
        self._tester = ValidityTester()
        self._crawler = FreeProxyGetter()  # 动态获取代理

    def is_over_threshold(self):
        """
        judge if count is overflow.
        """
        if self._conn.queue_len >= self._threshold:  # 判断是否达到上线
            return True
        else:
            return False

    def add_to_queue(self):
        print('PoolAdder is working')
        proxy_count = 0
        while not self.is_over_threshold():
            for callback_label in range(self._crawler.__CrawlFuncCount__):
                callback = self._crawler.__CrawlFunc__[callback_label]
                raw_proxies = self._crawler.get_raw_proxies(callback)
                # test crawled proxies
                self._tester.set_raw_proxies(raw_proxies)
                self._tester.test()
                proxy_count += len(raw_proxies)
                if self.is_over_threshold():
                    print('IP is enough, waiting to be used')
                    break
            if proxy_count == 0:
                raise ResourceDepletionError


class Schedule(object):
    @staticmethod
    def valid_proxy(cycle=VALID_CHECK_CYCLE):
        """
        Get half of proxies which in redis
        """
        conn = RedisClient()
        tester = ValidityTester()
        while True:
            print('Refreshing ip')
            count = int(0.5 * conn.queue_len)
            if count == 0:
                print('Waiting for adding')
                time.sleep(cycle)
                continue
            raw_proxies = conn.get(count)
            tester.set_raw_proxies(raw_proxies)
            tester.test()
            time.sleep(cycle)

    @staticmethod
    def check_pool(lower_threshold=POOL_LOWER_THRESHOLD,
                   upper_threshold=POOL_UPPER_THRESHOLD,
                   cycle=POOL_LEN_CHECK_CYCLE):
        """
        If the number of proxies less than lower_threshold, add proxy
        """
        conn = RedisClient()
        adder = PoolAdder(upper_threshold)
        while True:
            if conn.queue_len < lower_threshold:
                adder.add_to_queue()
            time.sleep(cycle)

    def run(self):
        print('Ip processing running')
        valid_process = Process(target=Schedule.valid_proxy)
        check_process = Process(target=Schedule.check_pool)
        valid_process.start()
        check_process.start()
