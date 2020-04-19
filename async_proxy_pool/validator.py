#!/usr/bin/env python
# coding=utf-8

import os
import asyncio

import aiohttp

from .config import VALIDATOR_BASE_URL, VALIDATOR_BATCH_COUNT, REQUEST_TIMEOUT, DB_TYPE
from .logger import logger
from .database import RedisClient
from .database import MemoryDB


VALIDATOR_BASE_URL = os.environ.get("VALIDATOR_BASE_URL") or VALIDATOR_BASE_URL
db_type = DB_TYPE


class Validator:
    def __init__(self, redis=True):
        if db_type == 'redis':
            self.redis = RedisClient()
        elif db_type == 'memory':
            self.memorydb = MemoryDB()
        self.db_type = db_type

    async def test_proxy(self, proxy):
        """
        测试代理

        :param proxy: 指定代理
        """
        async with aiohttp.ClientSession() as session:
            try:
                if isinstance(proxy, bytes):
                    proxy = proxy.decode("utf8")
                async with session.get(
                    VALIDATOR_BASE_URL, proxy=proxy, timeout=REQUEST_TIMEOUT
                ) as resp:
                    if db_type == 'redis':
                        if resp.status == 200:
                            self.redis.increase_proxy_score(proxy)
                            logger.info("Validator √ {}".format(proxy))
                        else:
                            self.redis.reduce_proxy_score(proxy)
                            logger.info("Validator × {}".format(proxy))
                    elif db_type == 'memory':
                        if resp.status == 200:
                            self.memorydb.increase_proxy_score(proxy)
                            logger.info("Validator √ {}".format(proxy))
                        else:
                            self.memorydb.reduce_proxy_score(proxy)
                            logger.info("Validator × {}".format(proxy))
            except:
                if db_type == 'redis':
                    self.redis.reduce_proxy_score(proxy)
                elif db_type == 'memory':
                    self.memorydb.reduce_proxy_score(proxy)
                logger.info("Validator × {}".format(proxy))

    def run(self):
        """
        启动校验器
        """
        logger.info("Validator working...")
        logger.info("Validator website is {}".format(VALIDATOR_BASE_URL))
        if self.db_type == 'redis':
            proxies = self.redis.all_proxies()
        elif self.db_type == 'memory':
            proxies = self.memorydb.all_proxies()
        loop = asyncio.get_event_loop()
        for i in range(0, len(proxies), VALIDATOR_BATCH_COUNT):
            _proxies = proxies[i : i + VALIDATOR_BATCH_COUNT]
            tasks = [self.test_proxy(proxy) for proxy in _proxies]
            if tasks:
                loop.run_until_complete(asyncio.wait(tasks))
        
        if db_type == 'redis':
            proxy_num = self.redis.count_all_proxies()
            proxy_3 = self.redis.get_proxies(3)
            proxy_10score = self.redis.count_score_proxies(10)
            self.redis.clear_proxies(7)
        elif db_type == 'memory':
            proxy_num = self.memorydb.count_all_proxies()
            proxy_3 = self.memorydb.get_proxies(3)
            proxy_10score = self.memorydb.count_score_proxies(10)
            self.memorydb.clear_proxies(7)
        logger.info("Validator Finish. Proxy Sum: {}".format(str(proxy_num)))
        logger.info("{} proxies is 10 score".format(str(proxy_10score)))
        logger.info("return 3 proxy: {}".format(str([i for i in proxy_3])))
        logger.info('deleted proxy which score <= 7')
        logger.info("Validator resting...")


validator = Validator()
