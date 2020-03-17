#!/usr/bin/env python3
import traceback
import time
import asyncio
import aiohttp
import urllib.parse as urlparse
import farmhash
import lzma
from pymongo import MongoClient
import motor.motor_asyncio
from urlpool import UrlPool
import functions as fn
import config


class NewsCrawlerAsync:
    def __init__(self, name):
        self._workers = 0
        self._workers_max = 5
        self.logger = fn.init_file_logger(name + '.log')
        self.urlpool = UrlPool(name)
        self.loop = asyncio.get_event_loop()
        self.session = aiohttp.ClientSession(loop=self.loop)
        self.db = motor.motor_asyncio.AsyncIOMotorClient(config.MONGO_URI)["haodaifu"]

    async def load_hubs(self,):
        data = self.db.doctor_para.find({"status": {"$ne": True}})
        urls = []
        async for d in data:
            urls.append("{}&{}".format(d['faculty'], d["page"]))
        self.urlpool.addmany(urls)

    async def process(self, url):
        para = {"faculty": url.split("&")[0], "page": url.split("&")[1]}
        status, html = await fn.fetch(self.session, para)
        self.urlpool.set_status(url, status)
        if status != 200:
            return
        await self.db.doctors.insert_one(html)
        await self.db.doctor_para.update_one(para, {"$set": {"status": True}})
        self._workers -= 1

    async def loop_crawl(self):
        await self.load_hubs()
        last_rating_time = time.time()
        counter = 0
        while True:
            tasks = self.urlpool.db.pop_from_redis(self._workers_max)
            if not tasks:
                print('no url to crawl, sleep 10S')
                await asyncio.sleep(10)
                continue
            for url in tasks:
                self._workers += 1
                counter += 1
                print('crawl:', url, self._workers, counter)
                asyncio.ensure_future(self.process(url))

            gap = time.time() - last_rating_time
            if gap > 5:
                rate = counter / gap
                print('\tloop_crawl() rate:%s, counter: %s, workers: %s' % (round(rate, 2), counter, self._workers))
                last_rating_time = time.time()
                counter = 0
            if self._workers >= self._workers_max:
                print('====== got workers_max, sleep 3 sec to next worker =====')
                await asyncio.sleep(1)

    def run(self):
        try:
            self.loop.run_until_complete(self.loop_crawl())
        except KeyboardInterrupt:
            print('stopped by yourself!')
            del self.urlpool
            pass


if __name__ == '__main__':
    nc = NewsCrawlerAsync('haodf-async')
    nc.run()