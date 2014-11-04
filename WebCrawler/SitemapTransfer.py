#!/usr/bin/env python
#coding:utf8

import eventlet
from eventlet.green import urllib2
import sys, re, logging, redis,traceback, time, os
from multiprocessing import Pool, Queue
from collections import defaultdict
from bson.binary import Binary
#self module
sys.path.append('/data/CloudSE/YhHadoop')
import YhLog, YhCompress, YhMongo
import SitemapCrawler, PatternCrawler


logger = logging.getLogger(__name__)

redis_one = redis.Redis(port=7777, unix_socket_path='/tmp/redis.sock', db=1)
pipeline = redis_one.pipeline()
mongo = YhMongo.yhMongo.mongo_cli

class Transfer(object):
    def __init__(self, prefix='sitemap:%s', company='120ask', db='sitemap'):
        self.company = company
        self.prefix = prefix % self.company
        self.db = db
    def process(self):
        try:
            urls = redis_one.hkeys(self.prefix)
            logger.error('total urls len %s' % len(urls))
            i = 0
            while i <= len(urls):
                datas = redis_one.hmget(self.prefix, urls[i:i+10000])
                list_data = []
                for u,d in zip(urls[i:i+10000], datas):
                    if d and len(d)>1:
                        mongo.db[self.db].insert({'url':u, 'content':Binary(d)})
                        redis_one.hset(self.prefix, u, 1)
                        
                #mongo.db.sitemap.insert(list_data)
                #pipeline.execute()
                mongo.db.sitemap.ensure_index([('url', 1)], background=True)
                logger.error('Transfer %s %s' % (i, len(list_data)))
                i += 10000
        except:
            logger.error(traceback.format_exc())
    
if __name__=='__main__':
    #Transfer().process()
    Transfer(prefix='urlcontent:%s', company='120ask', db='urlcontent').process()