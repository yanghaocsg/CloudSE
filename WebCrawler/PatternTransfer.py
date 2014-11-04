#!/usr/bin/env python
#coding:utf8

import eventlet
from eventlet.green import urllib2
import sys, re, logging, redis,traceback, time, os, simplejson
from multiprocessing import Pool, Queue
from collections import defaultdict
from bson.binary import Binary

#self module
sys.path.append('/data/CloudSE/YhHadoop')
import YhLog, YhCompress, YhMongo
import SitemapCrawler, PatternCrawler


logger = logging.getLogger(__name__)

redis_one = redis.Redis(port=7777, unix_socket_path='/tmp/redis.sock', db=1)
redis_zero = redis.Redis(port=7777, unix_socket_path='/tmp/redis.sock', db=0)
redis_two = redis.Redis(port=7777, unix_socket_path='/tmp/redis.sock', db=2)
pipeline = redis_one.pipeline()
mongo = YhMongo.yhMongo.mongo_cli

class Transfer(object):
    def __init__(self, prefix='urlcontent:%s', company='120ask', db='urlcontent'):
        self.company = company
        self.prefix = prefix % self.company
        self.db = db
        mongo.db[self.db].ensure_index([('url', 1)], unique=True,  background=True, dropDups =True)
            
    def process(self):
        try:
            urls = redis_one.hkeys(self.prefix)
            logger.error('total urls len %s' % len(urls))
            i = 0
            while i <= len(urls):
                datas = redis_one.hmget(self.prefix, urls[i:i+10000])
                for u,d in zip(urls[i:i+10000], datas):
                    if d and len(d)>1:
                        try:
                            dict_res = {'url':u, 'content':Binary(d)}
                            mongo.db[self.db].insert(dict_res, continue_on_error=True)
                        except:
                            logger.error(traceback.format_exc())
                        redis_one.hset(self.prefix, u, 1)
                logger.error('Transfer %s %s' % (i, mongo.db[self.db].count()))
                i += 10000
        except:
            logger.error(traceback.format_exc())
    
    def test(self):
        datas = mongo.db[self.db].find({})
        for d in datas[:10]:
            u = d['url']
            c = d['content']
            buf = YhCompress.decompress(c)
            
            tmp = simplejson.loads(buf)
            keyword  = tmp['keyword'] if 'keyword' in tmp else ''
            description = tmp['description'] if 'description' in tmp else ''
            
            logger.error('%s\t%s\t%s\t%s' % (u, keyword, description, tmp['title']))
            
        
if __name__=='__main__':
    p = os.fork()
    if p:
        sys.exit()
    else:
        while True:
            Transfer().process()
            Transfer().test()
            time.sleep(3600*2)