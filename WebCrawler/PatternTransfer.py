#!/usr/bin/env python
#coding:utf8

import eventlet
from eventlet.green import urllib2
import sys, re, logging, redis,traceback, time, os, simplejson
from multiprocessing import Pool, Queue
from collections import defaultdict
from bson.binary import Binary
import lz4,cPickle
#self module
sys.path.append('/data/CloudSE/YhHadoop')
import YhLog, YhMongo
import SitemapCrawler, PatternCrawler


logger = logging.getLogger(__name__)

redis_one = redis.Redis(port=7777, unix_socket_path='/tmp/redis.sock', db=1)
redis_zero = redis.Redis(port=7777, unix_socket_path='/tmp/redis.sock', db=0)
redis_two = redis.Redis(port=7777, unix_socket_path='/tmp/redis.sock', db=2)
pipeline = redis_one.pipeline()
mongo = YhMongo.yhMongo.mongo_cli

class Transfer(object):
    def __init__(self, prefix='urlcontent:%s', company='120ask', db='content', pattern_url_id=r'/(\d+?).htm', pattern_title=r'<title>(.*?)</title>', pattern_description='<meta name="description" content="(.*?)">', pattern_keyword='<meta name="keywords" content="(.*?)">'):
        self.company = company
        self.prefix = prefix % self.company
        self.db = db
        mongo.db[self.db].ensure_index([('url', 1)], unique=True,  background=True, dropDups =True)
    
    def process(self):
        len_data = 0
        try:
            urls = redis_one.hkeys(self.prefix)
            logger.error('total urls len %s' % len(urls))
            i = 0
            len_data = len(urls)
            
            while i <= len(urls):
                datas = redis_one.hmget(self.prefix, urls[i:i+10000])
                for u,d in zip(urls[i:i+10000], datas):
                    if d and len(d)>1:
                        try:
                            mongo.db[self.db].insert({'url':u, 'data':Binary(d)}, continue_on_error=True)
                        except:
                            logger.error(traceback.format_exc())
                    redis_one.hset(self.prefix, u, 1)
                logger.error('Transfer %s %s' % (i, mongo.db[self.db].count()))
                i += 10000
                
        except:
            logger.error(traceback.format_exc())
        return i
    
    def test(self):
        logger.error('test')
        datas = mongo.db[self.db].find({})
        for d in datas[:3]:
            buf = cPickle.loads(lz4.loads(d['data']))
            logger.error('|'.join([buf[k] for k in ['url','title', 'keyword', 'description']]))
        return mongo.db[self.db].count()
    
if __name__=='__main__':
    p = os.fork()
    if p:
        sys.exit()
    else:
        while True:
            len_data = Transfer().process()
            len_test = Transfer().test()
            if len_data:
                logger.error('pattern_transfer %s add %s' % (len_data, len_test))
                
            else:
                time.sleep(1800)    
