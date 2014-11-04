#!/usr/bin/env python
#coding:utf8

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

class DomainKeywordBuilder(object):
    def __init__(self, prefix='urlcontent:%s', company='120ask', db='urlcontent'):
        self.company = company
        self.prefix = prefix % self.company
        self.db = db
            
    def process(self):
        try:
            datas = mongo.db[self.db].find({})
            for d in datas[:10]:
                u = d['url']
                c = d['content']
                buf = YhCompress.decompress(c)
                
                tmp = simplejson.loads(buf)
                keyword  = tmp['keyword'] if 'keyword' in tmp else ''
                description = tmp['description'] if 'description' in tmp else ''
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
    Transfer().process()
    Transfer().test()