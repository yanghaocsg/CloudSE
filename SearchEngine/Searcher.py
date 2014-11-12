#!/usr/bin/env python
#coding:utf8

import sys, re, logging, redis,traceback, time, os, simplejson
from multiprocessing import Pool, Queue
from collections import defaultdict
from bson.binary import Binary
import string
from unipath import Path
import cPickle, lz4

#self module
sys.path.append('../YhHadoop')
import YhLog, YhMongo, YhCrawler
import YhTrieSeg

logger = logging.getLogger(__name__)
cwd = Path(__file__).absolute().ancestor(1)
mongo = YhMongo.yhMongo.mongo_cli
yhTrieSeg = YhTrieSeg.YhTrieSeg([Path(cwd, '../data/tag_120ask.txt')])
redis_zero = redis.Redis(port=7777, unix_socket_path='/tmp/redis.sock', db=0)

class Searcher(object):
    def __init__(self, company='120ask', db='tag'):
        self.cwd = Path(__file__).absolute().ancestor(1)
        self.company = company
        self.db = mongo[db]
        self.collection = self.db[company]
        self.collection.ensure_index([('tag', 1)], unique=True,  background=True, dropDups =True)
        self.ofh_tag = open(Path(self.cwd, 'tag_%s.txt' % company), 'w+')
    def process(self, query=''):
        try:
            list_s = yhTrieSeg.seg(query)
            logger.error('query %s' % '|'.join(list_s))
            if not list_s:
                return {}
            else:
                list_res = []
                for s in self.parse_query(list_s):
                    list_res.append((s, 'http://www.120ask.com/question/%s.html'%s))
                return simplejson.dumps(list_res)
        except:
            logger.error(traceback.format_exc())
            
    def parse_query(self, list_s=[]):
        set_docid = set()
        if list_s:
            for i, s in enumerate(list_s):
                try:
                    if i == 0:
                        set_docid |= cPickle.loads(lz4.loads(redis_zero.get('idx:%s:%s' % (self.company, s))))
                except:
                    logger.error('%s %s' % (s, traceback.format_exc()))
                    return []
        list_docid = list(set_docid)
        list_docid.reverse()
        logger.error('list_docid %s' % list_docid[:20])
        return list_docid[:20]
        
if __name__=='__main__':
    Searcher().process(u'糖尿病')