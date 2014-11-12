#!/usr/bin/env python
#coding:utf8

import sys, re, logging, redis,traceback, time, os, simplejson
from multiprocessing import Pool, Queue
from collections import defaultdict
from bson.binary import Binary
import string
from unipath import Path
import cPickle
from bitstring import BitArray
import lz4
#self module
sys.path.append('/data/CloudSE/YhHadoop')
import YhLog, YhCompress, YhMongo, YhCrawler


logger = logging.getLogger(__name__)
mongo = YhMongo.yhMongo.mongo_cli
redis_zero = redis.Redis(port=7777, unix_socket_path='/tmp/redis.sock', db=0)
class Indexer(object):
    def __init__(self, company='120ask', db='tag'):
        self.cwd = Path(__file__).absolute().ancestor(1)
        self.company =company
    def process(self):
        logger.error('begin process ' )
        dict_company = {'name':'120ask', 'db_urlcontent':'urlcontent', 'prefix_db': 'tmp:db:%s', 'ofn_db':'../data/%s.db.%s','ofn_key_idx':'../data/%s.key.idx', 'ofn_content_idx':'../data/%s.content.idx', 'pattern_url_id':r'/(\d+?).htm', 'step':10000}
        #Process(target=dump, args=(dict_company, )).start()
        #self.merge()
        #self.idx_to_bitmap()
        self.bitmap_to_redis()
    def merge(self):
        pattern_url_id = r'/(\d+?).htm'
        dict_bitset = defaultdict(set)
        for i in range(6000):
            try:
                dict_tmp = cPickle.load(open('../data/120ask.idx.%s' % i))
                for k, set_v in dict_tmp.iteritems():
                    set_t = set()
                    for v in set_v:
                        t = 0
                        if unicode.isnumeric(v):
                            t = int(v)
                        elif v[:4]=='http':
                            re_url_id = re.search(pattern_url_id, v)
                            t = int(re_url_id.group(1))
                        set_t.add(t)
                    if k in dict_bitset:
                        dict_bitset[k] |= set_t
                    else:
                        dict_bitset[k] = set_t
                    #logger.error('%s\t%s' % (k, set_t))
                logger.error('%s\t%s' % (i, len(dict_bitset)))
            except:
                logger.error('%s\t%s' % (i, traceback.format_exc()))
                
        cPickle.dump(dict_bitset, open('./test.idx', 'w+'))
    
    def idx_to_bitmap(self, start=0):
        dict_bitset= cPickle.load(open('./test.idx'))
        logger.error('dict_bitset %s' % len(dict_bitset))
        dict_bitarray = {}
        dict_bitarraybytes = {}
        for k, set_v in dict_bitset.iteritems():
            dict_bitarray[k] = lz4.dumps(cPickle.dumps(set_v))
            logger.error('dict_bitarray %s' % len(dict_bitarray))
            #dict_bitarraybytes[k] = bitarray_tmp.tobytes()
        cPickle.dump(dict_bitarray, open('./test.bitarray', 'w+'))
        #cPickle.dump(dict_bitarraybyte, open('./test.bitarraybyte', 'w+'))
        logger.error('dict_bitarray %s ' % len(dict_bitarray))
    
    def bitmap_to_redis(self):
        dict_bitmap = cPickle.load(open('./test.bitarray'))
        i = 0
        pipeline = redis_zero.pipeline()
        for k in dict_bitmap:
            pipeline.set('idx:%s:%s' % (self.company, k), dict_bitmap[k])
            i+=1
            if i % 10000 == 1:
                pipeline.execute()
                logger.error('saved %s' % i)
        pipeline.execute()
        logger.error('saved all %s' % i)
if __name__=='__main__':
    Indexer().process() 