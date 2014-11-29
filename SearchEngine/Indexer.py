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
sys.path.append('../YhHadoop')
sys.path.append('../WebCrawler')
import YhLog, YhMongo
import AgentCrawler_360
import YhBitset

logger = logging.getLogger(__file__)
#mongo = YhMongo.yhMongo.mongo_cli
redis_zero = redis.Redis(port=7777, unix_socket_path='/tmp/redis.sock', db=0)
redis_187 = redis.Redis(host='219.239.89.187', port=7777)
#redis_zero = redis.Redis(host='219.239.89.186', port=7777)

class Indexer(object):
    def __init__(self, company='120ask', db='tag', kv_prefix='se:kv', idx_prefix='idx'):
        self.cwd = Path(__file__).absolute().ancestor(1)
        self.company =company
        self.kv_prefix = '%s:%s' % (kv_prefix, self.company)
        self.idx_prefix = '%s:%s' % (idx_prefix, self.company)
        
    def save_kv(self, dict_kv={}):
        for k in dict_kv:
            #redis_zero.set('%s:%s' % (self.kv_prefix, k), cPickle.dumps(dict_kv[k]))
            yhBitset = YhBitset.YhBitset()
            yhBitset.set_list(dict_kv[k])
            redis_zero.set('%s:%s' % (self.kv_prefix, k), yhBitset.to_bytes())
            redis_zero.expire('%s:%s' % (self.kv_prefix, k), 3600 * 24)
        logger.error('save_kv %s' % len(dict_kv))
        
    def get_kv(self, query=u'品他病'):
        list_id = []
        try:
            buf_q = redis_zero.get('%s:%s' % (self.kv_prefix, query))
            if buf_q:
                bitset_q = YhBitSet.YhBitSet()
                bitset_q.from_bytes(buf_q)
                list_id = bitset_q.search()
        except:
            logger.error('get_kv %s %s' % (query, traceback.format_exc()))
            len_add = AgentCrawler_360.agentCrawler_360.add_query(query)
            logger.error('add_query %s %s' % (query,len_add))
        logger.error('get_kv %s %s' % (query, ','.join(list_id)))
        return list_id, len(list_id)
        
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
         self.idx_to_bitset(self)       
        #cPickle.dump(dict_bitset, open('./test.idx', 'w+'))
    
    def idx_to_bitset(dict_bitset):
        i = 0
        pipeline = redis_zero.pipeline()
        for k, set_v in dict_bitset.iteritems():
            yhBitset = YhBitset.YhBitset()
            yhBitset.set_list(set_v)
            pipeline.set('%s:%s' % (self.idx_prefix, k), yhBitset.to_bytes())
            i+=1
            if i % 10000 == 1:
                pipeline.execute()
                logger.error('saved %s' % i)
        pipeline.execute()
        logger.error('saved all %s' % i)
    
    def parse_query(self, list_s=[u'品他病']):
        list_bitset, list_docid = [], []
        for s in list_s:
            str_s = redis_zero.get('%s:%s' % (self.idx_prefix, s))
            if str_s:
                yhBitset = YhBitset.YhBitset()
                yhBitset.from_bytes(str_s)
        if list_bitset:
            bitset = list_bitset[0]
            for bs in list_bitset[1:]:
                bitset.anditem(bs)
            list_docid = bitset.search()
            list_docid.sort(reverse=True)
        logger.error('parse_query seg %s  len %s' % ('|'.join(list_s), len(list_docid)))
        return list_docid[:200], len(list_docid)
        
indexer = Indexer()

if __name__=='__main__':
    #Indexer().process() 
    indexer.load_kv()
    #Indexer().get_kv()
    #indexer.parse_query()