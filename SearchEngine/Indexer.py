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
from Redis_zero import redis_zero
import YhBitset

logger = logging.getLogger(__file__)

class Indexer(object):
    def __init__(self, company='120ask', db='tag', kv_prefix='se:kv', idx_prefix='idx'):
        self.cwd = Path(__file__).absolute().ancestor(1)
        self.company =company
        self.kv_prefix = '%s:%s' % (kv_prefix, self.company)
        self.idx_prefix = '%s:%s' % (idx_prefix, self.company)
        
    def save_kv(self, dict_kv={}):
        for k in dict_kv:
            redis_zero.set('%s:%s' % (self.kv_prefix, k), ','.join(dict_kv[v]))
            redis_zero.expire('%s:%s' % (self.kv_prefix, k), 3600 * 24)
        logger.error('save_kv %s' % len(dict_kv))
        
    def get_kv(self, query=u'品他病'):
        list_id = []
        try:
            buf_q = redis_zero.get('%s:%s' % (self.kv_prefix, query))
            list_id = buf_q.split(',')
        except:
            redis_zero.delete('%s:%s' % (self.kv_prefix, query))
            logger.error('get_kv %s %s' % (query, traceback.format_exc()))
            len_add = AgentCrawler_360.agentCrawler_360.add_query(query)
            logger.error('add_query %s %s' % (query,len_add))
        logger.error('get_kv %s %s' % (query, ','.join(list_id)))
        return list_id, len(list_id)
        
    def process(self):
        self.merge()
        
    def merge(self):
        list_file = Path(self.cwd, '../data/').listdir(pattern='120ask.idx.part.*')
        
        len_list_file = len(list_file)
        id_file = 0
        while id_file < len_list_file:
            dict_idx = defaultdict(set)
            for id in range(id_file, min(id_file+100, len_list_file)):
                dict_sub = cPickle.load(open(list_file[id]))
                for k in dict_sub:
                    dict_idx[k] |= dict_sub[k]
                logger.error('merge idx %s file %s len %s totallen %s' % (id, list_file[id], len(dict_sub), len(dict_idx)))
            self.save_idx(dict_idx)
            logger.error('merge files %s total %s' % (id_file, len_list_file))
            id_file += 100
            
    def save_idx(self, dict_idx):
        dict_bitset = {}
        for k in dict_idx:
            yhBitset = YhBitset.YhBitset()
            list_idx = list(dict_idx[k])
            list_idx.sort()
            yhBitset.set_list(list_idx)
            bs_old_str = redis_zero.get('%s:%s' % (self.idx_prefix, k))
            if bs_old_str:
                bs_old = YhBitset.YhBitset()
                bs_old.frombytes(bs_old_str)
                dict_bitset[k] = yhBitset.oritem(bs_old)
            else:
                dict_bitset[k] = yhBitset
            
        i = 0
        pipeline = redis_zero.pipeline()
        for k in dict_bitset:
            pipeline.set('%s:%s' % (self.idx_prefix, k), dict_bitset[k].tobytes())
            i+=1
            if i % 10000 == 1:
                pipeline.execute()
                logger.error('saved idx %s' % i)
        pipeline.execute()
        logger.error('saved all %s' % i)
    
    def parse_query(self, list_s=[u'品他病']):
        list_bitset, list_docid = [], []
        for s in list_s:
            str_s = Redis_zero.redis_zero.get('%s:%s' % (self.idx_prefix, s))
            if str_s:
                yhBitset = YhBitset.YhBitset()
                yhBitset.frombytes(str_s)
                list_bitset.append(yhBitset)
                logger.error('%s matched len %s' % (s, len(yhBitset.search())))
            else:
                logger.error('%s filtered' % s)
        if list_bitset:
            bitset = list_bitset[0]
            for bs in list_bitset[1:]:
                test = bitset.anditem(bs)
                if len(test.search()) == 0:
                    break
                bitset = bitset.anditem(bs)
                logger.error('bitset len %s' % len(bitset.search()))
            list_docid = bitset.search()
            list_docid.sort(reverse=True)
        if len(list_docid)<10:
            list_docid.extend(list_bitset[0].search())
        logger.error('parse_query seg %s  len %s' % ('|'.join(list_s), len(list_docid)))
        return list_docid[:200], len(list_docid)
        
indexer = Indexer()

if __name__=='__main__':
    p = os.fork()
    if p:
        sys.exit()
    else:
        indexer.process() 
    