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
import YhLog
import AgentCrawler_360
from Redis_zero import redis_zero
import YhBitset

logger = logging.getLogger(__file__)

class Indexer(object):
    def __init__(self, company='120ask', db='tag', kv_prefix='se:kv', idx_prefix='idx', title_prefix = 'title'):
        self.cwd = Path(__file__).absolute().ancestor(1)
        self.company =company
        self.kv_prefix = '%s:%s' % (kv_prefix, self.company)
        self.idx_prefix = '%s:%s' % (idx_prefix, self.company)
        self.title_prefix = '%s:%s' % (title_prefix, self.company)
        
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
            #logger.error('get_kv %s %s' % (query, traceback.format_exc()))
            #len_add = AgentCrawler_360.agentCrawler_360.add_query(query)
            #logger.error('add_query %s %s' % (query,len_add))
        #logger.error('get_kv %s %s' % (query, ','.join(list_id)))
        return list_id, len(list_id)
        
    def process(self):
        self.merge_title()
        #self.merge_idx()
        
    def merge_title(self):
        list_file = Path(self.cwd, '../data/').listdir(pattern='120ask.title.part.*')
        len_list_file = len(list_file)
        id_file = 0
        while id_file < len_list_file:
            dict_idx = defaultdict(set)
            for id in range(id_file, min(id_file+100, len_list_file)):
                dict_sub = cPickle.load(open(list_file[id]))
                for k in dict_sub:
                    dict_idx[k] |= dict_sub[k]
                logger.error('merge idx %s file %s len %s totallen %s' % (id, list_file[id], len(dict_sub), len(dict_idx)))
            self.save_idx(dict_idx, self.title_prefix)
            logger.error('merge files %s total %s' % (id_file, len_list_file))
            id_file += 100
    
    def merge_idx(self):
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
            self.save_idx(dict_idx, self.idx_prefix)
            logger.error('merge files %s total %s' % (id_file, len_list_file))
            id_file += 100
        
    def save_idx(self, dict_idx, prefix=''):
        if not prefix:
            raise
        dict_bitset = {}
        for k in dict_idx:
            yhBitset = YhBitset.YhBitset()
            list_idx = list(dict_idx[k])
            list_idx.sort()
            yhBitset.set_list(list_idx)
            bs_old_str = redis_zero.get('%s:%s' % (prefix, k))
            if bs_old_str:
                bs_old = YhBitset.YhBitset()
                bs_old.frombytes(bs_old_str)
                dict_bitset[k] = yhBitset.oritem(bs_old)
            else:
                dict_bitset[k] = yhBitset
            
        i = 0
        pipeline = redis_zero.pipeline()
        for k in dict_bitset:
            pipeline.set('%s:%s' % (prefix, k), dict_bitset[k].tobytes())
            i+=1
            if i % 10000 == 1:
                pipeline.execute()
                logger.error('saved prefix %s num %s' % (prefix,  i))
        pipeline.execute()
        logger.error('saved prefix %s all  %s' % (prefix, i))
    
    def parse_query(self, list_s=[u'品他病']):
        list_id = []
        list_id.extend(self.match(list_s, self.title_prefix))
        if len(list_id)<10:
            list_id.extend(self.match(list_s, self.idx_prefix))
        if len(list_id)<10:
            list_id.extend(self.match_fuzzy(list_s, self.title_prefix))
        return list_id, len(list_id)
        
        
    def match(self, list_s=[], prefix=''):
        list_bitset, list_docid, len_list_docid = [], [], 0
        for s in list_s:
            str_s = redis_zero.get('%s:%s' % (prefix, s))
            if str_s:
                yhBitset = YhBitset.YhBitset()
                yhBitset.frombytes(str_s)
                list_bitset.append(yhBitset)
                #logger.error('%s matched len %s' % (s, len(yhBitset.search())))
            else:
                logger.error('%s filtered' % s)
        bitset = YhBitset.YhBitset()
        if list_bitset:
            bitset = list_bitset[0]
            for bs in list_bitset[1:]:
                test = bitset.anditem(bs)
                if len(test.search(200, 1))<10:
                    break
                bitset = test
                
        list_docid = bitset.search(200, 1)
        #list_docid.sort(reverse=True)
        #logger.error('match_title seg %s  len %s ids %s' % ('|'.join(list_s), len(list_docid), list_docid[:3]))
        return list_docid[:200]
    
    def match_fuzzy(self, list_s=[], prefix=''):
        list_bitset, list_docid, len_list_docid = [], [], 0
        for s in list_s:
            str_s = redis_zero.get('%s:%s' % (prefix, s))
            if str_s:
                yhBitset = YhBitset.YhBitset()
                yhBitset.frombytes(str_s)
                list_docid = yhBitset.search(200, 1)
                break
            else:
                logger.error('%s filtered' % s)
        #logger.error('match_title seg %s  len %s ids %s' % ('|'.join(list_s), len(list_docid), list_docid[:3]))
        return list_docid[:200]
    
    
    
        
indexer = Indexer()

if __name__=='__main__':
    p = os.fork()
    if p:
        sys.exit()
    else:
        indexer.process() 
    