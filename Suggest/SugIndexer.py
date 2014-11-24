#!/usr/bin/env python
#coding:utf8

import sys, re, logging, redis,traceback, time, os, simplejson
from multiprocessing import Pool, Queue, Process
from collections import defaultdict
from bson.binary import Binary
import string
from unipath import Path
import cPickle
from bitstring import BitArray
import lz4
#self module
sys.path.append('/data/CloudSE/YhHadoop')
import YhLog, YhPinyin


logger = logging.getLogger(__file__)
redis_zero = redis.Redis(port=7777, unix_socket_path='/tmp/redis.sock')

class SugIndexer(object):
    def __init__(self, company='120ask', idx_prefix='sug:idx', kv_prefix='sug:kv', query_pic='query.pic'):
        self.cwd = Path(__file__).absolute().ancestor(1)
        self.company =company
        self.idx_prefix = '%s:%s' % (idx_prefix, self.company)
        self.kv_prefix = '%s:%s' % (kv_prefix, self.company)
        self.query_pic = query_pic
        
    def load_tag(self, ifn_sug_pic='../WebCrawler/tag_120ask.txt.sug.pic', ifn_se_pic='../WebCrawler/tag_120ask.se.pic'):
        pipeline = redis_zero.pipeline()
        dict_sug = cPickle.load(open(ifn_sug_pic))
        dict_se = cPickle.load(open(ifn_se_pic))
        #save kv
        for k in dict_sug:
            if dict_sug[k]:
                pipeline.set('%s:%s' % (self.kv_prefix, k), ','.join(dict_sug[k]))
        pipeline.execute()
        logger.error('load_tag sug:kv %s' % len(dict_sug))
        dict_kw = {}
        for k in dict_sug:
            if k: dict_kw[k] = 1
            for v in dict_sug[k]:
                dict_kw[v] = 1
        for k in dict_se:
            dict_kw[k] = 2
        cPickle.dump(dict_kw, open(self.query_pic, 'w+'))
        self.build_sug(dict_kw)
    
    def build_sug(self, dict_kw={}):
        dict_part = {}
        for k in dict_kw:
            dict_part[k] = dict_kw[k]
            if len(dict_part)>=10000:
                break
        logger.error('len_part %s' % len(dict_part))
        try:
            #pool = Pool(10)
            #pool.apply_async(SugIndexer.build_sug_part, [dict_part, 'sug_part.txt'])
            Process(target=SugIndexer.build_sug_part, args=[dict_part, 'sug_part.txt']).start()
        except:
            logger.error(traceback.format_exc())
    
    @staticmethod
    def build_sug_part(dict_kw={}, ofn='sug_part.txt'):
        logger.error('begin sug_part %s' % len(dict_kw))
        dict_part = defaultdict(set)
        for k in dict_kw:
            len_k = len(k)
            for i in range(len_k-1):
                for j in range(len_k,i+20):
                    dict_part[k[i:j]].add(k)
            list_pinyin = YhPinyin.yhpinyin.line2py_list(k)
            for i in range(1, len_k+1):
                k_pinyin = ''.join(list_pinyin[j][0] for j in range(i))
                dict_part[k_pinyin].add(k)
            str_pinyin = ''.join(list_pinyin)
            for i in range(1,11):
                dict_part[str_pinyin[:i]].add(k)
        cPickle.dump(dict_part, open(ofn, 'w+'))
        logger.error('build_sug_part %s' % len(dict_part))
        dict_t = cPickle.load(open(ofn))
        for k in dict_t:
            logger.error('%s\t%s' % (k, '|'.join(dict_t[k])))
            
    def get_kv(self, query=u'品他病'):
        list_id = []
        try:
            buf_q = redis_zero.get('%s:%s' % (self.kv_prefix, query))
            list_id = unicode(buf_q, 'utf8', 'ignore').split(',')
        except:
            logger.error('get_kv %s %s' % (query, traceback.format_exc()))
        logger.error('get_kv %s %s' % (query, ','.join(list_id)))
        return list_id, len(list_id)
        
        
sugindexer = SugIndexer()

if __name__=='__main__':
    sugindexer.load_tag()