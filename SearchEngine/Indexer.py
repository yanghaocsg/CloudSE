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


logger = logging.getLogger(__file__)
#mongo = YhMongo.yhMongo.mongo_cli
#redis_zero = redis.Redis(port=7777, unix_socket_path='/tmp/redis.sock', db=0)
#redis_187 = redis.Redis(host='219.239.89.187', port=7777)
redis_zero = redis.Redis(host='219.239.89.186', port=7777)

class Indexer(object):
    def __init__(self, company='120ask', db='tag', kv_prefix='se:kv', idx_prefix='idx'):
        self.cwd = Path(__file__).absolute().ancestor(1)
        self.company =company
        self.kv_prefix = '%s:%s' % (kv_prefix, self.company)
        self.idx_prefix = '%s:%s' % (idx_prefix, self.company)
    
    def load_kv(self, ifn='../WebCrawler/tag_120ask.se.pic', pattern='/(\d+?).htm'):
        pipeline = redis_zero.pipeline()
        dict_res = cPickle.load(open(ifn))
        dict_kv = defaultdict(list)
        for k in dict_res:
            for v in dict_res[k]:
                re_v = re.search(pattern, v)
                if re_v:
                    id_kv = re_v.group(1)
                    dict_kv[k].append(id_kv)
            if dict_kv[k]:
                logger.error('%s\t%s' % (k, '|'.join(dict_kv[k])))
        for k in dict_kv:
            if dict_kv[k]:
                redis_zero.set('%s:%s' % (self.kv_prefix, k), cPickle.dumps(dict_kv[k]))
        pipeline.execute()
        logger.error('load_kv %s ' % len([k for k in dict_kv if dict_kv[k]]))
    
    def get_kv(self, query=u'品他病'):
        list_id = []
        try:
            buf_q = redis_zero.get('%s:%s' % (self.kv_prefix, query))
            list_id = cPickle.loads(buf_q)
        except:
            logger.error('get_kv %s %s' % (query, traceback.format_exc()))
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
            pipeline.set('%s:%s' % (self.idx_prefix, k), dict_bitmap[k])
            i+=1
            if i % 10000 == 1:
                pipeline.execute()
                logger.error('saved %s' % i)
        pipeline.execute()
        logger.error('saved all %s' % i)
    
    def parse_query(self, list_s=[u'品他病']):
        set_docid = set()
        if list_s:
            for i, s in enumerate(list_s):
                logger.error('%s:%s' % (self.idx_prefix, s))
                try:
                    if i == 0:
                        str_s = redis_zero.get('%s:%s' % (self.idx_prefix, s))
                        if str_s: set_docid |= cPickle.loads(lz4.loads(str_s))
                    else:
                        str_s = redis_zero.get('%s:%s' % (self.idx_prefix, s))
                        if str_s: set_docid &= cPickle.loads(lz4.loads(str_s))
                except:
                    logger.error('parse_query %s %s' % (s, traceback.format_exc()))
                    return [], 0
        list_docid = list(set_docid)
        list_docid.reverse()
        if len(list_s)>=2 and len(list_docid)<20:
            set_new = cPickle.loads(lz4.loads(redis_zero.get('%s:%s' % (self.idx_prefix, list_s[0]))))
            if set_new:
                list_docid.extend([s for s in set_new if s not in set_docid])
        logger.error('parse_query seg %s  len %s' % ('|'.join(list_s), len(list_docid)))
        return list_docid[:200], len(list_docid)
        
indexer = Indexer()

if __name__=='__main__':
    #Indexer().process() 
    #Indexer().load_kv()
    Indexer().get_kv()
    indexer.parse_query()