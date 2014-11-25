#!/usr/bin/env python
#coding:utf8

import sys, re, logging, redis,traceback, time, os, simplejson, subprocess
from multiprocessing import Pool, Queue, Process
from collections import defaultdict
from bson.binary import Binary
import string
from unipath import Path
import cPickle, copy_reg
from bitstring import BitArray
import lz4
from tornado.wsgi import WSGIContainer
from tornado.httpserver import HTTPServer
from tornado.ioloop import IOLoop
import tornado.gen, tornado.web
#self module
sys.path.append('../YhHadoop')


import YhLog, YhPinyin, YhTool


logger = logging.getLogger(__file__)
redis_zero = redis.Redis(port=7777, unix_socket_path='/tmp/redis.sock')

class SugIndexer(object):
    def __init__(self, company='120ask', idx_prefix='sug:idx', kv_prefix='sug:kv', query_pic='./data/query.pic'):
        self.cwd = Path(__file__).absolute().ancestor(1)
        self.company =company
        self.idx_prefix = '%s:%s' % (idx_prefix, self.company)
        self.kv_prefix = '%s:%s' % (kv_prefix, self.company)
        self.query_pic = query_pic
        self.dict_kw = defaultdict(int)
        try:
            self.dict_kw = cPickle.load(open(Path(self.cwd, query_pic)))
        except:
            logger.error('load tag_120ask.pic error %s' % traceback.format_exc())
            
    def load_tag(self, ifn_sug_pic='../WebCrawler/tag_120ask.txt.sug.pic', ifn_se_pic='../WebCrawler/tag_120ask.se.pic'):
        pipeline = redis_zero.pipeline()
        dict_sug = cPickle.load(open(Path(self.cwd, ifn_sug_pic)))
        dict_se = cPickle.load(open(Path(self.cwd, ifn_se_pic)))
        #save kv
        for k in dict_sug:
            if dict_sug[k]:
                pipeline.set('%s:%s' % (self.kv_prefix, k), ','.join(dict_sug[k]))
                logger.error('load_tag kv %s\t%s' % (k, ','.join(dict_sug[k])))
        pipeline.execute()
        logger.error('load_tag sug:kv %s' % len(dict_sug))
        dict_kw = {}
        for k in dict_sug:
            if k: dict_kw[k] = 1
            for v in dict_sug[k]:
                dict_kw[v] = 1
        for k in dict_se:
            dict_kw[k] = 2
        cPickle.dump(dict_kw, open(Path(self.cwd, self.query_pic), 'w+'))
        self.build_sug(dict_kw)
        self.merge()
        
    def build_sug(self, dict_kw={}):
        list_kw = dict_kw.keys()
        try:
            #pool = Pool(2)
            #copy_reg.pickle(SugIndexer.build_sug_part)
            #res = pool.apply_async(SugIndexer.build_sug_part, [list_kw[:10000], 'sug_part.txt'])
            #logger.error(res.get())
            len_kw = len(list_kw)
            list_p = []
            for i in range(10):
                p = Process(target=SugIndexer.build_sug_part, args=[list_kw[i * len_kw/10:(i+1)*len_kw/10], '%s.%s' % ('sug_part.txt', i)])
                p.start()
                list_p.append(p)
            for p in list_p:
                p.join()
        except:
            logger.error(traceback.format_exc())
    
    def merge(self):
        list_file = Path(__file__).ancestor(1).listdir(pattern='sug_part.txt*')
        dict_sug = defaultdict(set)
        for f in list_file:
            dict_f = cPickle.load(open(f))
            for k in dict_f:
                dict_sug[k] |= dict_f[k]
            logger.error('merge %s %s' % (f, len(dict_f)))
        pipeline = redis_zero.pipeline()
        num_p = 0
        for k in dict_sug:
            pipeline.set('%s:%s' % (self.idx_prefix, k), cPickle.dumps(dict_sug[k]))
            if k in [u'品','abc', 'p']:
                logger.error('test %s\t%s' % (k, '|'.join(dict_sug[k])))
            if num_p % 10000 == 1:
                pipeline.execute()
                logger.error('merge pipeline %s' % num_p)
            num_p += 1
        pipeline.execute()
        logger.error('merge finished %s' % len(dict_sug))
        for f in list_file:
            subprocess.call('rm -rf %s' % f, shell=True)

    
        
    @staticmethod
    def build_sug_part(list_kw={}, ofn='sug_part.txt'):
        logger.error('begin sug_part %s' % len(list_kw))
        dict_part = defaultdict(set)
        for k in list_kw:
            len_k = len(k)
            for i in range(len_k-1):
                for j in range(i + 1, min(len_k,i+20)):
                    dict_part[k[i:j]].add(k)
            list_pinyin = YhPinyin.yhpinyin.line2py_list(k)
            for i in range(1, len_k+1):
                k_pinyin = ''.join(list_pinyin[j][0] for j in range(i))
                dict_part[k_pinyin].add(k)
            str_pinyin = ''.join(list_pinyin)
            for i in range(1,11):
                dict_part[str_pinyin[:i]].add(k)
        cPickle.dump(dict_part, open(Path(Path(__file__).ancestor(1), ofn), 'w+'))
        logger.error('build_sug_part %s' % len(dict_part))
        dict_t = cPickle.load(open(Path(Path(__file__).ancestor(1), ofn)))
        '''
        for k in dict_t:
            logger.error('%s\t%s' % (k, '|'.join(dict_t[k])))
        '''
        return len(dict_t)
    
    def get(self, query=u'品他病'):
        list_kv, len_kv = self.get_kv(query)
        set_idx, len_idx = self.get_idx(query)
        sorted_idx = sorted(set_idx, key=lambda x: self.dict_kw[x], reverse=True)
        set_kv = set(list_kv)
        
        for s in sorted_idx:
            if s not in set_kv:
                list_kv.append(s)
        return list_kv[:10]
        
    def get_kv(self, query=u'品他病'):
        list_id = []
        try:
            buf_q = redis_zero.get('%s:%s' % (self.kv_prefix, query))
            list_id = unicode(buf_q, 'utf8', 'ignore').split(',')
        except:
            logger.error('get_kv %s %s' % (query, traceback.format_exc()))
        logger.error('get_kv %s %s' % (query, ','.join(list_id)))
        return list_id, len(list_id)
    
    def get_idx(self, query=u'abc'):
        set_id = set()
        try:
            set_id = cPickle.loads(redis_zero.get('%s:%s' % (self.idx_prefix, query)))
        except:
            logger.error('get_kv %s %s' % (query, traceback.format_exc()))
        logger.error('get_idx %s %s' % (query, len(set_id)))
        return set_id, len(set_id)
    
class Sug_Handler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    @tornado.gen.engine
    def get(self):
        try:
            dict_qs = YhTool.yh_urlparse_params(self.request.uri, ['query', 's', 'n'], ['', '0', '20'] )
            query = dict_qs['query']
            list_sug = sugindexer.get(query)
            logger.error('Sug_Handler %s %s' % (query, len(list_sug)))
            self.write(simplejson.dumps({'status':0, 'query':query, 'sug':list_sug}))
        except Exception:
            logger.error('Sug_Handler error time[%s][%s][%s]'% (self.request.request_time(), traceback.format_exc(), self.request.uri))
            self.write(simplejson.dumps({'status':1, 'errlog':traceback.format_exc(), 'url':self.request.uri}))
        finally:
            self.finish()
            
sugindexer = SugIndexer()

if __name__=='__main__':
    sugindexer.load_tag()
    #sugindexer.merge()
    #sugindexer.get()