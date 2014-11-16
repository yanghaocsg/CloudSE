#!/usr/bin/env python
#coding:utf8

import sys, re, logging, redis,traceback, time, os, simplejson
from multiprocessing import Pool, Queue
from collections import defaultdict
from bson.binary import Binary
import string
from unipath import Path
import cPickle, lz4
from tornado.wsgi import WSGIContainer
from tornado.httpserver import HTTPServer
from tornado.ioloop import IOLoop
import tornado.gen, tornado.web


#self module
sys.path.append('../YhHadoop')
import YhLog, YhMongo, YhTool
import YhTrieSeg

logger = logging.getLogger(__name__)
cwd = Path(__file__).absolute().ancestor(1)
mongo = YhMongo.yhMongo.mongo_cli
yhTrieSeg = YhTrieSeg.YhTrieSeg(fn_domain=[Path(cwd, '../data/tag_120ask.txt')], fn_pic=Path(cwd, '../data/trieseg_120ask.pic'))
redis_zero = redis.Redis(port=7777, unix_socket_path='/tmp/redis.sock', db=0)

class Searcher(object):
    def __init__(self, company='120ask', db='tag'):
        self.cwd = Path(__file__).absolute().ancestor(1)
        self.company = company
        self.db = mongo[db]
        self.collection = self.db[company]
        self.collection.ensure_index([('tag', 1)], unique=True,  background=True, dropDups =True)
        self.ofh_tag = open(Path(self.cwd, 'tag_%s.txt' % company), 'w+')
    def process(self, query='', start=0, num=20):
        try:
            list_s = yhTrieSeg.seg(query)
            logger.error('query %s %s' % (query, '|'.join(list_s)))
            list_res = []
            if list_s:
                list_res = self.parse_query(list_s, start, num)
                list_url = []
                for s in self.parse_query(list_s):
                    list_url.append((s, 'http://www.120ask.com/question/%s.htm'%s))
            
            dict_res = {'seg':list_s, 'res':list_url, 'status':0}
            return simplejson.dumps(dict_res)
        except:
            dict_res={'status':2, 'errlog':traceback.format_exc()}
            logger.error(traceback.format_exc())
            return simplejson.dumps(dict_res)
            
    def parse_query(self, list_s=[],start=0, num=20):
        set_docid = set()
        if list_s:
            for i, s in enumerate(list_s):
                try:
                    if i == 0:
                        set_docid |= cPickle.loads(lz4.loads(redis_zero.get('idx:%s:%s' % (self.company, s))))
                    else:
                        set_docid &= cPickle.loads(lz4.loads(redis_zero.get('idx:%s:%s' % (self.company, s))))
                except:
                    logger.error('%s %s' % (s, traceback.format_exc()))
                    return []
        list_docid = list(set_docid)
        list_docid.reverse()
        if len(list_docid)<20:
            set_docid = cPickle.loads(lz4.loads(redis_zero.get('idx:%s:%s' % (self.company, s))))
            if set_docid:
                list_docid.extend(list(set_docid)[:20])
        logger.error('list_docid %s' % list_docid[start:num])
        return list_docid[start:num]
        
searcher = Searcher()
class Search_Handler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    @tornado.gen.engine
    def get(self):
        try:
            dict_qs = YhTool.yh_urlparse_params(self.request.uri, ['query', 's', 'n'], ['', '0', '20'])
            query, start, num = dict_qs['query'], int(dict_qs['s']), int(dict_qs['n'])
            self.write(searcher.process(query, start, num))
        except Exception:
            logger.error('svs_handler error time[%s][%s][%s]'% (self.request.request_time(), traceback.format_exc(), self.request.uri))
            self.write(simplejson.dumps({'status':1, 'errlog':traceback.format_exc(), 'url':self.request.uri}))
        finally:
            self.finish()
            
if __name__=='__main__':
    Searcher().process(u'糖尿病')