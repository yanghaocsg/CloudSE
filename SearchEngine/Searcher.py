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
import YhTrieSeg, Info

logger = logging.getLogger(__name__)
cwd = Path(__file__).absolute().ancestor(1)
mongo = YhMongo.yhMongo.mongo_cli
yhTrieSeg = YhTrieSeg.YhTrieSeg(fn_domain=[Path(cwd, '../data/tag_120ask.txt')], fn_pic=Path(cwd, '../data/trieseg_120ask.pic'))
redis_zero = redis.Redis(port=7777, unix_socket_path='/tmp/redis.sock', db=0)
redis_187 = redis.Redis(host='219.239.89.187', port=7777)

class Searcher(object):
    def __init__(self, company='120ask', db='tag', cache_prefix='cache:%s:%s'):
        self.cwd = Path(__file__).absolute().ancestor(1)
        self.company = company
        self.db = mongo[db]
        self.collection = self.db[company]
        self.collection.ensure_index([('tag', 1)], unique=True,  background=True, dropDups =True)
        self.ofh_tag = open(Path(self.cwd, 'tag_%s.txt' % company), 'w+')
        self.cache_prefix = cache_prefix
        
    def process(self, query='', start=0, num=10):
        try:
            if isinstance(query, str):
                query = unicode(query, 'utf8', 'ignore')
            list_s = yhTrieSeg.seg(query)
            logger.error('query %s %s' % (query, '|'.join(list_s)))
            list_url, num_url = [], 0
            if list_s:
                list_url, num_url = self.get_cache(list_s, start, num)
            else:
                raise
            logger.error('list_url %s num_url %s' % (len(list_url), num_url))
            dict_res = {'seg':list_s, 'res':list_url, 'status':0, 'totalnum':num_url}
            return simplejson.dumps(dict_res)
        except:
            dict_res={'status':2, 'errlog':traceback.format_exc()}
            logger.error(traceback.format_exc())
            return simplejson.dumps(dict_res)
            
    def get_cache(self, list_s=[], start=0, num=10):
        res = redis_187.get(self.cache_prefix  % (self.company, '|'.join(list_s)))
        if res:
            try:
                logger.error('get_cache cached [%s]' % '|'.join(list_s))
                dict_res = simplejson.loads(lz4.loads(res))
                return dict_res['list_url'][start:start+num], dict_res['num_url']
            except:
                redis_187.delete(self.cache_prefix % (self.company, '|'.join(list_s)))
                raise
        else:
            logger.error('get_cache dached [%s]' % '|'.join(list_s))
            list_res, num_res = self.parse_query(list_s)
            if list_res:
                list_url = Info.Info().getInfoById(list_res)
            buf = simplejson.dumps({'list_url':list_url, 'num_url':num_res})
            logger.error('lz4 test len[%s] lz4len[%s]' % (len(buf), len(lz4.dumps(buf))))
            redis_187.set(self.cache_prefix % (self.company, '|'.join(list_s)), lz4.dumps(buf))
            redis_187.expire(self.cache_prefix % (self.company, '|'.join(list_s)), 3600)
            return list_url[start:start+num], num_res
            
    def parse_query(self, list_s=[]):
        set_docid = set()
        if list_s:
            for i, s in enumerate(list_s):
                try:
                    if i == 0:
                        set_docid |= cPickle.loads(lz4.loads(redis_zero.get('idx:%s:%s' % (self.company, s))))
                    else:
                        set_docid &= cPickle.loads(lz4.loads(redis_zero.get('idx:%s:%s' % (self.company, s))))
                except:
                    logger.error('parse_query %s %s' % (s, traceback.format_exc()))
                    return [], 0
        list_docid = list(set_docid)
        list_docid.reverse()
        if len(list_docid)<20:
            set_docid = cPickle.loads(lz4.loads(redis_zero.get('idx:%s:%s' % (self.company, s))))
            if set_docid:
                list_docid.extend([s for s in list(set_docid)[:20] and s not in list_docid])
        logger.error('parse_query seg %s  len %s' % ('|'.join(list_s), len(list_docid)))
        return list_docid[:200], len(list_docid)
        
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
    Searcher().process(sys.argv[1], int(sys.argv[2]), int(sys.argv[3]))