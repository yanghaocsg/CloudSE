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
import YhLog, YhMongo, YhTool, YhChineseNorm
import YhTrieSeg, Info, Indexer

logger = logging.getLogger(__name__)
cwd = Path(__file__).absolute().ancestor(1)
mongo = YhMongo.yhMongo.mongo_cli
yhTrieSeg = YhTrieSeg.YhTrieSeg(fn_domain=[Path(cwd, '../data/tag_120ask.txt')], fn_pic=Path(cwd, '../data/trieseg_120ask.pic'))
redis_zero = redis.Redis(port=7777, unix_socket_path='/tmp/redis.sock', db=0)


class Searcher(object):
    def __init__(self, company='120ask', db='tag', cache_prefix='cache'):
        self.cwd = Path(__file__).absolute().ancestor(1)
        self.company = company
        '''
        self.db = mongo[db]
        self.collection = self.db[company]
        self.collection.ensure_index([('tag', 1)], unique=True,  background=True, dropDups =True)
        self.ofh_tag = open(Path(self.cwd, 'tag_%s.txt' % company), 'w+')
        '''
        self.cache_prefix = '%s:%s' % (cache_prefix, self.company)
    
    def process(self, query='', start=0, num=10, cache=1):
        try:
            if isinstance(query, str):
                query = unicode(query, 'utf8', 'ignore')
            query = YhChineseNorm.uniform(query)
            list_s = yhTrieSeg.seg(query)
            logger.error('query %s %s' % (query, '|'.join(list_s)))
            list_url, num_url = [], 0
            if list_s:
                list_url, num_url = self.get_cache(query, list_s, start, num, cache)
            else:
                raise
            logger.error('list_url %s num_url %s' % (len(list_url), num_url))
            dict_res = {'seg':list_s, 'res':list_url, 'status':0, 'totalnum':num_url}
            return dict_res
        except:
            dict_res={'status':2, 'errlog':traceback.format_exc()}
            logger.error(traceback.format_exc())
            return dict_res
            
    def get_cache(self, query='',  list_s=[], start=0, num=10, cache=1):
        res = redis_zero.get('%s:%s' % (self.cache_prefix, '|'.join(list_s)))
        if res and cache:
            try:
                logger.error('get_cache cached [%s]' % '|'.join(list_s))
                dict_res = simplejson.loads(lz4.loads(res))
                return dict_res['list_url'][start:start+num], len(dict_res['list_url'])
            except:
                redis_zero.delete('%s:%s' % (self.cache_prefix, '|'.join(list_s)))
                raise
        else:
            logger.error('get_cache dached [%s]' % '|'.join(list_s))
            list_kv, num_kv = Indexer.Indexer().get_kv(query)
            list_idx, num_idx = Indexer.Indexer().parse_query(list_s)
            list_res = []
            set_res = set()
            list_url = []
            for item in list_kv + list_idx:
                if item not in set_res:
                    set_res.add(item)
                    list_res.append(item)
            if list_res:
                list_url = Info.Info().getInfoById(list_res)
            buf = simplejson.dumps({'list_url':list_url, 'num_url':len(list_url)})
            logger.error('lz4 test len[%s] lz4len[%s]' % (len(buf), len(lz4.dumps(buf))))
            redis_zero.set('%s:%s' % (self.cache_prefix, '|'.join(list_s)), lz4.dumps(buf))
            redis_zero.expire('%s:%s' % (self.cache_prefix, '|'.join(list_s)), 3600)
            return list_url[start:start+num], len(list_url)
            
    
        
searcher = Searcher()
class Search_Handler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    @tornado.gen.engine
    def get(self):
        try:
            dict_qs = YhTool.yh_urlparse_params(self.request.uri, ['query', 's', 'n', 'cache'], ['', '0', '20', '1'] )
            query, start, num,cache = dict_qs['query'], int(dict_qs['s']), int(dict_qs['n']), int(dict_qs['cache'])
            logger.error('%s\t%s\t%s\t%s' % (query, start, num, cache))
            self.write(simplejson.dumps(searcher.process(query, start, num, cache)))
        except Exception:
            logger.error('svs_handler error time[%s][%s][%s]'% (self.request.request_time(), traceback.format_exc(), self.request.uri))
            self.write(simplejson.dumps({'status':1, 'errlog':traceback.format_exc(), 'url':self.request.uri}))
        finally:
            self.finish()
            
class SearchHtml_Handler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    @tornado.gen.engine
    def get(self):
        try:
            dict_qs = YhTool.yh_urlparse_params(self.request.uri, ['query', 's', 'n', 'cache'], ['', '0', '20', '1'] )
            query, start, num,cache = dict_qs['query'], int(dict_qs['s']), int(dict_qs['n']), int(dict_qs['cache'])
            logger.error('%s\t%s\t%s\t%s' % (query, start, num, cache))
            dict_res = searcher.process(query, start, num, cache)
            logger.error(dict_res.keys())
            buf_html = '<html><body><p> 搜索词： %s' % query.encode('utf8')
            
            for r in dict_res['res']:
                buf_html += '<p/><span>'
                buf_html += '<h4><a href=%s>%s</a></h4>' % (r['url'].encode('utf8'), r['title'].encode('utf8'))
                buf_html += '<p/><h5>%s</h5>' % r['content'].encode('utf8')
                buf_html += '</span>'
            buf_html += '</body></html>'
            self.write(buf_html)
        except Exception:
            logger.error('svs_handler error time[%s][%s][%s]'% (self.request.request_time(), traceback.format_exc(), self.request.uri))
            self.write(simplejson.dumps({'status':1, 'errlog':traceback.format_exc(), 'url':self.request.uri}))
        finally:
            self.finish()            
if __name__=='__main__':
    Searcher().process(sys.argv[1], int(sys.argv[2]), int(sys.argv[3]))