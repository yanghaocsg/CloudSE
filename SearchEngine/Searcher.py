#!/usr/bin/env python
#coding:utf8

import sys, re, logging, redis,traceback, time, os, simplejson, datetime
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
import YhLog, YhTool, YhChineseNorm, YhMc
import YhTrieSeg, Info, Indexer, Query
from Redis_zero import redis_zero

logger = logging.getLogger(__name__)
cwd = Path(__file__).absolute().ancestor(1)



class Searcher(object):
    def __init__(self, company='120ask', db='tag', cache_prefix='cache'):
        self.cwd = Path(__file__).absolute().ancestor(1)
        self.company = company
        self.cache_prefix = '%s:%s' % (cache_prefix, self.company)
    
    def process(self, query='', start=0, num=10, cache=1, mark=1, st=1):
        try:
            before = datetime.datetime.now()
            if isinstance(query, str):
                query = unicode(query, 'utf8', 'ignore')
            list_s = Query.query.run(query)
            list_url, num_url = [], 0
            if list_s:
                #logger.error('process st %s' % st) 
                list_url, num_url = self.get_cache(query, list_s, start, num, cache, st)
            else:
                raise
            
            dict_res = {'seg':list_s, 'res':list_url, 'status':0, 'totalnum':num_url}
            end = datetime.datetime.now()
            logger.error('query %s list_url %s num_url %s time %s' % (query, len(list_url), num_url, end-before))
            return dict_res
        except:
            dict_res={'status':2, 'errlog':traceback.format_exc()}
            logger.error(traceback.format_exc())
            return dict_res
    def mark_red(self, list_s=[], buf=''):
        res_buf = buf
        for s in list_s[:3]:
            res_buf = re.sub(s, 'mr_begin%smr_end' % s, res_buf)
        return res_buf
        
    def get_cache(self, query='',  list_s=[], start=0, num=10, cache=1, st=1):
        res = YhMc.yhMc.get_cache('%s:%s' % (self.cache_prefix, '|'.join(list_s)))
        list_url,num_url = [], 0
        if res and cache:
            try:
                logger.error('get_cache cached [%s]' % '|'.join(list_s))
                dict_res = simplejson.loads(res)
                list_url = dict_res['list_url']
                num_url = dict_res['num_url']
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
                list_url = Info.Info().getInfoById(list_res[:200])
                num_url = len(list_res)
                for d in list_url:
                    d['mark_red_title'] = self.mark_red(list_s, d['title'])
                    d['mark_red_content'] = self.mark_red(list_s, d['content'])
            buf = simplejson.dumps({'list_url':list_url, 'num_url':num_url})
            if list_url:
                YhMc.yhMc.add_cache('%s:%s' % (self.cache_prefix, '|'.join(list_s)), buf)
                
        list_res_st = []
        #logger.error('get_cache st %s' % st)
        if st==4:
            list_res_st = list_url
        elif st == 3:
            for l in list_url:
                d = {}
                d['id'] = l['id']
                d['title'] = l['title']
                d['content'] = l['content']
                list_res_st.append(d)
        elif st == 2:
            for l in list_url:
                d = {}
                d['id'] = l['id']
                d['title'] = l['title']
                list_res_st.append(d)
        elif st == 1:
            for l in list_url:
                d = {}
                d['id'] = l['id']
                list_res_st.append(d)
        logger.error('get_cache list_res_st %s' % list_res_st[:3])
        return list_res_st[start:start+num], num_url
    
        
searcher = Searcher()
class Search_Handler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    @tornado.gen.engine
    def get(self):
        try:
            dict_qs = YhTool.yh_urlparse_params(self.request.uri, ['query', 's', 'n', 'cache', 'st'], ['', '0', '20', '1', '4'] )
            query, start, num,cache, st = dict_qs['query'], int(dict_qs['s']), int(dict_qs['n']), int(dict_qs['cache']), int(dict_qs['st'])
            logger.error('query[%s]\tstart[%s]\tnum[%s]\tcache[%s]\tst[%s]\tpid[%s]' % (query, start, num, cache, st, os.getpid()))
            #self.set_header('Content-Type', 'application/json; charset=UTF-8')
            self.write(simplejson.dumps(searcher.process(query, start, num, cache, st=st)))
        except Exception:
            logger.error('svs_handler error time[%s][%s][%s]'% (self.request.request_time(), traceback.format_exc(), self.request.uri))
            self.write(simplejson.dumps({'status':1, 'errlog':traceback.format_exc(), 'url':self.request.uri}))
        finally:
            self.finish()
            logger.error('request_time %s [%s]' %(self.request.uri, self.request.request_time()))
            
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
    Searcher().process(sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), cache=0)