#!/usr/bin/env python
#coding:utf8

from tornado.wsgi import WSGIContainer
from tornado.httpserver import HTTPServer
from tornado.ioloop import IOLoop
import tornado.gen, tornado.web

import eventlet, urllib
from eventlet.green import urllib2
import sys, re, logging, redis,traceback, time, datetime, cPickle
import multiprocessing, os, lz4

#self module
sys.path.append('/data/CloudSE/YhHadoop')
import YhLog


logger = logging.getLogger(__file__)

def httpget(url=''):
    data = ''
    with eventlet.timeout.Timeout(3, False):
        try:
            data = urllib2.urlopen(url).read()
        except:
            #logger.error(traceback.format_exc())
            pass
    data = unicode(data, 'utf8', 'ignore')
    return url, data
    


class AgentCrawler(object):
    def __init__(self):
        pass
        
    def process(self, list_url=[]):
        dict_res = {}
        try:
            i = 0
            end = len(list_url)
            
            while i <= end:
                dict_res.update(self.craw(list_url[i:i+10]))
                hour = datetime.datetime.now().hour
                logger.error(hour)
                if hour > 10:
                    time.sleep(2)
                logger.error('Crawler %s %s' % (list_url[i], len(dict_res)))
                i += 10
            #for u in dict_res:
            #    logger.error('%s\t%s' % (u, dict_res[u]))
            logger.error('AgentCrawler %s %s' % (len(list_url), len(dict_res)))
        except:
            logger.error(traceback.format_exc())
        return dict_res
        
    def craw(self, list_url=[], pool_len=10):
        pool = eventlet.greenpool.GreenPool(pool_len)
        dict_res = {}
        for u, d in pool.imap(httpget, list_url):
            if d:
                dict_res[u] = d
        return dict_res
    
    
class AgentCrawler_Handler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    @tornado.gen.engine
    def get(self):
        try:
            str_list_url = self.request.get('lu', '')
            list_url = simplejson.loads(str_list_url)
            dict_res = AgentCrawler().process(list_url)
            logger.error('AgentCrawler_Handler %s %s' % (len(list_url), len(dict_res)))
            return lz4.dumps(simplejson.loads(dict_res))
        except Exception:
            logger.error('svs_handler error time[%s][%s][%s]'% (self.request.request_time(), traceback.format_exc(), self.request.uri))
            self.write(simplejson.dumps({'status':1, 'errlog':traceback.format_exc(), 'url':self.request.uri}))
        finally:
            self.finish()

if __name__=='__main__':
    logger.error('\t'.join(u'abcdef糖尿病'))
    list_url = []
    for q in u'abcdef糖尿病':
        list_url.append('http://m.so.com/suggest/mso?%s&src=mso&callback=suggest' % urllib.urlencode({'kw':q.encode('utf8', 'ignore')}))
    logger.error('\n'.join(list_url))
    AgentCrawler().process(list_url)