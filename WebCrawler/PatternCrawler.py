#!/usr/bin/env python
#coding:utf8

import eventlet
from eventlet.green import urllib2
import sys, re, logging, redis,traceback, time
import multiprocessing, os

#self module
sys.path.append('/data/CloudSE/YhHadoop')
import YhLog, YhCompress


logger = logging.getLogger(__name__)

redis_one = redis.Redis(port=7777, unix_socket_path='/tmp/redis.sock', db=1)
    
def httpget(url=''):
    data = ''
    with eventlet.timeout.Timeout(3, False):
        try:
            data = urllib2.urlopen(url).read()
        except:
            logger.error(traceback.format_exc())
    logger.error('httpget %s %s' % (url, len(data)))
    return url, data
    
def craw(list_url=[]):
    pool = eventlet.greenpool.GreenPool(30)
    dict_res = {}
    for u, d in pool.imap(httpget, list_url):
        if d:
            dict_res[u] = d
    return dict_res

class Crawler(object):
    def __init__(self, company='120ask'):
        self.company = company
        self.prefix = 'urlcontent:%s' % self.company

    def save(self, dict_res={}):
        for u,v in dict_res.iteritems():
            redis_one.hset(self.prefix, u,  YhCompress.compress(v))
        
    def notsaved(self, list_url=[]):
        if list_url:
            list_res = redis_one.hmget(self.prefix, list_url)
            list_saved, list_notsaved=[], []
            for i, r in enumerate(list_res):
                if not r:
                    list_notsaved.append(list_url[i])
                    #logger.error('%s\t%s notsaved' % (list_url[i], r))
                else:
                    list_saved.append(list_url[i])
                    #logger.error('%s\t%s havesaved' % (list_url[i], len(r)))
            return list_saved, list_notsaved
        else:
            return [], []
    def process(self, pattern_url='http://www.120ask.com/question/%s.htm', start=10000000, end=50000000):
        try:
            i = start
            while i <= end:
                list_url = [pattern_url % j  for j in range(i, i+100)]
                list_remain = self.notsaved(list_url)[1]
                dict_res = craw(list_remain)
                self.save(dict_res)
                #time.sleep(1)
                i += 100
                logger.error('Crawler %s %s %s' % (list_url[0], len(dict_res), redis_one.hlen(self.prefix)))
        except:
            logger.error(traceback.format_exc())
            
    def get_content(self, list_url=[]):
        if not list_url: return []
        list_res = redis_one.hmget(self.prefix, list_url)
        list_content = []
        for r in list_res:
            if r:
                list_content.append(YhCompress.decompress(r))
        return list_content
        
class Parser():
    def __init__(self, company='120ask'):
        self.company = company
        self.sitemap_prefix= 'sitemap:%s' % self.company
        self.sitemap_parsed_prefix= 'sitemap:parsed:%s' % self.company
        self.content_prefix = 'content:%s' % self.company
        
    def process(self, start_url='http://www.120ask.com/list/all/', start=0, end=2538777):
        try:
            i = start
            while i <= end:
                list_url = ['%s/%s/' % (start_url, j) for j in range(i, i+100)]
                list_remain = Crawler().notsaved(list_url)[0]
                list_remain = self.notparsed(list_remain)[1]
                set_contenturl = self.parse_contenturl(list_remain)
                dict_url = {}
                for u in set_contenturl:
                    dict_url[u] = 1
                self.save_contenturl(dict_url)
                if len(dict_url) == 0:
                    time.sleep(1)
                i+=100
                logger.error('Parser %s %s %s' %  (list_url[0], len(dict_url), redis_one.hlen(self.content_prefix)))
        except:
            logger.error(traceback.format_exc())
            
    def notparsed(self, list_url=[]):
        logger.error('notparsed len %s' % len(list_url))
        if list_url:
            list_res = redis_one.hmget(self.sitemap_parsed_prefix, list_url)
            list_yes, list_no = [], []
            for i, r in enumerate(list_res):
                if not r:
                    list_no.append(list_url[i])
                else:
                    list_yes.append(YhCompress.decompress(r))
            return list_yes, list_no
        else:
            return [], []
    def parse_contenturl(self, list_url=[],  url_prefix='http://www.ask.com', pattern_url = 'http://www.120ask.com/question'):
        if not list_url: return set()
        try:
            list_content = Crawler().get_content(list_url)
            set_suburl = set()
            for page in list_content:
                list_u = re.findall('<a.*?href=\"(.*?)\".*?>', page)
                
                for u in list_u:
                    if u[:4]!='http':
                        u = 'http://www.120ask.com' + u
                    if re.match(pattern_url, u):
                        set_suburl.add(u)
            return set_suburl
        except:
            logger.error(traceback.format_exc())
            return set()
    
    def save_contenturl(self, dict_url={}):
        try:
            if dict_url:
                redis_one.hmset(self.content_prefix, dict_url)
        except:
            logger.error('%s' % traceback.format_exc())


def test_crawler():
    c = Crawler()
    c.process()

def test_parser():
    sys.exit()
    p = Parser()
    p.process()
    
if __name__=='__main__':
    for f in [test_crawler, test_parser]:
        t = os.fork()
        if t > 0:
            continue
        else:
            f()
    sys.exit(0)
    