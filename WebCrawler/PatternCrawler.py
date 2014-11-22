#!/usr/bin/env python
#coding:utf8

import eventlet
from eventlet.green import urllib2
import sys, re, logging, redis,traceback, time, datetime, cPickle
import multiprocessing, os, lz4

#self module
sys.path.append('/data/CloudSE/YhHadoop')
import YhLog


logger = logging.getLogger(__name__)

redis_one = redis.Redis(port=7777, unix_socket_path='/tmp/redis.sock', db=1)
    
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
    
def craw(list_url=[]):
    pool = eventlet.greenpool.GreenPool(30)
    dict_res = {}
    for u, d in pool.imap(httpget, list_url):
        if d:
            dict_res[u] = d
    return dict_res

class Crawler(object):
    def __init__(self, prefix='urlcontent:%s', company='120ask', db='rawcontent', pattern_url_id=r'/(\d+?).htm', pattern_title=r'<title>(.*?)</title>', pattern_description='<meta name="description"\s+content="(.*?)"', pattern_keyword='<meta name="keywords"\s+content="(.*?)"'):
        self.company = company
        self.prefix = prefix % self.company
        self.pattern_url_id = pattern_url_id
        self.pattern_title = pattern_title
        self.pattern_keyword = pattern_keyword
        self.pattern_description = pattern_description
        #redis_one.delete(self.prefix)
        
    def save(self, dict_res={}):
        pipeline = redis_one.pipeline()
        for u,v in dict_res.iteritems():
            title, keyword, description = self.get_keyword(v)
            content = self.get_content(v)
            id = self.get_id(u)
            dict_info = {'url':u, 'title':title, 'keyword':keyword, 'description':description, 'content':content, 'id':id}
            #logger.error('|'.join([dict_info[t] for t in ['url', 'title', 'keyword', 'description']]))
            pipeline.hset(self.prefix, u,  lz4.dumps(cPickle.dumps(dict_info)))
        pipeline.execute()
        #logger.error('crawler save %s' % redis_one.hlen(self.prefix))
        
    def notexist(self, list_url = []):
        list_remain= []
        if list_url:
            list_res = redis_one.hmget(self.prefix, list_url)
            for i, r in enumerate(list_res):
                if not r:
                    list_remain.append(list_url[i])
        return list_remain
        
    def process(self, pattern_url='http://www.120ask.com/question/%s.htm', start=0, end=10000000):
        try:
            i = start
            while i <= end:
                list_url = [pattern_url % j  for j in range(i, i+100)]
                list_url = self.notexist(list_url)
                dict_res = craw(list_url)
                self.save(dict_res)
                hour = datetime.datetime.now().hour
                logger.error(hour)
                if hour > 10:
                    time.sleep(1)
                i += 100
                logger.error('Crawler %s %s %s' % (list_url[0], len(dict_res), redis_one.hlen(self.prefix)))
        except:
            logger.error(traceback.format_exc())
            
    def get_keyword(self, webpage=''):
        title, keyword, description = '', '', ''
        try:
            re_title = re.search(self.pattern_title, webpage)
            if re_title: title = re_title.group(1)
            re_keyword = re.search(self.pattern_keyword, webpage)
            if re_keyword: keyword = re_keyword.group(1)
            re_description = re.search(self.pattern_description, webpage)
            if re_description: description = re_description.group(1)
        except:
            logger.error(traceback.format_exc())
        return title, keyword, description
    
    def get_content(self, page=''):
        content = ''
        try:
            set_res = set()
            new_content = re.sub('\s+', ' ', page, flags=re.M|re.I)
            new_content = re.sub('<!--.*?-->', '', new_content, flags=re.M|re.I)
            new_content = re.sub('<style.*?>.*?</style>', '', new_content, flags=re.M|re.I)
            new_content = re.sub('<script.*?>.*?</script>', '', new_content, flags=re.M|re.I)
            new_content =  re.sub('<.*?>', '', new_content, flags=re.M|re.I)
            new_content =  re.sub('&nbsp;|&gt;|&lt;', '', new_content, flags=re.M|re.I)
            content = new_content
        except:
            logger.error(traceback.format_exc())
        return content
    
    def get_id(self, url=''):
        re_url_id = re.search(self.pattern_url_id, url)
        id = int(re_url_id.group(1))
        return id
    
def test_crawler():
    c = Crawler()
    c.process()

if __name__=='__main__':
    t = os.fork()
    if t > 0:
        sys.exit(0)
    else:
        '''
        try:
            multiprocessing.Process(target=Crawler().process, kwargs={'start':0, 'end':100}).start()
        except:
            logger.error(traceback.format_exc())
        '''
        multiprocessing.Process(target=Crawler().process, kwargs={'start':0, 'end':1000*1000*5}).start()
        multiprocessing.Process(target=Crawler().process, kwargs={'start':1000*1000*15, 'end':1000 * 1000 * 20}).start()
        multiprocessing.Process(target=Crawler().process, kwargs={'start':1000*1000*25, 'end':1000 * 1000 * 30}).start()
        multiprocessing.Process(target=Crawler().process, kwargs={'start':1000*1000*35, 'end':1000 * 1000 * 40}).start()
        multiprocessing.Process(target=Crawler().process, kwargs={'start':1000*1000*45, 'end':1000 * 1000 * 50}).start()
        multiprocessing.Process(target=Crawler().process, kwargs={'start':1000*1000*55, 'end':1000 * 1000 * 60}).start()
