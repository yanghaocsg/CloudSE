#!/usr/bin/env python
#coding:utf8

import eventlet
from eventlet.green import urllib2
import sys, re, logging, redis,traceback


#self module
sys.path.append('/data/CloudSE/YhHadoop')
import YhLog


logger = logging.getLogger(__name__)

def process(start_url='http://www.120ask.com/list/all/', start=0, end=2538777):
    pass
    
def httpget(url=''):
    with eventlet.timeout.Timeout(3, False):
        data = urllib2.urlopen(url).read()
    return url, data
    
def craw(list_url=[]):
    pool = eventlet.greenpool.GreenPool(30)
    dict_res = {}
    for u, d in pool.imap(httpget, list_url):
        #(url, data) = pool.spawn(httpget, u)
        #pool.spawn(httpget, u)
        if d:
            dict_res[u] = d
            logger.error('%s\t%s' % (u, len(d)))
    return dict_res

def save(dict_res={}, hash_prefix='120ask', str_prefix='org|%s'):
    redis_one = redis.Redis(port=7777, unix_socket_path='/tmp/redis.sock', db=1)
    for u,v in dict_res.iteritems():
        redis_one.hset(hash_prefix, str_prefix % u, v)
    logger.error('save %s' % len(dict_res))

def parsePage(url='', page='', url_prefix='http://www.ask.com', pattern_url = 'http://www.120ask.com/question'):
    try:
        title, keywords, description, content = '', '', '', ''
        re_m = re.search('<title>(.*?)</title>', page)
        if re_m:
            title = re_m.group(1)
        re_keywords = re.search(r'<meta name="keywords" content="(.*?)" />', page)
        if re_keywords:
            keywords = re_keywords.group(1)
        re_description = re.search(r'<meta name="description" content="(.*?)" />', page)
        if re_description:
            description = re_description.group(1)
        new_content = re.sub('\s+', ' ', page, flags=re.M|re.I)
        new_content = re.sub('<!--.*?-->', '', new_content, flags=re.M|re.I)
        new_content = re.sub('<style.*?>.*?</style>', '', new_content, flags=re.M|re.I)
        new_content = re.sub('<script.*?>.*?</script>', '', new_content, flags=re.M|re.I)
        content =  re.sub('<.*?>', '', new_content)
        list_url = re.findall('<a.*?href=\"(.*?)\".*?>', page)
        
        set_suburl = set()
        for uu in list_url:
            if uu[:4]!='http':
                uu = 'http://www.120ask.com' + uu
            if re.match(pattern_url, uu):
                set_suburl.add(uu)
        dict_res= {'title':title, 'keywords':keywords, 'description':description, 'content':content}
        logger.error('\n'.join(dict_res.values()))
        return dict_res, set_suburl
    except:
        logger.error(traceback.format_exc())
        return {}, set()
        
def parse(dict_res={}, hash_prefix='120ask', pattern_url=pattern_url):
    redis_one = redis.Redis(port=7777, unix_socket_path='/tmp/redis.sock', db=1)
    dict_res = redis_one.hgetall(hash_prefix)
    dict_content = {}
    set_url = set()
    for u in dict_res:
        logger.error('%s\t%s' % (u, len(dict_res[u])))
        content = dict_res[u]
        
        logger.error(set_suburl)
        break

def test_craw():
    list_url = []
    for i in range(10):
        u = 'http://www.120ask.com/list/all/%s/' % i
        list_url.append(u)
    dict_res = craw(list_url)
    save(dict_res)

def test_parse():
    parse()
    
if __name__=='__main__':
    #test_craw()
    test_parse()
    