#!/usr/bin/env python
#coding:utf8

import eventlet
from eventlet.green import urllib2
import sys, re, logging, redis,traceback, time, os
from multiprocessing import Pool, Queue
from collections import defaultdict
#self module
sys.path.append('/data/CloudSE/YhHadoop')
import YhLog, YhCompress
import SitemapCrawler, PatternCrawler


logger = logging.getLogger(__name__)

redis_one = redis.Redis(port=7777, unix_socket_path='/tmp/redis.sock', db=1)


def parse_content(list_url = []):
    dict_res = defaultdict(int)
    try:
        list_content = PatternCrawler.Crawler().get_content(list_url)
        for page in list_content:
            if page:
                set_res = set()
                new_content = re.sub('\s+', ' ', page, flags=re.M|re.I)
                new_content = re.sub('<!--.*?-->', '', new_content, flags=re.M|re.I)
                new_content = re.sub('<style.*?>.*?</style>', '', new_content, flags=re.M|re.I)
                new_content = re.sub('<script.*?>.*?</script>', '', new_content, flags=re.M|re.I)
                content =  re.sub('<.*?>', '', new_content)
                if content:
                    set_res = set([s for s in re.split('[\s\[\]]+', content)if s])
                for s in set_res:
                    if re.match('\d+$', s):
                        continue
                    dict_res[s] += 1
    except:
        logger.error(traceback.format_exc())
    return dict_res
    
class Parser(object):
    def __init__(self, company='120ask'):
        self.company = company
        self.sitemap_prefix = 'urlcontent:%s' % self.company

    def process(self):
        
        try:
            urls = redis_one.hkeys(self.sitemap_prefix)
            ofh = open('test_urls.txt', 'w+')
            urls.sort()
            ofh.write(('\n'.join(urls)).encode('utf8', 'ignore'))
            logger.error('total urls len %s' % len(urls))
            dict_res = defaultdict(int)
            i = 0
            while i <= len(urls):
                pool = Pool(processes=15)
                q = Queue()
                dict_subres = defaultdict(int)
                list_urls = [urls[i + j * 10000:i+(j+1)*10000] for j in range(15)]
                #list_dict_res = list(pool.map_async(parse_content, list_urls))
                for d in pool.imap(parse_content, list_urls):
                    for k, v in d.iteritems():
                        dict_res[k] += v
                logger.error('Parser %s %s' % (len(list_urls), len(dict_res)))
                i += 10000 * 15
            sorted_dict_res = sorted(dict_res.iteritems(), key = lambda s: s[1], reverse=True)
            ofh = open('./test_sitemap_keywords', 'w+')
            ofh.write('\n'.join(['%s\t%s' % (k,v) for (k,v) in sorted_dict_res if v>=3]).encode('utf8', 'ignore'))
            ofh.close()
        except:
            logger.error(traceback.format_exc())
    
if __name__=='__main__':
    Parser().process()