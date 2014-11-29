#!/usr/bin/env python
# coding:utf8
import eventlet, urllib
from eventlet.green import urllib2
from eventlet import Timeout
import sys, re, logging, redis,traceback, time, datetime, cPickle
import multiprocessing, os, lz4, simplejson
import ConfigParser
from unipath import Path
from collections import defaultdict

sys.path.append('../YhHadoop')
sys.path.append('../SearchEngine')
import YhLog, Indexer


logger = logging.getLogger(__file__)
redis_zero = redis.Redis(host='219.239.89.186', port=7777)
class AgentCrawler_360:
    def __init__(self, conf_file = './conf/crawler.conf'):
        self.cwd = Path(__file__).absolute().ancestor(1)
        self.config = ConfigParser.ConfigParser()
        self.config.read(Path(self.cwd, conf_file))
        self.list_agent = self.config.get('agent', 'url').split(',')
        self.len_agent = len(self.list_agent)
        self.url_se = 'http://www.so.com/s?src=srp&fr=360sou_newhome&q='
        self.url_sug = 'http://m.so.com/suggest/mso?src=mso&callback=suggest&kw='
        self.query_se_prefix = 'query_se'
        
    def httpget(self, list_url=[]):
        data = ''
        len_url = len(list_url)
        id_agent = 0
        dict_res = {}
        i = 0
        j = 0
        while i < len_url:
            list_suburl = list_url[i:i+5]
            str_suburl = simplejson.dumps(list_suburl)
            url = self.list_agent[j] % urllib.quote_plus(str_suburl)
            try:
                with Timeout(3):
                    data = urllib2.urlopen(url).read()
            except:
                logger.error(traceback.format_exc())
                pass
            
            if data:
                json_data = simplejson.loads(data)
                if json_data['status'] == 0:
                    dict_res.update(json_data['data'])
            j = (j+1) % self.len_agent
            i += 5
            logger.error('httpget %s\t%s' % (len(list_url), len(dict_res)))
        logger.error('httpget %s\t%s' % (len(list_url), len(dict_res)))
        return dict_res
    
    def craw_sug(self, list_query=[]):
        list_url = [self.url_sug + urllib.quote_plus(q.encode('utf8', 'ignore')) for q in list_query]
        dict_res = self.httpget(list_url)
        dict_sug = defaultdict(list)
        for k, v in dict_res.iteritems():
            re_sub = re.search('suggest\((.*?)\)$', v)
            if not re_sub: continue
            v = re_sub.group(1)
            #logger.error('%s\n%s' % (k, v))
            dict_v = simplejson.loads(v)
            if 'query' in dict_v['data']:
                query = dict_v['data']['query']
                dict_sug[query] = []
                if 'sug' in dict_v['data']:
                    for word in dict_v['data']['sug']:
                        dict_sug[query].append(word['word'])
                logger.error('%s\t%s' % (query, '|'.join(dict_sug[query])))
        logger.error('parse_sug %s' % len(dict_sug))
        return dict_sug
        
    def craw_se(self, list_query=[], site_phrase='site:www.120ask.com ', url_phrase='http://www.120ask.com/', id_phrase='/(\d+).htm'):
        dict_url = {}
        for q in list_query:
            logger.error('%s\t%s' % (q, type(q)))
            quote_k = urllib.quote_plus(('%s %s'% (site_phrase, q)).encode('utf8', 'ignore'))
            dict_url[q] = self.url_se + quote_k
        logger.error('craw_se len %s\n%s' % (len(dict_url), '\n'.join(dict_url.values()[:10])))
        dict_se = self.httpget(dict_url.values())
        dict_res = defaultdict(list)
        for k in dict_url:
            if dict_url[k] in dict_se:
                v = dict_se[dict_url[k]]
                list_v = re.findall('<a href="(%s.*?)"' % url_phrase, v)
                for v in list_v:
                    re_id =  re.search(id_phrase, v)
                    if re_id:
                        id = re_id.group(1)
                        dict_res[k].append(id)
        Indexer.Indexer().save_kv(dict_res)
        logger.error('craw_se %s' % len(dict_res))
        return dict_res
    
    def add_query(self, query=u''):
        if query:
            redis_zero.lpush(self.query_se_prefix, query)
        logger.error('add_query %s' % query)
        return 1
    
    def run(self):
        list_query = []
        while True:
            len_query = redis_zero.llen(self.query_se_prefix)
            if not len_query:
                time.sleep(10)
            else:
                for i in range(min(10,len_query)):
                    query = redis_zero.rpop(self.query_se_prefix)
                    if query:
                        list_query.append(unicode(query, 'utf8', 'ignore'))
                self.craw_se(list_query)
                list_query=[]   
    
agentCrawler_360 = AgentCrawler_360()    
if __name__=='__main__':
    pid = os.fork()
    if(pid >0):
        os._exit(0)
    dict_res = agentCrawler_360.run()