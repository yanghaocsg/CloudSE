#!/usr/bin/env python
# coding:utf8
import eventlet, urllib
from eventlet.green import urllib2
import sys, re, logging, redis,traceback, time, datetime, cPickle
import multiprocessing, os, lz4, simplejson
import ConfigParser
from unipath import Path
from collections import defaultdict
sys.path.append('../YhHadoop')

import YhLog


logger = logging.getLogger(__file__)

class Test_AgentCrawler:
    def __init__(self, conf_file = './conf/crawler.conf'):
        self.cwd = Path(__file__).absolute().ancestor(1)
        self.config = ConfigParser.ConfigParser()
        self.config.read(Path(self.cwd, conf_file))
        self.list_agent = self.config.get('agent', 'url').split(',')
        self.len_agent = len(self.list_agent)
        self.url_se_360 = 'http://www.so.com/s?src=srp&fr=360sou_newhome&q='
        self.url_sug_360 = 'http://m.so.com/suggest/mso?src=mso&callback=suggest&kw='
        self.url_se_sogou = 'http://www.sogou.com/web?&_asf=www.sogou.com&_ast=&w=01019900&p=40040100&ie=utf8&sut=1171&sst0=1416744428350&lkt=0%2C0%2C0&query=%s+site%3Awww.120ask.com'
        self.url_sug_sogou = 'http://w.sugg.sogou.com/sugg/ajaj_json.jsp?key=%s&type=web&ori=yes&pr=web&abtestid=1&ipn=false'
        
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
            logger.error('url %s' % url)
            try:
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
    
    def craw_sug(self, ifn='tag_120ask.txt', url_sug=''):
        list_q = [unicode(l.strip(), 'utf8', 'ignore') for l in open(ifn) if l.strip()]
        logger.error('test_fn len %s' % len(list_q))
        if not url_sug:
            url_sug = self.url_sug_360
        list_url = [url_sug + urllib.quote_plus(q.encode('utf8', 'ignore')) for q in list_q]
        dict_res = Test_AgentCrawler().httpget(list_url[:20])
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
        ofh = open('%s.sug.pic' % ifn, 'w+')
        cPickle.dump(dict_sug, ofh)
        logger.error('parse_sug %s' % len(dict_sug))
        
    def craw_se(self, pic_sug='tag_120ask.txt.sug.pic', pic_se='tag_120ask.se.pic', site_phrase='site:www.120ask.com '):
        dict_sug = cPickle.load(open(pic_sug))
        dict_url = {}
        for k, v in dict_sug.iteritems():
            
            quote_k = urllib.quote_plus((site_phrase+ k).encode('utf8', 'ignore'))
            dict_url[k] = self.url_se_360 + quote_k
            for w in v:
                quote_w = urllib.quote_plus((site_phrase+ k).encode('utf8', 'ignore'))
                dict_url[w] = self.url_se_360 + quote_w
        logger.error('craw_se len %s\n%s' % (len(dict_url), '\n'.join(dict_url.values()[:10])))
        dict_se = self.httpget(dict_url.values())
        dict_res = defaultdict(list)
        for k in dict_url:
            if dict_url[k] in dict_se:
                v = dict_se[dict_url[k]]
                list_v = re.findall('<a href="(http://www.120ask.com/.*?)"', v)
                dict_res[k] = list_v
        ofh = open(pic_se, 'w+')
        cPickle.dump(dict_res, ofh)
        ofh.close()
        logger.error('craw_se %s' % len(dict_res))
        '''
        dict_test = cPickle.load(open(pic_se))
        for k in dict_test:
            for i, v in enumerate(dict_test[k]):
                logger.error('%s\t%s\t%s' % (k, i, v))

        '''
        
def test():
    list_url=[]
    for q in u'abc糖尿病efghijklmn':
        list_url.append('http://m.so.com/suggest/mso?kw=%s&src=mso&callback=suggest' % urllib.quote_plus(q.encode('utf8', 'ignore')))
    logger.error('\n'.join(list_url))
    dict_res = Test_AgentCrawler().httpget(list_url)
    logger.error('\n'.join(['%s\t%s' % (k,v) for (k, v) in dict_res.iteritems()]))

    
if __name__=='__main__':
    #test()
    #Test_AgentCrawler().craw_sug()
    Test_AgentCrawler().craw_se()
    sys.exit(0)
    t = os.fork()
    if t:
        sys.exit(0)
    else:
        #test_fn(sys.argv[1])
        Test_AgentCrawler().craw_sug()
        Test_AgentCrawler().craw_se()