#!/usr/bin/env python
#coding:utf8

import sys, re, logging, redis,traceback, time, os, simplejson
from multiprocessing import Pool, Queue
from collections import defaultdict
from bson.binary import Binary
import string
from unipath import Path

#self module
sys.path.append('/data/CloudSE/YhHadoop')
import YhLog, YhCompress, YhMongo, YhCrawler


logger = logging.getLogger(__name__)
mongo = YhMongo.yhMongo.mongo_cli

class TagBuilder(object):
    def __init__(self, company='120ask', db='tag'):
        self.cwd = Path(__file__).absolute().ancestor(1)
        self.company = company
        self.db = mongo[db]
        self.collection = self.db[company]
        self.collection.ensure_index([('tag', 1)], unique=True,  background=True, dropDups =True)
        self.ofh_tag = open(Path(self.cwd, 'tag_%s.txt' % company), 'w+')
    def process(self):
        try:
            datas = self.collection.find({})
            pass
        except:
            logger.error(traceback.format_exc())
    def parse(self, url_tag='http://tag.120ask.com/jibing/pinyin/%s.html', range_tag=string.ascii_lowercase, url_match=r'<a title=.*?href="/jibing/.*?/".*?>(.*?)</a>'):
        list_url = [url_tag %t for t in range_tag]
        dict_res = YhCrawler.craw(list_url)
        set_tag = set()
        for u, r in dict_res.iteritems():
            list_tag = re.findall(url_match, r)
            set_tag |= set(list_tag)
        for s in set_tag:
            try:
                self.collection.insert({'tag':s})
            except:
                logger.error(traceback.format_exc())
        logger.error('tag saved [%s] all[%s]' % (len(set_tag), self.collection.count()))
    def test(self):
        for i in range(10):
            self.db['test'].insert({'tag':str(i)})
        datas = self.db['test'].find({})
        for d in datas[:10]:
            logger.error(d)
    
    def dump(self):
        dict_tags = self.collection.find({})
        list_tags =[]
        for d in dict_tags:
            list_tags.append(d['tag'])
        self.ofh_tag.write('\n'.join(list_tags).encode('utf8', 'ignore'))
        self.ofh_tag.close()
        
if __name__=='__main__':
    #TagBuilder().test()
    #TagBuilder().parse()
    TagBuilder().dump()