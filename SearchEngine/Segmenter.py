#!/usr/bin/env python
#coding:utf8

import sys, re, logging, redis,traceback, time, os, simplejson
from multiprocessing import Pool, Queue
from collections import defaultdict
from bson.binary import Binary
import string
from unipath import Path


import YhLog, YhCompress, YhMongo, YhCrawler, YhTrieSeg


logger = logging.getLogger(__name__)
mongo = YhMongo.yhMongo.mongo_cli
cwd = Path(__file__).absolute().ancestor(1)
yhTrieSeg = YhTrieSeg.YhTrieSeg([Path(cwd, '../data/tag_120ask.txt')])

class Segmenter(object):
    def __init__(self, company='120ask', db='urlcontent'):
        self.cwd = Path(__file__).absolute().ancestor(1)
        self.company = company
        self.db = mongo[db]
        self.collection = self.db[company]
            
    def process(self):
        try:
            list_data = self.collection.find({},skip=0, limit=100)
            logger.error('\n'.join(list_data))
            while start < len_data:
                list_tmp = self.collecton.find({}).
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