#!/usr/bin/env python
#coding:utf8

import sys, re, logging, redis,traceback, time, os, simplejson, subprocess
from multiprocessing import Pool, Queue, Lock, managers, Process
import copy
from collections import defaultdict
from bson.binary import Binary
import string
from unipath import Path
import cPickle
sys.path.append('../YhHadoop')

import YhLog, YhCompress, YhMongo, YhCrawler
import YhTrieSeg

logger = logging.getLogger(__file__)

redis_zero = redis.Redis(port=7777, unix_socket_path='/tmp/redis.sock')




   
    
class Segmenter(object):
    def __init__(self, company='120ask', db='urlcontent', ifn='../data/120ask.db', ofn='../data/120ask.idx'):
        self.cwd = Path(__file__).absolute().ancestor(1)
        self.company = company
        self.db = db
        self.ifn = ifn
        self.ofn = ofn
        self.yhTrieSeg = YhTrieSeg.YhTrieSeg([Path(self.cwd, '../data/tag_120ask.txt')])
    def run(self):
        list_file = Path(self.cwd, self.ifn).ancestor(1).listdir(pattern='120ask.db.part.*')
        logger.error('\n'.join(list_file))
        for f in list_file[:3]:
            dict_idx = defaultdict(set)
            for l in open(f):
                l = unicode(l.strip(), 'utf8', 'ignore')
                if not l: continue
                try:
                    id, url, title, description, content = l.split('\t')[:5]
                    list_s = self.yhTrieSeg.seg('%s\t%s' % (title, description))
                    for s in list_s:
                        dict_idx[s].add(int(id))
                except:
                    logger.error('error line %s' % l)
            ofn = re.sub(r'.db.', r'.idx.', f)
            cPickle.dump(dict_idx, open(ofn, 'w+'))
            logger.error('idx len %s ifn %s ofn %s' % (len(dict_idx), f, ofn))
                
    
if __name__=='__main__':
    p = os.fork()
    if p:
        sys.exit()
    else:
        Segmenter().run()