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

import YhLog, YhCompress, YhCrawler
import YhTrieSeg, YhBitset

logger = logging.getLogger(__file__)
    
class Segmenter(object):
    def __init__(self, company='120ask', db='urlcontent', ifn='../data/120ask.db', ofn='../data/120ask.idx'):
        self.cwd = Path(__file__).absolute().ancestor(1)
        self.company = company
        self.db = db
        self.ifn = ifn
        self.ofn = ofn
        self.yhTrieSeg = YhTrieSeg.YhTrieSeg([Path(self.cwd, '../data/tag_120ask.txt')])
    
    def run(self):
        self.run_title()
        #self.run_idx()
        
    def run_title(self):
        list_file = Path(self.cwd, self.ifn).ancestor(1).listdir(pattern='120ask.db.part.*')
        #logger.error('\n'.join(list_file))
        idx_file = 0
        len_list_file = len(list_file)
        while idx_file < len_list_file:
            list_subfile = list_file[idx_file:idx_file+10]
            list_process = []
            for f in list_subfile:
                p = Process(target=Segmenter.title, args=(f, self.yhTrieSeg))
                p.start()
                list_process.append(p)
            for p in list_process:
                p.join()
            idx_file += 10
            logger.error('finished %s total %s' % (idx_file, len(list_file)))
            
    def run_idx(self):
        list_file = Path(self.cwd, self.ifn).ancestor(1).listdir(pattern='120ask.db.part.*')
        #logger.error('\n'.join(list_file))
        idx_file = 0
        len_list_file = len(list_file)
        while idx_file < len_list_file:
            list_subfile = list_file[idx_file:idx_file+10]
            list_process = []
            for f in list_subfile:
                p = Process(target=Segmenter.idx, args=(f, self.yhTrieSeg))
                p.start()
                list_process.append(p)
            for p in list_process:
                p.join()
            idx_file += 10
            logger.error('finished %s total %s' % (idx_file, len(list_file)))
    
    @staticmethod
    def title(ifn=None, yhTrieSeg=None):
        dict_idx = defaultdict(set)
        for l in open(ifn):
            l = unicode(l.strip(), 'utf8', 'ignore')
            if not l: continue
            try:
                id, url, title, description, content = re.split('\t', l, 4)
                id = int(id)
                list_s = yhTrieSeg.seg('%s' % title)
                for s in list_s:
                    dict_idx[s].add(id)
            except:
                logger.error('error line %s' % l)
        if dict_idx:
            ofn = re.sub(r'.db.', r'.title.', ifn)
            cPickle.dump(dict_idx, open(ofn, 'w+'))
            logger.error('idx len %s ifn %s ofn %s' % (len(dict_idx), ifn, ofn))
        else:
            logger.error('file error %s' % ifn)
            subprocess.call('rm -rf %s' % ifn, shell=True)
            
    @staticmethod
    def idx(ifn=None, yhTrieSeg=None):
        dict_idx = defaultdict(set)
        for l in open(ifn):
            l = unicode(l.strip(), 'utf8', 'ignore')
            if not l: continue
            try:
                id, url, title, description, content = re.split('\t', l, 4)
                id = int(id)
                list_s = yhTrieSeg.seg('%s\t%s' % (title, description))
                for s in list_s:
                    dict_idx[s].add(id)
            except:
                logger.error('error line %s' % l)
        if dict_idx:
            ofn = re.sub(r'.db.', r'.idx.', ifn)
            cPickle.dump(dict_idx, open(ofn, 'w+'))
            logger.error('idx len %s ifn %s ofn %s' % (len(dict_idx), ifn, ofn))
        else:
            logger.error('file error %s' % ifn)
            subprocess.call('rm -rf %s' % ifn, shell=True)
            

if __name__=='__main__':
    p = os.fork()
    if p:
        sys.exit()
    else:
        Segmenter().run()