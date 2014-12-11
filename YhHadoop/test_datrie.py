# -*- coding: UTF-8 -*-
import tornado.gen, tornado.web
import sys, os, string
import re, traceback
import simplejson
from unipath import Path
import logging
import datrie
sys.path.insert(0, "../YhHadoop")

#3rd module
from collections import defaultdict


#logger = logging.getLogger(__name__)





class Datrie_Seg:
    def __init__(self, fn_domain=[]):
        pass
    def load(self, fn='./txt/dict_keyword.txt.sort'):
        logging.error('load')
        set_keyword = set()
        for l in open(fn):
                try:
                    l = unicode(l.strip(), 'utf8', 'ignore')
                    l = l.split()[0]
                    if not l: continue
                    set_keyword.add(l)
                except:
                    logging.error(traceback.format_exc())
        list_keyword = list(set_keyword)
        list_keyword.sort()
        logging.error('len keyword %s' % len(list_keyword))
        try:
            d = datrie.Trie(ranges=[(u'\u0000', u'\u9fff')])
            for l in list_keyword:
                d[l] = 1
                subl = l[:-1]
                logging.error('data len %s [%s] [%s] [%s]' % (len(d), l, d[l], len(d.keys(subl))))
        except:
            logging.error(traceback.format_exc())
if __name__=='__main__':
    Datrie_Seg().load()
    