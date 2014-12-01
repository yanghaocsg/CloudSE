# -*- coding: UTF-8 -*-
import tornado.gen, tornado.web
import sys, os, string
import re, traceback, cPickle
import simplejson
from unipath import Path
import logging
import datrie
sys.path.insert(0, "../YhHadoop")

#3rd module
from collections import defaultdict

import YhLog, YhTool, YhChineseNorm 
logger = logging.getLogger(__name__)





class YhTrieSeg:
    def __init__(self, fn_domain=[], fn_pic='../data/trieseg.pic'):
        self.dict_keyword = {}
        self.dict_all = {}
        self.dict_syn = {}
        self.dict_stop = {}
        self.dict_clue = {}
        self.dict_redirect = {}
        self.dict_entity = {}
        self.fn_domain = fn_domain
        self.max_len = 6
        self.cwd = Path(__file__).absolute().ancestor(1)
        self.pickle(fn_pic)
    
    def pickle(self, fn_pic='../data/trieseg.pic'):
        try:
            self.dict_all = cPickle.load(open(fn_pic))
        except:
            logger.error('pickle error %s' % traceback.format_exc())
            self.load(fn_pic=fn_pic)
        logger.error('pickle dict_all %s' % len(self.dict_all))
        
    def load(self, fn='./txt/dict_keyword.txt', synfn='./txt/dict_synwords.txt', stopfn='./txt/dict_stoplist.txt', cluefn='./txt/dict_cluelist.txt', 
            redirectfn='./txt/dict_redirect.txt', entityfn='./txt/dict_entity.txt', fn_pic='../data/trieseg.pic'):
        for f in self.fn_domain+[fn]:
            for k in open(Path(self.cwd, f)):
                k = unicode(k.strip(), 'utf8', 'ignore')
                if not k: continue
                list_k = k.split()
                if list_k and len(list_k[0])>1:
                    self.dict_all[list_k[0]] = 1
                    #if len(list_k[0]) >= self.max_len:
                    #    self.max_len = len(list_k[0])
        for k in open(Path(self.cwd, stopfn)):
            k = unicode(k.strip(), 'utf8', 'ignore')
            if not k: continue
            del self.dict_all[k]
        cPickle.dump(self.dict_all, open(fn_pic, 'w+'))
        logger.error('load dict_all %s' % len(self.dict_all))

    def presuffix(self,  list_seg=[], ulist=u'的了地和'):
        set_add = set()
        set_ulist=set(ulist)
        for i, s in enumerate(list_seg):
            #logger.error(s)
            if len(s)>=2:
                if s[0] in set_ulist:
                    if len(s[1:])>=2 and s[1:] in self.dict_all:
                        set_add.add(s[1:])
                    if i < len(list_seg)-1:
                        new = s[1:]+list_seg[i+1]
                        if new in self.dict_all:
                            set_add.add(new)
                if s[-1] in set_ulist:
                    if len(s[:-1])>=2 and s[:-1] in self.dict_all:
                        set_add.add(s[:-1])
                    if i > 0:
                        new = list_seg[i-1]+s[:-1]
                        if new in self.dict_all:
                            set_add.add(new)
        #logger.error('presuffix %s' % '|'.join(list_seg))
        return list_seg + list(set_add)
    
    def clue(self, list_seg=[], ulist= u'第一二三四五六七八九十零1234567890年月日'):
        set_ulist = set(ulist)
        set_add = set()
        len_seg = len(list_seg)
        i = 0
        while i < len_seg:
            if list_seg[i] and list_seg[i][-1] in set_ulist:
                j = i+1
                while j < len_seg:
                    if  list_seg[j] and list_seg[j][0] in set_ulist:
                        j += 1
                    else:
                        break
                if j - i >=2:
                    set_add.add(''.join(list_seg[i:j]))
                i = j
            else:
                i += 1
        return list_seg + list(set_add)
                
    def right_match(self, query=''):
        try:
            #logger.error('query %s' % query)
            #dict clue 
            list_res = []
            end = len(query)
            
            while(end >= 2):
                start = end - 1
                for i in range(max(0, end - self.max_len), end):
                    #logger.error('subquery %s' % query[i:end])
                    if(query[i:end] in self.dict_all):
                        start = i
                        break
                s = query[start:end]
                if s in self.dict_all: 
                    list_res.append(s)
                elif YhChineseNorm.is_number_alphabet(s):
                    j = start
                    while j >= 0:
                        if YhChineseNorm.is_number_alphabet(query[j]):
                            j -= 1
                        else:
                            break
                    list_res.append(query[j+1:end])
                    start = j+1
                elif not YhChineseNorm.is_other(s):
                    list_res.append(s)
                end = start
                #logger.error('|'.join(list_res))
            if end > 0 and query[:end] and not YhChineseNorm.is_other(query):
                list_res.append(query[:end])
            
            list_res.reverse()
            list_res = self.presuffix(list_res)
            list_res = self.clue(list_res)
            logger.error('right_match %s\n %s' % (query, '|'.join(list_res)))
            return list_res
        except:
            logger.error('%s' % traceback.format_exc())
            return [query]
    
    

    def left_match(self, query):
        try:
            query = query.lower()
            
            #dict clue 
            list_res = []
            start = 0
            while(start < len(query)):
                end = start+1
                for i in range(start+self.max_len, end, -1):
                    logger.debug(query[start:i])
                    if(query[start : i] in self.dict_all):
                        end = i
                        break
                logger.debug('%s\t%s' % (start, i))
                s = query[start:end]
                start = end
                list_res.append(s)
            logger.debug('norm split %s' % ('|'.join(list_res)))
            num_oneword= 0
            
                
            for s in list_res:
                if len(s) == 1: num_oneword += 1
            list_res = self.merge_unigram(list_res)
            return list_res, num_oneword    
        except:
            logger.error('%s[%s]' % (query, traceback.format_exc()))
            return [query], 0
 
    def seg(self, query):
        try:
            line = YhChineseNorm.stringQ2B(query)
            list_res = self.right_match(line)
            return list_res
        except:
            logger.error('%s[%s]' % (query, traceback.format_exc()))
            return query


    
def test_match(kw=u'北京旅游'):
    list_res, num_unigram = trie.right_match(kw)
    logger.error('|'.join(list_res))
    list_res, num_unigram = trie.left_match(kw)
    logger.error('|'.join(list_res))
    
def test_file(ifn='./txt/music_query.txt', ofn='./txt/seg_music_query.txt'):
    trie = YhTrieSeg()
    ofh = open(ofn, 'w+')
    for line in open(ifn):
        line = unicode(line.strip(), 'utf-8', 'ignore')
        if not line: continue
        list_res = trie.seg(line)
        ofh.write('%s\n%s\n' % (line.encode('utf-8', 'ignore'), '\t'.join([t.encode('utf-8', 'ignore') for t in list_res if t])))
        raw_input()
        '''
        if list_res:
            max_len = max([len(l) - 1 for l in list_res]) 
            list_res = trie.seg(line)
            ofh.write('%s\t%s\n' % (l.encode('utf-8', 'ignore'), '\t'.join([t.encode('utf-8', 'ignore') for t in list_res if t])))
        '''
if __name__=='__main__':
    #test_match()
    test_file()
    