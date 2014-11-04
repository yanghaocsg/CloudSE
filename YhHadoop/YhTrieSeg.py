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

import YhLog, YhTool, YhChineseNorm 
logger = logging.getLogger(__name__)





class YhTrieSeg:
    def __init__(self, fn_domain=[]):
        self.dict_keyword = {}
        self.dict_all = {}
        self.dict_syn = {}
        self.dict_stop = {}
        self.dict_clue = {}
        self.dict_redirect = {}
        self.dict_entity = {}
        self.fn_domain = fn_domain
        self.max_len = 0
        self.cwd = Path(__file__).absolute().ancestor(1)
        self.load()
        
    def load(self, fn='./txt/dict_keyword.txt', synfn='./txt/dict_synwords.txt', stopfn='./txt/dict_stoplist.txt', cluefn='./txt/dict_cluelist.txt', 
            redirectfn='./txt/dict_redirect.txt', entityfn='./txt/dict_entity.txt'):
        for f in self.fn_domain+[fn]:
            for k in open(Path(self.cwd, f)):
                k = unicode(k.strip(), 'utf8', 'ignore')
                if not k: continue
                list_k = k.split()
                if list_k and len(list_k[0])>1:
                    self.dict_all[list_k[0]] = 1
                    if len(list_k[0]) >= self.max_len:
                        self.max_len = len(list_k[0])
        logger.error('dict_all %s' % len(self.dict_all))

    def presuffix(self,  list_seg=[], ulist=u'的了地和'):
        set_add = set()
        set_ulist=set(ulist)
        for i, s in enumerate(list_seg):
            logger.error(s)
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
        logger.error('presuffix %s' % '|'.join(list_seg))
        return list_seg + list(set_add)
    
    def clue(self, list_seg=[], ulist= u'第一二三四五六七八九十零'):
        set_ulist = set(ulist)
        set_add = set()
        for i, s in enumerate(list_seg):
            logger.error(s)
            if i>=1:
                if s[0] in set_ulist and list_seg[i-1][-1] in set_ulist:
                    set_add.add(list_seg[i-1]+s)
        return list_seg + list(set_add)
                
    def right_match(self, query):
        try:
            query = query.lower()
            #dict clue 
            list_res = []
            end = len(query)
            
            while(end >=2):
                start = 0
                if query[start:end] in self.dict_all:
                    s = query[start:end]
                else:
                    for i in range(max(0, end - self.max_len), end):
                        logger.error(query[:end])
                        if(query[i:end] in self.dict_all):
                            start = i
                            break
                    logger.error('%s\t%s' % (start, end))
                    s = query[start:end]
                    end = start
                if s: list_res.append(s)
            logger.error('end %s|%s' % (end, query[:end]))
            if query[:end+1]:   list_res.append(query[:end])
            list_res.reverse()
            list_res = self.presuffix(list_res)
            list_res = self.clue(list_res)
            logger.error('right_match %s\n %s' % (query, '|'.join(list_res)))
            return list_res
        except:
            logger.error('%s' % traceback.format_exc())
            return [query]
    
    def merge_unigram(self, list_res, start = 0):
        #logger.error('merge_unigram %s %s' % ('|'.join(list_res), start))
        if(start >= len(list_res) -1): return list_res
        if(min([len(l) for l in list_res[start:]]) >= 2): return list_res
        
        list_merge = []
        for i in range(start, len(list_res)):
            if len(list_res[i]) == 1:
                if i > 0 and len(list_res[i-1])>2:
                    before = list_res[i-1][:-1]
                    now = ''.join((list_res[i-1][-1:], list_res[i]))
                    if(before in self.dict_all and now in self.dict_all):
                        list_merge.extend(list_res[:i-1])
                        list_merge.append(before)
                        list_merge.append(now)
                        list_merge.extend(list_res[i+1:])
                        return self.merge_unigram(list_merge, start = i+1)
                elif i < len(list_res)-1  and len(list_res[i+1]) > 2:
                    before = ''.join((list_res[i], list_res[i+1][0]))
                    now = ''.join((list_res[i+1][1:]))
                    if(before in self.dict_all and now in self.dict_all):
                        list_merge.extend(list_res[:i])
                        list_merge.append(before)
                        list_merge.append(now)
                        list_merge.extend(list_res[i+2:])
                        return self.merge_unigram(list_merge, start = i+1)
        return self.merge_unigram(list_res, start = i+1)

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
            query = query.lower()
            list_res = []
            #first redirect
            if query in self.dict_redirect:
                logger.debug('redirect [%s] to [%s]'%(s, self.dict_redirect[query]))
                return [self.dict_redirect[query]]
            
            #second norm split, max left match
            pars = YhChineseNorm.string2List(query)
            for p in pars:
                if(YhChineseNorm.is_alphabet(p) or YhChineseNorm.is_number(p) or len(p) <= 2):
                    list_res.append(p)
                else:
                    #list_left, unigram_left = self.left_match(p)
                    list_left, unigram_left = self.right_match(p)
                    if list_left:
                        list_res.extend(list_left)
            list_clue = list_res
            #todu syn
            list_syn = list_clue
            
            #stop
            list_stop = list_clue
            entity_find = 0
            for tmp in list_clue:
                if tmp in self.dict_entity: entity_find = 1
            if entity_find:
                list_stop = [tmp for tmp in list_clue if tmp and tmp not in self.dict_stop]
            logger.debug('seg %s [%s]' % (query, '|'.join(list_stop)))
            return list_stop
        except:
            logger.error('%s[%s]' % (query, traceback.format_exc()))
            return query

trie = YhTrieSeg()
    
def test_match(kw=u'北京旅游'):
    list_res, num_unigram = trie.right_match(kw)
    logger.error('|'.join(list_res))
    list_res, num_unigram = trie.left_match(kw)
    logger.error('|'.join(list_res))
    
def test_file(ifn='./txt/music_query.txt', ofn='./txt/seg_music_query.txt'):
    ofh = open(ofn, 'w+')
    for line in open(ifn):
        line = unicode(line.strip(), 'utf-8', 'ignore')
        if not line: continue
        line = YhChineseNorm.stringQ2B(line)
        list_res = trie.right_match(line)
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
    