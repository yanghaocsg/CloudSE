# -*- coding: UTF-8 -*-
import tornado.gen, tornado.web
import sys, os
import re, traceback
import simplejson
from unipath import Path
import logging

#3rd module
from collections import defaultdict
import pickle
#self module
#, YhDecorator
import YhLog, YhTool, YhChineseNorm
logger = logging.getLogger(__name__)



def get_md5(s):
    m = hashlib.md5()
    m.update(s)
    return m.hexdigest()
    
def load_dict(fn='test'):
    logger.debug('load_dict %s' % fn)
    dict_tmp = defaultdict(int)
    try:
        for l in open(Path(Path(__file__).parent, fn)):
            try:
                l = unicode(l.strip(), 'utf-8', 'ignore')
                pars = l.split('\t')
                if(pars[0][-1] in u'第与一二三四'): continue
                if(pars[0][0] in u'的'): continue
                dict_tmp[pars[0]] = 1
            except:
                logger.error('load_dict line error [%s][%s]' % (l, traceback.format_exc()))
    except:
        pass
    logger.error('load_dict [%s]' % len(dict_tmp))
    return dict_tmp

class Trie:
    def __init__(self):
        self.dict_all = {}
        self.dict_syn = {}
        self.dict_stop = {}
        self.dict_clue = {}
        self.dict_redirect = {}
        self.dict_entity = {}
        self.max_len = 0
    def load(self, fn='./txt/dict_keyword.txt', synfn='./txt/dict_synwords.txt', stopfn='./txt/dict_stoplist.txt', cluefn='./txt/dict_cluelist.txt', 
            redirectfn='./txt/dict_redirect.txt', entityfn='./txt/dict_entity.txt'):
        
        (self.dict_all, self.dict_syn, self.dict_stop, self.dict_clue, self.dict_redirect, self.dict_entity) = [load_dict(f) for f in [fn, synfn, stopfn, cluefn, redirectfn, entityfn]]
        logger.error('trie load finished')
        
    def right_match(self, query, max_len=4):
        try:
            query = query.lower()
            
            #dict clue 
            list_res = []
            end = len(query)
            while(end >0):
                for i in range(end - max_len, end):
                    #logger.error(query[i:end])
                    if(query[i:end] in self.dict_all):
                        break
                #logger.error('%s\t%s' % (i, end))
                s = query[i:end]
                end = i
                list_res.append(s)
            #logger.debug('norm split %s' % ('|'.join(list_res)))
            num_oneword= 0 
            for s in list_res:
                if len(s) == 1: num_oneword += 1
            list_res.reverse()
            return list_res, num_oneword    
        except:
            logger.error('%s' % traceback.format_exc())
            return [query], 0
    
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
    def left_match(self, query, max_len=4):
        try:
            query = query.lower()
            
            #dict clue 
            list_res = []
            start = 0
            while(start < len(query)):
                end = start+1
                for i in range(start+max_len, end, -1):
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
 
    def seg(self, query, max_len=5):
        try:
            if max_len == 0: 
                max_len = len(query)
            logger.debug('query %s max_len %s' % (query, max_len))
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
                    list_left, unigram_left = self.left_match(p, max_len)
                    if list_left:
                        list_res.extend(list_left)
                    '''
                    list_right, unigram_right = self.right_match(p, max_len)
                    if(unigram_left <= unigram_right):
                        logger.debug('left matched [%s][%s]' % ('|'.join(list_left), '|'.join(list_right)))
                        list_res.extend(list_left)
                    else:
                        logger.debug('right matched [%s][%s]' % ('|'.join(list_left), '|'.join(list_right)))
                        list_res.extend(list_right)
                        '''
            #logger.error('norm split %s' % ('|'.join(list_res)))
            
            '''
            #dict clue 
            list_clue = []
            if(len(list_res) >= 2):
                i = 0
                while i < len(list_res):
                    if ''.join(list_res[i:i+3]) in self.dict_clue:
                        list_clue.append(''.join(list_res[i:i+3]))
                        i += 3
                    elif ''.join(list_res[i:i+2]) in self.dict_clue:
                        list_clue.append(''.join(list_res[i:i+2]))
                        i += 2
                    else:
                        list_clue.append(list_res[i])
                        i += 1
            else:
                list_clue=list_res
            logger.debug('clue %s' % ('|'.join(list_clue)))
            '''
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
    
trie = Trie()
trie.load()
    
def test(kw=u'周杰伦演唱会2013'):
    list_res = trie.seg(kw, len(kw))
    logger.error( 'seg res:%s' % '|'.join(list_res))
    max_len = max([len(l) - 1 for l in list_res]) 
    list_minres = trie.seg(kw,  max_len)
    logger.error( 'seg min res:%s' % '|'.join(list_minres))

def test_match(kw=u'北京旅游', max_len=3):
    trie = Trie()
    trie.load()
    list_res, num_unigram = trie.right_match(kw, max_len)
    logger.error('|'.join(list_res))
    list_res, num_unigram = trie.left_match(kw, max_len)
    logger.error('|'.join(list_res))
    
def test_file(ifn='./txt/music_query.txt', ofn='./txt/seg_music_query.txt'):
    trie = Trie()
    trie.load()
    ofh = open(ofn, 'w+')
    for line in open(ifn):
        line = unicode(line.strip(), 'utf-8', 'ignore')
        if not line: continue
        list_res = trie.seg(line)
        ofh.write('%s\t%s\n' % (line.encode('utf-8', 'ignore'), '\t'.join([t.encode('utf-8', 'ignore') for t in list_res if t])))
        if list_res:
            max_len = max([len(l) - 1 for l in list_res]) 
            list_res = trie.seg(line,  max_len)
            ofh.write('%s\t%s\n' % (l.encode('utf-8', 'ignore'), '\t'.join([t.encode('utf-8', 'ignore') for t in list_res if t])))

if __name__=='__main__':
    kw=u'北京游记'
    #test(kw)
    #test_match(kw)
    test_file()
    