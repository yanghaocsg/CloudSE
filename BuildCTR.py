#!/usr/env/bin python
#coding: utf8

import sys
from collections import defaultdict
import re

sys.path.append('./YhHadoop')
#self module
import YhLog
logger = YhLog.logger

def BuildCtrDict(ifn='./data/test_ctr.file', p_url=''):
    dict_ctr = defaultdict(int)
    dict_unigram = defaultdict(int)
    for l in open(ifn):
        l = unicode(l.strip(), 'utf8', 'ignore')
        if not l: continue
        query, url, s_freq, _, c_freq, _ = re.split('\t', l, 5)
        dict_unigram[url] += int(c_freq)
        ctr = int(c_freq) * 100 /int(s_freq)
        if int(c_freq)>=3 and ctr>=1:
            dict_ctr['%s|%s' % (query, url)] = ctr
    ofh_ctr = open(ifn+'.ctr', 'w+')
    ofh_unigram = open(ifn+'.uni', 'w+')
    ofh_ctr.write(('\n'.join(['%s\t%s' % (k, dict_ctr[k]) for k in dict_ctr]).encode('utf8', 'ignore')))
    ofh_unigram.write(('\n'.join(['%s\t%s' % (k, dict_unigram[k]) for k in dict_unigram]).encode('utf8', 'ignore')))
    ofh_ctr.close()
    ofh_unigram.close()
    
if __name__=='__main__':
    if len(sys.argv)>=2:
        BuildCtrDict(sys.argv[1])
    else:
        print 'demo BuildCtrDict ifn'
        BuildCtrDict()
