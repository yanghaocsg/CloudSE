# -*- coding: UTF-8 -*-
import sys, os
import subprocess, re
from datetime import datetime, timedelta
import smtplib
import logging
from unipath import Path
from collections import defaultdict

#self module
import TrieSeg, YhLog
logger = logging.getLogger(__name__)

class TermFreq(object):
    def run(self, buf=''):
        trie = TrieSeg.trie
        list_res = trie.seg(buf)
        dict_kw = defaultdict(int)
        for k in list_res:
            dict_kw[k] += 1
        sorted_kw = sorted(dict_kw.iteritems(), key=lambda x: x[1], reverse=True)
        logger.error('\n'.join(['[%s]%s' % (k, v) for (k,v) in sorted_kw]))
        
termFreq = TermFreq()

def test(ifn='./tmp.htm'):
    list_line = [unicode(l.strip(), 'utf8', 'ignore') for l in open(Path(ifn).absolute()).readlines() if l.strip()]
    list_line = [re.sub('<.*?>', '', l) for l in list_line]
    buf = '\n'.join(list_line)
    
    termFreq.run(buf)
    
if __name__=='__main__':
    if sys.argv[1]:
        test(sys.argv[1])
    else:
        test()