#!/usr/bin/env python
#coding:utf8

import sys, re, logging, redis,traceback, time, os, simplejson
from multiprocessing import Pool, Queue
from collections import defaultdict
from bson.binary import Binary
import string
from unipath import Path
import lz4
#self module
sys.path.append('../YhHadoop')
import YhLog
import bitset

logger = logging.getLogger(__file__)

class YhBitset(bitset.bitset):
    def set_list(b, set_id=set()):
        if set_id:
            list_id = list(set_id)
            list_id.sort()
            for id in list_id:
                b.set(id)
            return 0
        return 1
        
        
        
def test():
    i = [1, 2,6,3, 100, 100000, 1000000000]
    yhBitset = YhBitset()
    yhBitset.set_list(set(i))
    
    logger.error('%s\t%s' % (yhBitset.length(), yhBitset.search()))
    list_id =  yhBitset.search()
    for id in list_id:
        logger.error('matched id %s %s' % (id, type(id)))
        
if __name__=='__main__':
    test()
    