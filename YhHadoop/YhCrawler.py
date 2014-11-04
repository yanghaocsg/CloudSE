#!/usr/bin/env python
#coding:utf8

import eventlet
from eventlet.green import urllib2
import sys, re, logging, redis,traceback, time
import multiprocessing, os

#self module
sys.path.append('/data/CloudSE/YhHadoop')
import YhLog, YhCompress


logger = logging.getLogger(__name__)

    
def httpget(url=''):
    data = ''
    with eventlet.timeout.Timeout(3, False):
        try:
            data = urllib2.urlopen(url).read()
            data = unicode(data, 'utf8', 'ignore')
        except:
            logger.error(traceback.format_exc())
    logger.error('httpget %s %s' % (url, len(data)))
    return url, data
    
def craw(list_url=[]):
    pool = eventlet.greenpool.GreenPool(30)
    dict_res = {}
    for u, d in pool.imap(httpget, list_url):
        if d:
            dict_res[u] = d
    return dict_res