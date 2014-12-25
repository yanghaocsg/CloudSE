# -*- coding: UTF-8 -*-
import sys, random,pprint, re, operator, os, gc
import httplib, urllib, urllib2, subprocess, urlparse
import logging.config, logging, logging.handlers
import ConfigParser
import hashlib,traceback
import memcache
from unipath import Path
import lz4

import YhLog, YhTool
logger = logging.getLogger(__name__)
logger.setLevel(logging.WARN)

def get_md5(s):
    if isinstance(s, unicode):
        s = s.encode('utf8', 'ignore')
    m = hashlib.md5()
    m.update(s)
    return m.hexdigest()
    
class YhMc:
    def __init__(self, list_ip=['localhost'], port='11211'):
        try:
            self.cwd = Path(__file__).absolute().ancestor(1)
            self.config = ConfigParser.ConfigParser()
            self.config.read(Path(self.cwd, './conf/redis.conf'))
            list_ip = self.config.get('memcache', 'ip').split(',')
            port = self.config.get('memcache', 'port')
        except:
            logger.error('mc error %s' % traceback.format_exc())
        self.mc = memcache.Client(['%s:%s' % (ip, port) for ip in list_ip])
        logger.error('mc config %s\t%s' % (list_ip, port))
        
            
    def add_cache(self, query, res):
        query_md5 = get_md5(query)
        self.mc.set(query_md5, res, 600)
        #logger.error('mc add cache [%s]' % query)
    def get_cache(self, query):
        query_md5 = get_md5(query)
        try:
            buf_comp = self.mc.get(query_md5)
            if buf_comp and buf_comp is not None:
                return buf_comp
        except:
            logger.error('mc not mached [%s][%s]' % (query, traceback.format_exc()))
        return ''
            
yhMc = YhMc()

def test():
    yhMc.add_cache(u'abc', 'abcde')
    logger.error(yhMc.get_cache(u'abc'))
    yhMc.add_cache('abcd', 'abcde')
    logger.error(yhMc.get_cache('abcd'))
    
if __name__=='__main__':
    test()