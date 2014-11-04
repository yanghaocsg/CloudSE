# -*- coding: UTF-8 -*-
import sys, random,pprint, re, operator, os, gc
import httplib, urllib, urllib2, subprocess, urlparse
import logging.config, logging, logging.handlers
import ConfigParser
import pymongo

#3rd module
from unipath import Path
import redis

#self module
import YhLog, YhTool

logger = logging.getLogger(__name__)
class YhMongo():
    def __init__(self):
        self.cwd = Path(__file__).absolute().ancestor(1)
        self.config = ConfigParser.ConfigParser()
        self.config.read(Path(self.cwd, './conf/redis.conf'))
        ip = self.config.get('mongo', 'ip')
        port = int(self.config.get('mongo', 'port'))
        self.mongo_cli = pymongo.MongoClient(['%s:%s' % (ip, port)], wtimeout=1000)
        logger.error('mongo cli %s\t%s' % (ip, port))
        
yhMongo = YhMongo()
def test():
    dbtest = yhMongo.mongo_cli.test
    for i in range(10):
        dbtest.test.save({'test':1, 'id':dbtest.test.count()})
    list_res = dbtest.test.find({'test':1})
    for r in list_res:
        logger.error(r)

if __name__== '__main__':
    test()
