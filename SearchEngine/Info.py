#!/usr/bin/env python
#coding:utf8
import sys, re, logging, redis,traceback, time, os, simplejson
from multiprocessing import Pool, Queue
from collections import defaultdict
from bson.binary import Binary
import string
from unipath import Path
import cPickle, lz4
from tornado.wsgi import WSGIContainer
from tornado.httpserver import HTTPServer
from tornado.ioloop import IOLoop
import tornado.gen, tornado.web

#self module
sys.path.append('../YhHadoop')
import YhLog, YhTool
import Redis_zero
logger = logging.getLogger(__file__)

class Info:
    def __init__(self, prefix='info:%s', company='120ask', db='content'):
        self.company = company
        self.prefix = prefix % self.company
    def getInfoById(self, list_id=range(100)):
        list_buf = Redis_zero.redis_zero.hmget(self.prefix, list_id)
        list_res = []
        for id, l in zip(list_id, list_buf):
            try:
                if l:
                    id, url, title, description, content = re.split('\t', unicode(l, 'utf8', 'ignore'), 4)
                    title = re.sub(u'_快速问医生_搜索更多专家答案_有问必答', '', title)
                    content = re.sub(u'有问必答网 → ', '', content)
                    #content = re.sub(u''
                    dict_l = {'id':id, 'url':url, 'title':title, 'content':description}
                    list_res.append(dict_l)
            except:
                logger.error(traceback.format_exc())
        return list_res



info = Info()        

if __name__=='__main__':
    info.getInfoById()