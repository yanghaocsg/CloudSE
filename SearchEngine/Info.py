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
import YhLog, YhMongo, YhTool, YhCompress
redis_zero = redis.Redis(port=7777, unix_socket_path='/tmp/redis.sock', db=0)
redis_187 = redis.Redis(host='219.239.89.187', port=7777)
pipeline = redis_zero.pipeline()
pipeline_187 = redis_187.pipeline()

mongo = YhMongo.yhMongo.mongo_cli
logger = logging.getLogger(__file__)

class Info:

    def __init__(self, prefix='info:%s', company='120ask', db='content'):
        self.company = company
        self.prefix = prefix % self.company
        self.db = db
        
    def getInfoByUrl(self, list_url=[]):
        datas = mongo.db[self.db].find({'url':{'$in':list_url}})
        list_res = []
        for d in datas:
            logger.error('raw data len %s' % len(d['data']))
            dict_d = cPickle.loads(lz4.loads(d['data']))
            
            logger.error('dict len %s' % len(dict_d))
            #logger.error('%s|%s|%s|%s' % ([dict_d[t] for t in ['id', 'title', 'description', 'content']]))
            title, content, url = dict_d.get('title', ''), dict_d.get('content', ''), dict_d.get('url', '')
            buf = '%s\t%s\t%s' % (url, title, content[:200])
            buf_lz4 = lz4.dumps(buf.encode('utf8', 'ignore'))
            buf_yhc = YhCompress.compress(buf)
            logger.error('org %s lz4 %s yhc %s' % (len(buf), len(buf_lz4), len(buf_yhc)))
        #logger.error('\n'.join([str(r) for r in list_res])) 
        return list_res
    
    def getInfoById(self, list_id=range(100)):
        list_buf = redis_187.hmget(self.prefix, list_id)
        list_res = []
        for id, l in zip(list_id, list_buf):
            try:
                if l:
                    url, title, content = unicode(l, 'utf8', 'ignore').split('\t')[:3]
                    dict_l = {'id':id, 'url':url, 'title':title, 'content':content}
                    list_res.append(dict_l)
            except:
                logger.error(traceback.format_exc())
        return list_res

class InfoBatch:
    def __init__(self, prefix='info:%s', company='120ask', db='content'):
        self.company = company
        self.prefix = prefix % self.company
        self.db = db

    def batchTransfer(self, list_url=[], re_brief = u'有问必答网'):
        datas = mongo.db[self.db].find({})
        num_p = 0
        start_pos = 0
        for d in datas:
            try:
                
                dict_d = cPickle.loads(lz4.loads(d['data']))
                title, content, url, id = dict_d.get('title', ''), dict_d.get('content', ''), dict_d.get('url', ''), dict_d.get('id', '')
                start_pos = 0
                re_t =  re.search(re_brief, content, re.U|re.L|re.M|re.DEBUG)
                if re_t: 
                    start_pos = re_t.start()
                #buf = '[%s]\t[%s]\t[%s]\t[%s]\t[%s]\t[%s]\t[%s]' % (url, start_pos, title, type(re_brief), type(content), content[start_pos : start_pos + 200], content[:400])
                buf = '%s\t%s\t%s' % (url, title, content[start_pos:start_pos+200])
                if id and redis_187.hget(self.prefix, id):
                    continue
                #logger.error(buf)
                if id and title:
                    pipeline_187.hset(self.prefix, id, buf)
                    num_p += 1
                if num_p % 10000 == 1:
                    logger.error('batchTransfer %s %s' % (num_p, buf))
                    pipeline_187.execute()
                
            except:
                logger.error('batchTransfer %s' % traceback.format_exc())
        pipeline_187.execute()
        logger.error('batchTransferTotal %s' % num_p) 
        
        
if __name__=='__main__':
    Info().getInfoById()
    '''
    t = os.fork()
    if t == 0:
        InfoBatch().batchTransfer()
    else:
        sys.exit(0)
    '''