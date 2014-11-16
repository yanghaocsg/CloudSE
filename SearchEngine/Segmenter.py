#!/usr/bin/env python
#coding:utf8

import sys, re, logging, redis,traceback, time, os, simplejson, subprocess
from multiprocessing import Pool, Queue, Lock, managers, Process
import copy
from collections import defaultdict
from bson.binary import Binary
import string
from unipath import Path
import cPickle
sys.path.append('../YhHadoop')

import YhLog, YhCompress, YhMongo, YhCrawler
import YhTrieSeg

logger = logging.getLogger(__name__)
mongo = YhMongo.yhMongo.mongo_cli
cwd = Path(__file__).absolute().ancestor(1)
yhTrieSeg = YhTrieSeg.YhTrieSeg([Path(cwd, '../data/tag_120ask.txt')])
redis_one = redis.Redis(port=7777, unix_socket_path='/tmp/redis.sock', db=1)
redis_zero = redis.Redis(port=7777, unix_socket_path='/tmp/redis.sock', db=0)
def idx(ifn= '', ofn='', columns=['keyword', 'descrption'], pattern_url_id=''): 
    try:
        list_data = cPickle.load(open(ifn))
        dict_idx = defaultdict(set)
        for d in list_data:
            try:
                u, t = d['url'], d['content']
                buf = YhCompress.decompress(t)
                dict_buf = simplejson.loads(buf)
                list_cols = [dict_buf.get(t, '') for t in columns]
                url_id = u
                if pattern_url_id:
                    re_url_id = re.search(pattern_url_id, u)
                    url_id = re_url_id.group(1)
                for c in list_cols:
                    if not c: continue
                    list_s = yhTrieSeg.seg(c)
                    for s in list_s:
                        if s:
                            dict_idx[s].add(url_id)
                        
            except:
                logger.error(traceback.format_exc())
        ofh= open(ofn, 'w+')
        cPickle.dump(dict_idx, ofh)
        subprocess.check_call('rm -rf %s' % ifn, shell=True)
        logger.error('idx %s %s' % (ifn, ofn))
    except:
        logger.error(traceback.format_exc())



   
    
class Segmenter(object):
    def __init__(self, company='120ask', db='urlcontent'):
        self.cwd = Path(__file__).absolute().ancestor(1)
        self.company = company
        self.db = db
        self.ofn_key_idx = Path(self.cwd, '../data/%s_key.idx' % self.company)
        self.ofn_content_idx = Path(self.cwd, '../data/%s_content.idx' % self.company)
        self.step = 10000
        self.pattern_url_id = r'/(\d+?).htm'
        self.prefix_db = 'tmp:db:%s' % self.company
        redis_one.delete(self.prefix_db)
    
    @staticmethod
    def process():
        logger.error('begin process ' )
        dict_company = {'name':'120ask', 'db_urlcontent':'urlcontent', 'prefix_db': 'tmp:db:%s', 'ofn_db':'../data/%s.db.%s','ofn_key_idx':'../data/%s.key.idx', 'ofn_content_idx':'../data/%s.content.idx', 'pattern_url_id':r'/(\d+?).htm', 'step':10000}
        Process(target=dump, args=(dict_company, )).start()
        Process(target=parse, args=(dict_company, )).start()
        
def dump(dict_company = {}):
    logger.error('begin dump')

    try:
        step = dict_company.get('step')
        collection = dict_company.get('db_urlcontent')
        logger.error('collection %s' % collection)
        cursor = mongo.db[collection].find({})
        len_count = cursor.count()
        
        redis_prefix_db = dict_company.get('prefix_db') % dict_company.get('name')
        logger.error('db count %s, group %s redis %s' % (len_count, int(len_count)/step, redis_prefix_db))
        set_data = set()
        for i in range(int(len_count/step)):
            logger.error(i)
            list_data = []
            for j in range(step):
                data = next(cursor, None)
                if data: list_data.append(data)
                if isinstance(data, dict) and 'url' in data: set_data.add(data['url'])
            ofn = dict_company.get('ofn_db') % (dict_company.get('name'), i)
            if list_data:
                cPickle.dump(list_data, open(ofn, 'w+'))
                redis_one.rpush(redis_prefix_db, ofn)
            while redis_one.llen(redis_prefix_db) > 20:
                time.sleep(30)
            logger.error('dump %s %s %s' % (i, len(set_data), ofn))
    except:
        logger.error('dump %s' % traceback.format_exc())

def parse(dict_company={}):
    logger.error('begin parse')        
    subpool = Pool(10)
    try:
        #try three times
        try_num = 0
        redis_prefix_db = dict_company.get('prefix_db') % dict_company.get('name')
        while try_num < 3:
            if redis_one.exists(redis_prefix_db):
                try_num = 0
                ofn = redis_one.lpop(redis_prefix_db)
                if not ofn:
                    try_num += 1
                    time.sleep(30)
                    continue
                ofn_idx = re.sub(r'.db', r'.idx', ofn)
                subpool.apply_async(idx, args=(ofn, ofn_idx, ['keyword', 'descrption'], dict_company.get('pattern_url_id')))
                logger.error('parse %s %s' % (ofn, ofn_idx))
            else:
                try_num += 1
                logger.error('parse is null %s' % redis_prefix_db)
                time.sleep(30)
    except:
        logger.error('parse %s' % traceback.format_exc())
        
    def merge_idx(self):
        pass
            
if __name__=='__main__':
    p = os.fork()
    if p:
        sys.exit()
    else:
        Segmenter.process()