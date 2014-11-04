# -*- coding: UTF-8 -*-
import sys, random,pprint, re, operator, os, gc
import httplib, urllib, urllib2, subprocess, urlparse
import logging.config, logging, logging.handlers
import ConfigParser,traceback

#3rd module
from unipath import Path
import redis

#self module
import YhLog, YhTool

logger = logging.getLogger(__name__)

class YhRedis:
    def __init__(self, ip='localhost', port=7777, password=''):
        self.cwd = Path(__file__).absolute().ancestor(1)
        self.config = ConfigParser.ConfigParser(defaults={'pass':'', 'sock':''})
        self.config.read(Path(self.cwd, './conf/redis.conf'))
        try:
            ip = self.config.get('redis', 'ip', vars={'ip':'localhost'})
            port = int(self.config.get('redis', 'port'))
            password = self.config.get('redis', 'pass')
            redis_sock = self.config.get('redis', 'sock')
        except:
            logger.error('%s' % traceback.format_exc())
        redis_type = ''
        if os.access(redis_sock, os.R_OK) and ('127.0.0.1' == ip or 'localhost' == ip):
            try:
                self.redis_zero = redis.Redis(unix_socket_path=redis_sock, password=password, db=0)
                self.redis_one = redis.Redis(unix_socket_path=redis_sock, password=password, db=1)
                self.redis_two = redis.Redis(unix_socket_path=redis_sock, password=password, db=2)
                self.r = self.redis_zero
                redis_type = 'unix_socket'
            except:
                logger.error('unix socket error[%s]' % traceback.format_exc())
                self.redis_zero = redis.Redis(host=ip, port=port, password=password, db=0)
                self.redis_one = redis.Redis(host=ip, port=port, password=password, db=0)
                self.redis_two = redis.Redis(host=ip, port=port, password=password, db=0)
                self.r = self.redis_zero
                redis_type = 'ip'
        else:
            self.r = redis.Redis(host=ip, port=port, password=password)
            redis_type = 'ip'
        logger.error('redis-type[%s] redis-conf[%s][%s][%s]' % (redis_type, ip, port, password))
        
    def get_handler(self):
        return self.r
        
yhRedis = YhRedis()