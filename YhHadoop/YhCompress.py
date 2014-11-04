# -*- coding: UTF-8 -*-
import sys, os
import subprocess, hashlib
from datetime import datetime, timedelta
import smtplib, zlib
import logging.config, logging, logging.handlers
import YhLog
try:
    import json
except:
    import simplejson as json
    
logger = logging.getLogger(__name__)


'''
    return comp from unicode string
'''
def compress(str_unicode=''):
    str_utf8 = str_unicode
    if(type(str_unicode) is unicode):
        str_utf8 = str_unicode.encode('utf-8', 'ignore')
    return  zlib.compress(str_utf8)

'''
    return unicode string from comp
'''
def decompress(str_comp=''):
    str_utf8 = str_comp
    if(type(str_comp) is unicode):
        str_utf8 = str_comp.encode('utf-8', 'ignore')
    return unicode(zlib.decompress(str_utf8), 'utf-8', 'ignore')

def test():
    logger.setLevel(logging.DEBUG)
    a = {'name':u'测试', 'val':u'返回值','error':'0'}
    str_a = json.dumps(a)
    logger.warn(str_a)
    comp_a = compress(str_a)
    decomp_a = decompress(comp_a)
    b = json.loads(decomp_a)
    logger.warn(str(b))

if __name__=='__main__':
    test()
    