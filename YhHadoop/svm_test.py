# -*- coding: UTF-8 -*-
import sys, random,pprint, re, operator, os, gc
import httplib, urllib, urllib2, subprocess, urlparse
import logging
import ConfigParser
import traceback
from unipath import Path
from ctypes import *

#self module
import svm
import YhLog
logger = logging.getLogger(__name__)

class node(Structure):
    
    
def test():
    logger.error('%s' % svm.LINEAR)
    logger.error('%s' % svm.genFields('abcde','12345'))
    a = (node * (10))()
    logger.error('%s' % a({'b':10}))
    
if __name__=='__main__':
    test()