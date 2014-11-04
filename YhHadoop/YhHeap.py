# -*- coding: UTF-8 -*-
import sys, os
import subprocess
from datetime import datetime, timedelta
import smtplib
import logging
from unipath import Path
from functools import wraps
import errno, os, signal
import time
import heapq

#self module
import YhLog
logger = logging.getLogger(__name__)


class MyHeap(object):
   def __init__(self, initial=None, key=lambda x:x):
       self.key = key
       if initial:
           self._data = [(key(item), item) for item in initial]
           heapq.heapify(self._data)
       else:
           self._data = []

   def push(self, item):
       heapq.heappush(self._data, (self.key(item), item))

   def pop(self):
       return heapq.heappop(self._data)[1]
   
def test():
    list_test=list('abcde')
    heapq.heapify(list_test)
    logger.error(heapq.nlargest(3, list_test))
    list_b = list('bcdef')
    heapq.heapify(list_b)
    list_res = [i for i in heapq.merge(list_test, list_b)]
    logger.error(heapq.nlargest(3, list_res))
if __name__ == '__main__':
    test()
    