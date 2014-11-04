# -*- coding: UTF-8 -*-
import sys, os
import subprocess
from datetime import datetime, timedelta
import smtplib
import logging.config, logging, logging.handlers
from unipath import Path
from functools import wraps
import errno, os, signal
import time

#self module
import YhLog
logger = logging.getLogger(__name__)

class TimeoutError(Exception):
    pass

def timeout(seconds=1, error_message=os.strerror(errno.ETIME)):
    def decorator(func):
        def _handle_timeout(signum, frame):
            raise TimeoutError(error_message)

        def wrapper(*args, **kwargs):
            signal.signal(signal.SIGALRM, _handle_timeout)
            signal.alarm(seconds)
            try:
                result = func(*args, **kwargs)
            finally:
                signal.alarm(0)
            return result
        return wraps(func)(wrapper)
    return decorator

@timeout()
def test():
    time.sleep(5)
    
    
if __name__=='__main__':
    test()