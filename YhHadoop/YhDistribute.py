# -*- coding: UTF-8 -*-
import sys, os, operator
import subprocess
from datetime import datetime, timedelta
import smtplib
import logging.config, logging, logging.handlers
from unipath import Path
import traceback
import argparse

#self module
import YhLog

logger = logging.getLogger(__file__)

        

def distribute(ifn=[]):
    logger.error(ifn)
    list_server = []
    list_server.extend(['219.239.89.185','219.239.89.186', '219.239.89.188', '219.239.89.189'])
    dest_path = Path(__file__).absolute().ancestor(1)
    for f in ifn:
        for s in list_server:
            cmd_str = 'rsync -zPuvr %s %s:%s' % (Path(f).absolute(), s, Path(f).absolute())
            try:
                subprocess.check_call(cmd_str, shell=True)
                logger.error('distribute cmd_str [%s] succeed' % cmd_str)
            except:
                logger.error('distribute cmd_str [%s] failed %s' % (cmd_str, traceback.format_exc()))
                continue
                
if __name__=='__main__':
    
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--fn', nargs='*', default='', type=str)
    args = parser.parse_args()
    logger.error('parse arg %s' % args)
    distribute(args.fn)