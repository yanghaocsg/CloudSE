# -*- coding: UTF-8 -*-
import sys, os, operator, re
import subprocess
from datetime import datetime, timedelta
import smtplib
import logging.config, logging, logging.handlers
from docopt import docopt

from unipath import Path

sys.path.insert(0, "../YhHadoop")
#self module
import YhLog
logger = logging.getLogger(__name__)
'''

def restart(str_process='MS_Server.py'):
    p = subprocess.Popen('ps -ef', stdout=subprocess.PIPE, shell=True)
    out, err = p.communicate()
    for line in out.splitlines():
        if str_process in line:
            pars = re.split(r'\s+', line)
            pid = int(pars[1])
            os.kill(pid, 9)
    logger.error('kill job ok')
    str_restart = '/data1/yanghao/software/python27/bin/python %s' % Path(Path(__file__).ancestor(1), str_process)
    logger.error(str_restart)
    ret = subprocess.check_call(str_restart, shell=True)
    logger.error('%s restart ok %s' % (str_restart, ret))
'''
'''Calculator using docopt
Usage:
  calc_docopt.py operation <num1> <num2>
  calc_docopt.py (-h | --help)

Arguments
  <operation> Math Operation
  <num1> First Number
  <num2> Second Number

Options:
  -h --help     Show this screen.

'''
def test(arg):
    logger.error(arg)
    
if __name__=='__main__':
    args = docopt(__doc__)
    print args