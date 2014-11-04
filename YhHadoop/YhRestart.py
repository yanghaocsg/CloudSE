# -*- coding: UTF-8 -*-
"""YhRestart.
Usage:
  YhRestart.py <operation> <name>... [-f <conf>...]
  YhRestart.py -h | --help
  YhRestart.py --version

Options:
  -h --help     Show this screen.
  --version     Show version.
  -f <conf>, --config <conf>.
"""

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

def kill(list_prog=[], list_conf=[]):
    logger.error('list_prog [%s][%s] list_conf [%s]' % (type(list_prog), list_prog, list_conf))
    for prog in list_prog:
        p = subprocess.Popen('ps -ef | grep %s' % prog, stdout=subprocess.PIPE, shell=True)
        out, err = p.communicate()
        logger.error('%s [%s]' % (prog, out))
        for line in out.splitlines():
            if 'ps -ef' in line or 'grep' in line or 'restart' in line:
                continue
            logger.error('line %s' % line)
            pars = re.split(r'\s+', line)
            pid = int(pars[1])
            logger.error('pid %s [%s][%s]' % (pid, prog, line))
            os.kill(pid, 9)
        logger.error('kill job ok [%s]' % prog)
    
def restart(list_prog='MS_Server.py', list_conf=[]):
    kill(list_prog)
    for prog in list_prog:
        if(prog[-2:] == 'py'):
            str_restart = 'python %s' % Path(Path(__file__).cwd(), prog).absolute()
        else:
            str_restart = 'nohup %s %s &' % (Path(Path(__file__).cwd(), prog), '\t'.join(list_conf))
        logger.error(str_restart)
        ret = subprocess.check_call(str_restart, shell=True)
        logger.error('%s restart ok %s' % (str_restart, ret))

if __name__=='__main__':
    print '__doc__ %s' % __doc__
    args = docopt(__doc__)
    print args
    print type(args['<name>'])
    if args['<operation>'] == 'kill':
        kill(list_prog=args['<name>'], list_conf=args['--config'])
    elif args['<operation>'] == 'restart':
        restart(list_prog=args['<name>'], list_conf=args['--config'])