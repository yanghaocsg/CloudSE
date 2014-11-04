# -*- coding: UTF-8 -*-

'''Naval Fate.

Usage:  naval_fate.py ship new <name>...
  naval_fate.py ship [<name>] move <x> <y> [--speed=<kn>]
  naval_fate.py ship shoot <x> <y>
  naval_fate.py mine (set|remove) <x> <y> [--moored|--drifting]
  naval_fate.py -h | --help
  naval_fate.py --version

Options:
  -h --help     Show this screen.
  --version     Show version.
  --speed=<kn>  Speed in knots [default: 10].
  --moored      Moored (anchored) mine.
  --drifting    Drifting mine.

'''
import sys, os, operator, re
import subprocess
from datetime import datetime, timedelta
import smtplib
import logging.config, logging, logging.handlers
from docopt import docopt

from unipath import Path

if __name__=='__main__':
    print __doc__
    args = docopt(__doc__)
    print args