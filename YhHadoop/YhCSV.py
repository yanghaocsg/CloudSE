# -*- coding: UTF-8 -*-
import sys, os, re, urllib, urlparse
import subprocess
from datetime import datetime, timedelta
import smtplib
import logging
import csv

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%m/%d/%Y-%I:%M:%S-%p', level=logging.ERROR)
logger = logging.getLogger(__file__)



def UnicodeDictReader(utf8_data, **kwargs):
    csv_reader = csv.DictReader(utf8_data, **kwargs)
    for row in csv_reader:
        #logger.error(row)
        yield dict([(key, unicode(value, 'utf-8')) for key, value in row.iteritems()])
<<<<<<< HEAD

=======
'''
def UnicodeDictReader(utf8_data, **kwargs):
    csv_reader = csv.DictReader(utf8_data, **kwargs)
    for row in csv_reader:
        #logger.error(row)
        yield dict([(key, unicode(value, 'utf-8')) for key, value in row.iteritems()])
'''
        
>>>>>>> 81e559041f01125699044a089316e6140d403291
if __name__=='__main__':
    list_res = list(UnicodeDictReader(open(sys.argv[1])))
    for r in list_res[:10]:
        logger.error('%s\t%s' % (r['name'], r['nick_name']))
    