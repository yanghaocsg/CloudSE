# -*- coding: UTF-8 -*-
import sys, os, re, urllib, urlparse
import subprocess, traceback
from datetime import datetime, timedelta
import smtplib
import logging

from unipath import Path
#self module
import YhLog, YhTool
'''
    YhHadoopMonitor for hdfs monitor & client monitor
'''


logger = logging.getLogger(__name__)
list_hostname = ['client10v.safe.zzbc.qihoo.net', 'client20v.safe.zzbc.qihoo.net']

def runcmd():
    try:
        str_cmd = 'hostname'
        p = subprocess.Popen(str_cmd, shell=True, stdout=subprocess.PIPE)
        host = (p.communicate()[0]).strip()
        logger.error('host [%s]' % host)
        subprocess.check_call('df -h > ./txt/%s_df.txt' % host, shell=True)
        subprocess.check_call('du -b /home/recommend/Production/* | sort -nr | head -n 20 > ./txt/%s_du.txt' % host, shell=True)
        subprocess.check_call('/home/work/software/hadoop/bin/hadoop fs -du /home/recommend/Production/ |  sort -nr | head -n 20 >./txt/%s_hdfs.txt' % host, shell=True)
        
    except:
        logger.error('%s' % traceback.format_exc())

def rsync():
    try:
        host2 = list_hostname[1]
        pwd = Path(__file__).absolute().ancestor(1)
        subprocess.check_call('rsync -zu %s:%s/txt/* ./txt' % (host2, pwd), shell=True)
    except:
        logger.error('%s' % traceback.format_exc())
        
def monitor():
    msg = []
    #df
    msg.append(u'客户端磁盘总体情况<table border=\"1\" width=\"600\" height=\"80\">')
    for host in list_hostname:
        file_df = './txt/%s_df.txt' % host
        list_l = [unicode(l.strip(), 'utf8', 'ignore') for l in open(file_df).readlines() if l.strip()]
        for l in list_l[:2]:
            msg.append('<tr><td>%s</td><td>%s</td></tr>' % (host, l))
            logger.error('%s\t%s' % (host,l))

    msg.append(u'</table>客户端大目录top10, 大于100G飘红<hr/><table border=\"1\" width=\"600\" height=\"80\">')
    for host in list_hostname:
        file_df = './txt/%s_du.txt' % host
        list_l = [unicode(l.strip(), 'utf8', 'ignore') for l in open(file_df).readlines() if l.strip()]
        for l in list_l[:10]:
            try:
                pars = l.split()
                f_size, f_name =pars[:2]
                f_size = int(f_size)/1000000000
                if f_size >1:
                    if f_size>100:
                        msg.append('<tr><td>%s</td><span style="color:red"><td>%sG</td><td>%s</td></span></tr>' % (host, f_size, f_name))
                    else:
                        msg.append('<tr><td>%s</td><td>%sG</td><td>%s</td></tr>' % (host, f_size, f_name))
                        logger.error('%s\t%sG\t%s' % (host, f_size, f_name))
            except:
                logger.error('%s\t%s' % (l, traceback.format_exc()))
    msg.append(u'</table>hdfs大目录top10， 大于1000G飘红<hr/><table border=\"1\" width=\"600\" height=\"80\">')
    for host in list_hostname[:1]:
        file_df = './txt/%s_hdfs.txt' % host
        list_l = [unicode(l.strip(), 'utf8', 'ignore') for l in open(file_df).readlines() if l.strip()]
        for l in list_l[:10]:
            try:
                pars = l.split()
                f_size, f_name =pars[:2]
                f_size = int(f_size)/1000000000
                if f_size >1:
                    if f_size>1000:
                         msg.append('<tr><span style="color:red"><td>%sG</td><td>%s</td></span></tr>' % (f_size, f_name))
                    else:
                        msg.append('<tr><td>%sG</td><td>%s</td></tr>' % (f_size, f_name))
                        logger.error('%sG\t%s' % (f_size, f_name))
            except:
                logger.error('%s\t%s' % (l, traceback.format_exc()))
    msg.append('</table>')
    msg_mail  = ('<meta http-equiv=\"Content-Type\" content=\"text/html; charset=utf-8\"><html>%s</html>' % ''.join(msg))
    toaddrs=['yanghao@360.cn', 'zhangjing-ps@360.cn',  'qikai@360.cn',  'liuyong-pd@360.cn',  'wukai-ps@360.cn',  'shaoshuai@360.cn',  'chenyuan@360.cn',  'caiheng@360.cn',  'wangzhilong@360.cn','denglong@360.cn', 'fengqiang@360.cn',  'renkejiang@360.cn']
    YhTool.sendmail('HadoopMonitor', msg=msg_mail, toaddrs=toaddrs)
    ofh = open('./txt/hadoopmonitor.txt', 'w+')
    ofh.write(('\n'.join(msg)).encode('utf8', 'ignore'))
    ofh.close()

    
if __name__=='__main__':
    runcmd()
    rsync()
    monitor()