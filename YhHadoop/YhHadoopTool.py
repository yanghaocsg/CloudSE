# -*- coding: UTF-8 -*-
import sys, os, re, urllib, urlparse
import subprocess
from datetime import datetime, timedelta
import smtplib


def unify_url(url_to):
    try:
        url_to = re.sub(r'\\', r'', url_to)
        url_to = urllib.unquote_plus(url_to)
        url_to = urllib.unquote_plus(url_to)
        url_part = urlparse.urlparse(url_to)
        #print '[%s][%s][%s][%s][%s][%s][%s][%s][%s][%s]' % (url_part.scheme, url_part.netloc, url_part.path, url_part.params, url_part.query, \
        #                                url_part.fragment, url_part.username, url_part.password, url_part.hostname, url_part.port)
        index_amp = url_part.path.find('&')
        str_url = '%s://%s%s' % (url_part.scheme, url_part.netloc, url_part.path)
        if(index_amp > 0):
            str_url = '%s://%s%s' % (url_part.scheme, url_part.netloc, url_part.path[:index_amp])
        if(not re.match('http:', str_url)):
            str_url = ''
        return str_url
    except:
        return url_to
        
def today(delta=0):
    return datetime.strftime(datetime.now() - timedelta(delta), "%Y%m%d")
    
def begin(ipath='', delta=10):
    cmdstr = "rm -rf %s/dat/done.file" %ipath
    print >>sys.stderr, cmdstr
    subprocess.call(cmdstr, shell=True)
    cmdstr = "rm -rf %s/dat/%s*" %(ipath, today(10))
    print >>sys.stderr, cmdstr
    subprocess.call(cmdstr, shell=True)
    def check_and_copy(ifn='', ofn='', lnum=1000):
    num = 0
    for l in open(ifn):
        num += 1
        if(num >= lnum):
            break
    if(num < lnum):
        raise ValueError(ifn+'\t'+str(lnum) +'Error')
    cmdstr = 'cp %s %s\n' % (ifn, ofn)
    print >>sys.stderr, cmdstr
    subprocess.check_call(cmdstr, shell=True)
def check_and_md5(ifn='', lnum=1000):
    num = 0
    for l in open(ifn):
        num += 1
        if(num >= lnum):
            break
    if(num < lnum):
        raise ValueError(ifn+'\t'+str(lnum) +'Error')
    cmdstr = 'md5sum %s >%s.md5\n' % (ifn, ifn)
    print >>sys.stderr, cmdstr
    subprocess.check_call(cmdstr, shell=True)

def end(ipath='', today=datetime.now()):
    cmdstr = "touch %s/dat/done.file\n" %ipath
    print >>sys.stderr, cmdstr
    subprocess.check_call(cmdstr, shell=True)

def copy_withday(withday=today(0), ofn=''):
    cmdstr = "cp %s_%s %s " % (ofn, withday, ofn)
    subprocess.check_call(cmdstr, shell=True)
    
def sendmail(subject='', msg='', fromaddr='yanghao@360.cn', toaddrs=['yanghao@360.cn',]):
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart  

    mail_msg = MIMEMultipart()
    mail_msg['Subject'] = subject
    mail_msg['From'] =fromaddr
    mail_msg['To'] = ','.join(toaddrs)
    mail_msg.attach(MIMEText(msg, 'html', 'utf-8'))
    #print mail_msg.as_string()
    s = smtplib.SMTP('localhost')
    s.sendmail(fromaddr, toaddrs, mail_msg.as_string())
    s.quit()
    
def unique_list(seq):
    seen = set()
    seen_add = seen.add
    return [ x for x in seq if x not in seen and not seen_add(x)]

def dump_dict(dict_d={}, str_d='', min= 0):
    if(str_d and len(dict_d)>0):
        ofh = open(str_d, 'w+')
        for (k,v) in dict_d.iteritems():
            if(type(k) is not unicode):
                k = unicode(k, 'utf-8', 'ignore')
            ofh.write('%s\t%d\n' %(k.encode('utf-8', 'igore'), v))
        ofh.close()
        
def load_unigram_dict(ifn=''):
    dict_unigram = {}
    for l in open(ifn):
        l = unicode(l.strip(), 'utf-8', 'ignore')
        pars = l.split('\t')
        freq = 0
        if(len(pars) < 2):
            continue
        try:
            freq = int(pars[1])
        except:
            logger.error('error line[%s]' % l)
            continue
        dict_unigram[pars[0]] = freq
    return dict_unigram

def dump_bigram_dict(dict_d={}, str_d=''):
    if(str_d and len(dict_d)>0):
        ofh= open(str_d, 'w+')
        for k in dict_d:
            dict_v = dict_d[k]
            list_v = sorted(dict_v.iteritems(), key=operator.itemgetter(1), reverse=True)
            for(rk, rv) in list_v[:100]:
                ofh.write('%s\t%s\t%d\n' % (k.encode('utf-8','ignore'), rk.encode('utf-8', 'ignore'), rv))
    return 

def load_bigram_dict(ifn=''):
    dict_bigram = {}
    for l in open(ifn):
        l = unicode(l.strip(), 'utf-8', 'ignore')
        pars = l.split('\t')
        freq = 0
        if(len(pars) < 3):
            continue
        try:
            freq = int(pars[2])
        except:
            continue
        (a, b) = (pars[0], pars[1])
        dict_bigram.setdefault(a, {})
        dict_bigram[a][b] = freq
    return dict_bigram
if __name__=='__main__':
    str_url = 'http://v.ifeng.com/vblog/dv/201303/500c09b2-5d86-49af-b1bf-25ecdbc17ee8.shtml&pos=1'
    print unify_url(str_url)