# -*- coding: UTF-8 -*-
import sys, os, re, urllib, urlparse
import subprocess
from datetime import datetime, timedelta
import smtplib
import logging


logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%m/%d/%Y-%I:%M:%S-%p', level=logging.ERROR)
logger = logging.getLogger(__file__)


def unify_url(url_to):
    url_to = re.sub(r'\\', r'', url_to)
    url_to = urllib.unquote_plus(url_to)
    url_to = urllib.unquote_plus(url_to)
    url_part = urlparse.urlparse(url_to)
    #print '[%s][%s][%s][%s][%s][%s][%s][%s][%s][%s]' % (url_part.scheme, url_part.netloc, url_part.path, url_part.params, url_part.query, \
    #                                url_part.fragment, url_part.username, url_part.password, url_part.hostname, url_part.port)
    url_path = url_part.path
    index_amp = url_path.rfind('&')
    if(index_amp > 0):
        url_path = url_path[:index_amp]
    index_unknown = url_part.path.rfind('?')
    if(index_unknown > 0):
        url_path = url_path[:index_unknonw]
    if(not url_part.scheme):
        return url_path
    str_url = '%s://%s%s' % (url_part.scheme, url_part.netloc, url_path)
    str_url = str_url.strip()
    if str_url and  str_url[-1] == '/':
        str_url = str_url[:-1]
        
    return str_url

def split_url(url='http://video.sina.com.cn/movie/zongyi/'):
    url = unify_url(url)
    if url[:7] == 'http://':
        url = url[7:]
    list_parts = re.split(r'/', url)
    list_parts = [l for l in list_parts if l]
    #logger.error('|'.join(list_parts))
    list_path = []
    if(list_parts):
        for i in range(1,len(list_parts)+1):
            list_path.append('/'.join(list_parts[:i]))
    #logger.error('|'.join(list_res))
    list_domain = []
    if list_parts:
        list_d = re.split(r'\.', list_parts[0])
        #logger.error('%s\t%s' % (list_parts[0], '|'.join(list_d)))
        len_list_d = len(list_d)
        if len_list_d >= 2:
            for i in range(2, len_list_d+1):
                list_domain.append('.'.join(list_d[len_list_d - i:]))
    list_path.reverse()
    list_path = ['http://%s' % p  for p in list_path]
    list_domain.reverse()
    return list_path, list_domain

def unify_list(seq):
    seen = set()
    seen_add = seen.add
    return [ x for x in seq if x not in seen and not seen_add(x)]
    
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
    def alarm(project='', msg=''):
    logger.error('[%s] JobFailed [%s]'%(project, msg))
    sendmail('[%s] JobFailed'%project, msg)

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
            freq = 1
        try:
            freq = int(pars[1])
        except:
            pass
        dict_unigram[pars[0]] = freq
    logger.error('unigram_dict[%s][%d]' % (ifn, len(dict_unigram)))
    return dict_unigram
    
def load_info_dict(ifn=''):
    dict_info = {}
    for l in open(ifn):
        l = unicode(l.strip(), 'utf-8', 'ignore')
        pars = l.split('\t')
        if(len(pars) < 2):
            continue
        dict_info[pars[0]] = l
    logger.error('info_dict[%s][%d]' % (ifn, len(dict_info)))
    return dict_info
    
def dump_bigram_dict(dict_d={}, str_d=''):
    if(str_d and len(dict_d)>0):
        ofh= open(str_d, 'w+')
        for k in dict_d:
            dict_v = dict_d[k]
            list_v = sorted(dict_v.iteritems(), key=operator.itemgetter(1), reverse=True)
            for(rk, rv) in list_v[:100]:
                ofh.write('%s\t%s\t%d\n' % (k.encode('utf-8','ignore'), rk.encode('utf-8', 'ignore'), rv))
    logger.error('bigram_dict[%s][%d]' % (str_d, len(dict_d)))
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
    logger.error('bigram_dict[%s][%d]' % (ifn, len(dict_bigram)))
    return dict_bigram

def del_log(d=3):
    list_day = [today(td) for td in range(d, d+7)]
    for td in list_day:
        cmdstr = "rm -rf *%s-%s-%s" % (td[:4], td[4:6],td[6:])
        logger.error(cmdstr)
        subprocess.check_call(cmdstr, shell=True)
        
def yh_urlparse(url='/seComp?query=%E4%B8%8B%E5%8D%88%E8%8C&s=0&song_3rd='):
    try:
        o =  urlparse.urlparse(url)
        dict_qs = urlparse.parse_qs(o.query)
        dict_res = {}
        for k in dict_qs:
            tmp = unicode(dict_qs[k][0], 'utf8', 'ignore')
            if tmp: dict_res[k] = tmp 
        return dict_res
    except:
        logger.error('yh_urlparse error [%s] [%s]' % (url, traceback.format_exc()))
        return {}

def yh_urlparse_params(url='/seComp?query=%E4%B8%8B%E5%8D%88%E8%8C&s=0&song_3rd=', list_qs=[], list_value=[]):
    try:
        dict_qs = yh_urlparse(url)
        dict_res = {}
        for i, s in enumerate(list_qs):
            if s in dict_qs:
                dict_res[s] = dict_qs[s]
            else:
                dict_res[s] = list_value[i]
        return dict_res
    except:
        logger.error('yh_urlparse_params error [%s][%s]' % (url, traceback.format_exc()))
        return {}
        
if __name__=='__main__':
    #list_path, list_domain = split_url()
    #logger.error('%s\t%s' % ('|'.join(list_path), '|'.join(list_domain)))
    str_url = unify_url('shoujihaomaguishudichaxun.com')
    logger.error(str_url)
    