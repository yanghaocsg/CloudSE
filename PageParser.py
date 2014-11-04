#!/usr/env/bin python
#coding: utf8

import sys
from collections import defaultdict
import re, traceback
import phpserialize
import simplejson

sys.path.append('./YhHadoop')
#self module
import YhLog
logger = YhLog.logger

def PageParser(ifn='./data/test_content.file'):
    ofh =  open(ifn+'.buf', 'w+')
    for l in open(ifn):
        l = unicode(l.strip(), 'utf8', 'ignore')
        if not l: continue
        url, content = re.split('\t', l, 1)
        #logger.error(content)
        dict_tmp = {}
        content_comm = '{%s}' % re.sub('\'', '\"', content)
        
        #logger.error(content_comm)
        try:
            content_dict = simplejson.loads(content_comm)
        except:
            logger.error('%s\n%s' % (traceback.format_exc(), content_comm))
        dict_res=defaultdict(str)
        for k in ['title', 'content']:
            list_k = []
            if k in content_dict:
                for buf in content_dict[k]: 
                    #logger.error(buf)
                    tag_str = ''
                    txt_str = ''
                    if 'txt' in buf:
                        txt_str = buf['txt']
                        txt_str = re.sub('\d+:', '', txt_str)
                        if txt_str:
                            list_k.append(txt_str)
            dict_res[k] = re.sub('\s+', ',', ','.join(list_k))
        ofh.write(('%s\t%s\t%s\n' % (url, dict_res['title'], dict_res['content'])).encode('utf8', 'ignore'))

def HbaseParser(ifn='./data/test_content.file'):
    for l in open(ifn):
        l = unicode(l.strip(), 'utf8', 'ignore')
        if not l: continue
        url, content = re.split('\t', l, 1)
        other = content
        logger.error(content)
        a = re.findall('(".*?":".*?",)|(".*?":\[.*?\],)|(".*?":\{.*?\},)', content)
        print '\n'.join(str(a))
        break
if __name__=='__main__':
    '''if len(sys.argv)>=2:
        PageParser(sys.argv[1])
    else:
        print 'demo BuildCtrDict ifn'
        PageParser()
    '''
    HbaseParser()