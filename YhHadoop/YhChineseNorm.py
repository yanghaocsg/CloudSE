# -*- coding: UTF-8 -*-
import re, sys
import logging

#self module
import YhLog
logger = logging.getLogger(__file__)


"""汉字处理的工具:
判断unicode是否是汉字，数字，英文，或者其他字符。
全角符号转半角符号。"""
def is_chinese(uchar):
    #print 'is_chinese [%s]' % str(type(uchar))
    """判断一个unicode是否是汉字"""
    if uchar >= u'\u4e00' and uchar<=u'\u9fa5':
            return True
    else:
            return False
 
def is_number(uchar):
    """判断一个unicode是否是数字"""
    if uchar >= u'\u0030' and uchar<=u'\u0039':
            return True
    else:
            return False
 
def is_alphabet(uchar):
    """判断一个unicode是否是英文字母"""
    if (uchar >= u'\u0041' and uchar<=u'\u005a') or (uchar >= u'\u0061' and uchar<=u'\u007a'):
            return True
    else:
            return False

def is_number_alphabet(uchar):
    return is_number(uchar) or is_alphabet(uchar) or uchar =='.'
    
def is_other(uchar):
    """判断是否非汉字，数字和英文字符"""
    if not (is_chinese(uchar) or is_number(uchar) or is_alphabet(uchar)):
            return True
    else:
            return False
 
def B2Q(uchar):
    """半角转全角"""
    inside_code=ord(uchar)
    if inside_code<0x0020 or inside_code>0x7e:      #不是半角字符就返回原来的字符
            return uchar
    if inside_code==0x0020: #除了空格其他的全角半角的公式为:半角=全角-0xfee0
            inside_code=0x3000
    else:
            inside_code+=0xfee0
    return unichr(inside_code)

def Q2B(uchar):
    """全角转半角"""
    inside_code=ord(uchar)
    if inside_code==0x3000:
            inside_code=0x0020
    else:
            inside_code-=0xfee0
    if inside_code<0x0020 or inside_code>0x7e:      #转完之后不是半角字符返回原来的字符
            return uchar
    return unichr(inside_code)
 
def stringQ2B(ustring):
    """把字符串全角转半角"""
    return "".join([Q2B(uchar) for uchar in ustring])
 
#def uniform(ustring):
#    """格式化字符串，完成全角转半角，大写转小写的工作"""
#    return stringQ2B(ustring).lower()
def uniform(ustring):
    """格式化字符串，完成全角转半角，大写转小写的工作"""
    str_tmp = stringQ2B(ustring).lower()
    utmp= []
    for u in str_tmp:
        if(is_other(u)):
            utmp.append(' ')
        else:
            utmp.append(u)
    return ''.join(utmp).lower()
    
def string2List(ustring):
    """将ustring按照中文，字母，数字分开"""
    retList=[]
    utmp=[]
    lastchar=u'-'
    
    for uchar in ustring:
        if is_other(uchar):
            if len(utmp)==0:
                lastchar = uchar
                continue
            else:
                retList.append(uniform(''.join(utmp)))
                utmp=[]
                lastchar=u'-'
        else:
            if(is_other(lastchar) or (is_number(lastchar) == is_number(uchar)) and (is_chinese(lastchar) == is_chinese(uchar)) and (is_alphabet(lastchar) == is_alphabet(uchar))):
                utmp.append(uchar)
            else:
                if(len(utmp) == 0):
                    lastchar = uchar
                    continue
                else:
                    retList.append(uniform(''.join(utmp)))
                    utmp=[uchar] 
        lastchar = uchar
    if len(utmp)!=0:
        retList.append(uniform(''.join(utmp)))
    return [i.lower() for i in retList]
    
def string2ListRaw(ustring):
    str_tmp = stringQ2B(ustring).lower()
    utmp= ''
    list_res = []
    for u in str_tmp:
        if(is_other(u)):
            if(utmp):
                list_res.append(utmp)
            utmp =''
            continue
        else:
            utmp += u
    if utmp:
        list_res.append(utmp)
    return list_res
    
def string2ListOther(ustring):
    retList = string2List(ustring)
    if(not retList):
        return [ustring.lower()]
    return retList
    
def string2Character(ustring):
    list_word = string2EngChnNum(ustring)
    list_char = []
    for l in list_word:
        if(is_chinese(l)):
            list_char.extend(l)
        elif(l):
            list_char.append(l)
    return list_char

    
def chinese_to_num(strnum):
    num={u'一':'1',u'二':'2',u'三':'3',u'四':'4',u'五':'5',u'六':'6',u'七':'7',u'八':'8',u'九':'9',u'零':'0','十':'','百':'','千':'','万':''}
    res = ''
    trans = 0
    for s in strnum:
        if(s in num):
            res += num[s]
            trans = 1
        else:
            res += s
    return res if trans else strnum

def is_chineseNum(strnum):
    chineseNum = u'^([第]?)([零一二三四五六七八九十百千万]+)([部季集]?)$'
    re_chineseNum = re.compile(chineseNum)
    m = re_chineseNum.match(strnum)
    if(m):
        strnum = m.group(2)
        if(strnum[0] == u'十'):
            return '1'+chinese_to_num(strnum[1:])
        else:
            return chinese_to_num(strnum)
    else:
        return strnum
        
def num_to_chinese(strnum):
    num={'1':u'一','2':u'二','3':u'三','4':u'四','5':u'五','6':u'六','7':u'七','8':u'八','9':u'九','0':u'零'}
    res = ''
    trans = 0
    for s in strnum:
        if(s in num):
            res += num[s]
            trans = 1
        else:
            res += s
    return res if trans else strnum


def string2EngChnNum(ustring):
    retList=[]
    utmp=[]
    lastchar=u'-'
    
    for uchar in ustring:
        if is_other(uchar):
            if len(utmp)==0:
                lastchar = uchar
                continue
            else:
                retList.append(uniform(''.join(utmp)))
                utmp=[]
                lastchar=u'-'
        else:
            if(is_other(lastchar) or (is_number_alphabet(lastchar) == is_number_alphabet(uchar)) and (is_chinese(lastchar) == is_chinese(uchar))):
                utmp.append(uchar)
            else:
                if(len(utmp) == 0):
                    lastchar = uchar
                    continue
                else:
                    retList.append(uniform(''.join(utmp)))
                    utmp=[uchar] 
        lastchar = uchar
    if len(utmp)!=0:
        retList.append(uniform(''.join(utmp)))
    return [i.lower() for i in retList]
    
def norm_query(ustring):
    return ' '.join(string2EngChnNum(ustring))

def get_keyword(ifn='', ofn=''):
    lines = [unicode(l, 'utf8', 'ignore') for l in open(ifn).readlines() if l.strip()]
    dict_keyword = {}
    for l in lines:
        list_res = string2List(l)
        for r in list_res:
            if is_chinese(r) and len(r) >=2 and len(r)<=7:
                dict_keyword[r] = 1
    list_keyword = dict_keyword.keys()
    list_keyword.sort()
    ofh = open('%s.keyword' % ifn, 'w+')
    ofh.write(('\n'.join(list_keyword)).encode('utf8', 'ignore'))

def get_nofuzzy_keyword(ifn='', ofn=''):
    lines = [unicode(l.strip(), 'utf8', 'ignore') for l in open(ifn).readlines() if l.strip()]
    set_keyword = set()
    for l in lines:
        list_res = string2List(l)
        if len(list_res) > 1 or not is_chinese(l[0]) or len(l)<= 1 or len(l)>7:
            continue
        set_keyword.add(l)
    list_keyword = list(set_keyword)
    list_keyword.sort()
    ofh = open('%s.keyword' % ifn, 'w+')
    ofh.write(('\n'.join(list_keyword)).encode('utf8', 'ignore'))


if __name__=="__main__":
    '''
    logger.error('here')
    ustring=u'中国 人名maggie.Q maggieQ, 江南style maggieq123'
    
    ret=string2List(ustring)
    logger.error('str2list %s[%s]' % ('\t'.join(ret), ustring))
    ret = string2Character(ustring)
    logger.error('str2char%s[%s]' % ('\t'.join(ret), ustring))
    ret = string2ListRaw(ustring)
    logger.error('str2char%s[%s]' % ('\t'.join(ret), ustring))
    ret = string2EngChnNum(ustring)
    logger.error('str2char%s[%s]' % ('\t'.join(ret), ustring))
    '''
    get_keyword(sys.argv[1])
    #get_nofuzzy_keyword(sys.argv[1])
    #ustring=u'中国 人名maggie.Q maggieQ, 江南style maggieq123'
    #logger.error('|'.join(string2Character(ustring)))