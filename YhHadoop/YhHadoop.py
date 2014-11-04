# -*- coding: UTF-8 -*-
import sys, random,pprint, re, operator, ConfigParser
import httplib, urllib, urllib2, subprocess, urlparse
import logging
from unipath import Path

#self module
import YhLog, YhTool
logger = logging.getLogger(__name__)
FNULL = open('/dev/null', 'w')
class YhHadoop:
    def __init__(self, fnConf='./conf/hadoop.conf'):
        self.conf = ConfigParser.ConfigParser()
        self.cwd = Path(__file__).absolute().ancestor(1)
        self.conf.read(Path(self.cwd, fnConf))
        self.client = self.conf.get('client', 'client')
        self.hadoop_conf_dir = self.conf.get('client', 'HADOOP_CONF_DIR')
        self.hadoop = self.conf.get('client', 'HADOOP')
        self.streamingjar = self.conf.get('client', 'STREAMINGJAR')
    def run(self):
        logger.warn('YhHadoop')
        pass
        
class YhHadoopLs(YhHadoop):
    def run(self, str_dir=('', YhTool.today(1)), fname=''):
        if(not str_dir[0]):
            raise
        s_dir = self.conf.get('data', str_dir[0], 0, {'today':str_dir[1]})
        try:
            #str_cmd = ' %s --config %s ' %(self.hadoop, self.hadoop_conf_dir)
            str_cmd = ' %s  ' %(self.hadoop)
            str_cmd += ' fs -ls %s ' % Path(s_dir,fname)
            subprocess.check_call(str_cmd, shell=True, stdout=FNULL)
            logger.error('ls ok [%s]' % Path(s_dir, fname))
            return 0
        except:
            logger.error('ls error [%s]' % s_dir)
            return -1

class YhHadoopRmr(YhHadoop):
    def run(self, str_dir=('', YhTool.today(1))):
        if(not str_dir[0]):
            return -1
        s_dir = self.conf.get('data', str_dir[0], 0, {'today':str_dir[1]})
        try:
            str_cmd = ' %s ' % self.hadoop
            str_cmd += ' fs -rmr %s ' % s_dir
            if(subprocess.call(str_cmd, shell=True, stdout=FNULL) != 0):
                logger.error('ls error [%s]' % s_dir)
                return -1
            return 0
        except:
            logger.error('ls error [%s]' % s_dir)
            raise
            
class YhHadoopStreaming(YhHadoop):
    def run(self, list_input_dir=[], output_dir=('', YhTool.today(1)), fmapper='', freducer='', fname='yanghao_hadoop', list_file=[]):
        yhl = YhHadoopLs()
        if yhl.run(output_dir) == 0:
            return 0

        #str_cmd = ' %s --config %s jar %s ' %(self.hadoop, self.hadoop_conf_dir, self.streamingjar)
        str_cmd = ' %s jar %s ' %(self.hadoop, self.streamingjar)
        str_cmd += ' -D mapred.map.tasks=3000 -D mapred.reduce.tasks=1000 \
        -inputformat org.apache.hadoop.mapred.lib.CombineTextInputFormat \
        -jobconf mapred.max.split.size=1073741824 \
        -jobconf stream.num.map.output.key.fields=1 \
        -jobconf num.key.fields.for.partition=1 '
        str_cmd += ' -jobconf mapred.job.name=%s_%s ' %(fname, YhTool.today(1))
        str_cmd += ' -jobconf mapred.job.priority=VERY_HIGH '
        for dir in list_input_dir:
            logger.error('hadoop stream list_input_dir [%s]' % str(dir))
            if(len(dir) == 2):
                (f, d) = dir
                yhl = YhHadoopLs()
                val = yhl.run((f,d))
                logger.error('ls %s %s [%d]' % (f, d, val))
                if(val >= 0):
                    str_cmd += ' -input %s '  % self.conf.get('data', f, 0, {'today':d})
            else:
                str_cmd += ' -input %s '  % dir
                
        str_cmd += ' -output %s ' % self.conf.get('data', output_dir[0], 0, {'today':output_dir[1]})
        str_cmd += ' -mapper \"./Tool/python27/bin/python %s \" ' % fmapper
        str_cmd += ' -reducer \"./Tool/python27/bin/python %s \"' % freducer
        for f in list_file:
            str_cmd += ' -file %s ' % f
        for f in [Path(self.cwd, 'YhHadoopTool.py')]:
            str_cmd += ' -file %s ' % f
        str_cmd += ' -cacheArchive /home/recommend/Production/Tool/python27.tar.gz#Tool '
        logger.error('%s' % str_cmd)
        v = subprocess.call(str_cmd, shell=True)
        if(v != 0):
            raise

class YhHadoopCustomStreaming(YhHadoop):
    def run(self, list_input_dir=[], output_dir=('', YhTool.today(1)), fmapper='', freducer='', fname='yanghao_hadoop', list_file=[], other_options=[]):
        yhl = YhHadoopLs()
        if yhl.run(output_dir) == 0:
            return 0

        str_cmd = ' %s jar %s ' %(self.hadoop, self.streamingjar)
        for opt in other_options:
            if (not isinstance(opt, tuple)) or (len(opt) != 2):
                logger.error('streaming option error [%s]' % opt)
                return -1
            str_cmd += ' %s %s ' % (opt[0], opt[1])
        str_cmd += ' -jobconf mapred.job.name=%s_%s ' %(fname, YhTool.today(1))
        str_cmd += ' -jobconf mapred.job.priority=VERY_HIGH '
        for dir in list_input_dir:
            logger.error('hadoop stream list_input_dir [%s]' % str(dir))
            if(len(dir) == 2):
                (f, d) = dir
                yhl = YhHadoopLs()
                val = yhl.run((f,d))
                logger.error('ls %s %s [%d]' % (f, d, val))
                if(val >= 0):
                    str_cmd += ' -input %s '  % self.conf.get('data', f, 0, {'today':d})
            else:
                str_cmd += ' -input %s '  % dir
                
        str_cmd += ' -output %s ' % self.conf.get('data', output_dir[0], 0, {'today':output_dir[1]})
        str_cmd += ' -mapper \"./Tool/python27/bin/python %s \" ' % fmapper
        str_cmd += ' -reducer \"./Tool/python27/bin/python %s \"' % freducer
        for f in list_file:
            str_cmd += ' -file %s ' % f
        for f in [Path(self.cwd, 'YhHadoopTool.py')]:
            str_cmd += ' -file %s ' % f
        str_cmd += ' -cacheArchive /home/recommend/Production/Tool/python27.tar.gz#Tool '
        logger.error('%s' % str_cmd)
        v = subprocess.call(str_cmd, shell=True)
        if(v != 0):
            raise
            
class YhHadoopText(YhHadoop):
    def run(self, str_dir=('', YhTool.today(1)), fname=''):
        if(not str_dir[0] or not fname):
            return -1
        s_dir = self.conf.get('data', str_dir[0], 0, {'today':str_dir[1]})
        try:
            str_cmd = ' %s  ' %(self.hadoop)
            str_cmd += ' fs -text %s/* > %s ' % (s_dir, fname)
            subprocess.call(str_cmd, shell=True, stdout=FNULL)
            logger.error(str_cmd)
            return 0
        except:
            logger.error('text error [%s]' % s_dir)
            return -1
            
class YhHadoopGet(YhHadoop):
    def run(self, str_dir=('', YhTool.today(1)), fname='', localdir='./txt'):
        if(not str_dir[0] or not fname):
            return -1
        s_dir = self.conf.get('data', str_dir[0], 0, {'today':str_dir[1]})
        try:
            #test hdfs file
            yhl = YhHadoopLs()
            yhl.run(str_dir, fname)
            #rm local
            str_cmd = 'rm -rf %s' % Path(localdir, fname)
            subprocess.call(str_cmd, shell=True, stdout=FNULL)
            logger.error(str_cmd)
            #get
            str_cmd = ' %s  ' %(self.hadoop)
            str_cmd += ' fs -get -ignoreCrc %s  %s ' % (Path(s_dir, fname), Path(localdir))
            subprocess.call(str_cmd, shell=True, stdout=FNULL)
            logger.error(str_cmd)
            return 0
        except:
            logger.error('get error [%s]' % s_dir)
            raise
            
class YhHadoopPut(YhHadoop):
    def run(self, fname, str_dir=('', YhTool.today(1))):
        if(not str_dir[0]):
            return -1
        s_dir = self.conf.get('data', str_dir[0], 0, {'today':str_dir[1]})
        try:
            str_cmd = ' %s  ' %(self.hadoop)
            str_cmd += ' fs -rmr  %s ' % Path(s_dir, fname)
            logger.error(str_cmd)
            subprocess.call(str_cmd, shell=True, stdout=FNULL)
            str_cmd = ' %s  ' %(self.hadoop)
            str_cmd += ' fs -put %s %s ' % (fname, Path(s_dir, fname))
            logger.error(str_cmd)
            subprocess.call(str_cmd, shell=True, stdout=FNULL)
            return 0
        except:
            logger.error('put error [%s]' % s_dir)
            raise

class YhHadoopCp(YhHadoop):
    def run(self, src_dir=('', YhTool.today(1)), des_dir=('', YhTool.today(1))):
        if(not src_dir[0] or not des_dir[0]):
            return -1
        s_dir = self.conf.get('data', src_dir[0], 0, {'today':src_dir[1]})
        d_dir = self.conf.get('data', des_dir[0], 0, {'today':des_dir[1]})
        try:
            str_cmd = ' %s ' % self.hadoop
            str_cmd += ' fs -cp %s %s' % (s_dir, d_dir)
            if(subprocess.call(str_cmd, shell=True, stdout=FNULL) != 0):
                logger.error('cp error [%s]' % s_dir)
                return -1
            return 0
        except:
            logger.error('cp error [%s]' % s_dir)
            raise
        
class YhHadoopCpFile(YhHadoop):
    def run(self, src_dir=('', YhTool.today(1)), des_dir=('', YhTool.today(1))):
        if(not src_dir[0] or not des_dir[0]):
            return -1
        s_dir = self.conf.get('data', src_dir[0], 0, {'today':src_dir[1]})
        d_dir = self.conf.get('data', des_dir[0], 0, {'today':des_dir[1]})
        try:
            str_cmd = ' %s ' % self.hadoop
            str_cmd += ' fs -cp %s/* %s' % (s_dir, d_dir)
            if(subprocess.call(str_cmd, shell=True, stdout=FNULL) != 0):
                logger.error('cp error [%s]' % s_dir)
                return -1
            return 0
        except:
            logger.error('cp error [%s]' % s_dir)
            raise
            
def test_hadoop():
    '''
    h = YhHadoopLs()
    h.run(('ShortVideoInfo', YhTool.today(1)))
    h = YhHadoopRmr()
    h.run(('ShortVideoInfo', YhTool.today(1)))
    '''
    h = YhHadoopText()
    h.run(str_dir=('ShortVideoInfo', YhTool.today(1)))
if __name__=='__main__':
    test_hadoop()
    
    
        