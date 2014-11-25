#!/usr/bin/env python
# -*- coding: UTF-8 -*-
from tornado.wsgi import WSGIContainer
from tornado.httpserver import HTTPServer
from tornado.ioloop import IOLoop
import tornado.gen, tornado.web

import sys, traceback, imp, os, subprocess, logging, datetime
from unipath import Path
import ConfigParser

sys.path.insert(0, "../YhHadoop")
sys.path.append('../Suggest')
#self module
import YhLog
import Searcher, SugIndexer
logger = logging.getLogger(__name__)

logger.error('global init start [%s]\n====================='%datetime.datetime.now())
class root_handler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    @tornado.gen.engine
    def get(self):
        try:
            dict_res=dict()
            self.write('ok')
        except Exception as e:
            logger.error('root handler fail')
            self.write('<Html><Body>Server fail:'+str(e)+'</Body></Html>')
        finally:
            try:
                self.finish()
            except:
                pass

class reload_handler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    @tornado.gen.engine
    def get(self):
        try:
            query = self.get_argument('query')
            fp, pathname, description = imp.find_module(query)
            imp.load_module(query, fp, pathname, description)
            logger.error('reload %s ok' % query)
            self.write('reload %s ok' % query)
        except:
            msg_err = traceback.format_exc()
            logger.error('reload failed, %s' % msg_err) 
            self.write(msg_err)
        finally:
            try:
                self.finish()
            except:
                pass

class restart_handler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    @tornado.gen.engine
    def get(self):
        try:
            logger.error('restart ok')  
            self.finish()
            p = subprocess.Popen('ps -ef | grep %s' __file__)
            logge.error(p.stdout)
            pid = p.stdout.split(' ')[1]
            str_restart = 'python  %s' % Path(Path(__file__).ancestor(1), str_process)
            p = subprocess.call(str_restart, stdout= None, shell=True)
        except:
            msg_err = traceback.format_exc()
            logger.error('restart failed, %s' % msg_err) 
            self.write(msg_err)
        finally:
            try:
                self.finish()
            except:
                pass

def multi_app():
    cwd = Path(__file__).absolute().ancestor(1)
    config = ConfigParser.ConfigParser()
    #config.read(Path(cwd, './conf/backend.conf'))
    port = 8888
    app = tornado.web.Application(handlers=[
        (r'/', root_handler),
        (r'/favicon.ico', root_handler),
        (r'/reload', reload_handler),
        (r'/restart', restart_handler),
        (r'/se', Searcher.Search_Handler),
        (r'/sug', SugIndexer.Sug_Handler),
        ])
    http_server = HTTPServer(app)
    http_server.bind(port)
    http_server.start()
    logger.error('listen port %s' % port)
    IOLoop.instance().start()
        
        
if __name__ == '__main__':
    pid = os.fork()
    if(pid >0):
        os._exit(0)
    multi_app()
