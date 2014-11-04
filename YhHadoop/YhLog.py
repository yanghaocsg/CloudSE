# -*- coding: UTF-8 -*-
import sys, os
import logging, logging.config
from unipath import Path
logging.config.fileConfig(Path(Path(__file__).absolute().ancestor(1), './conf/logging.conf'), defaults=None, disable_existing_loggers=True)

