#!/usr/bin/env python

import logging

# https://stackoverflow.com/questions/15780151/how-to-use-python-logging-in-multiple-modules

loglevels = [logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR]

class Logger(object):
    @property
    def logger(self):
        return logging.getLogger('fi')


log_fmt = "%(asctime)s | %(levelname)s | %(module)s.%(funcName)s | %(message)s"

logging.basicConfig(
  level=loglevels[0],  # Al iniciar está en modo DEBUG
  filename='/tmp/fi.log', filemode='w',
  format=log_fmt,
  datefmt="%Y-%m-%d %H:%M:%S")
