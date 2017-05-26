#!/env python

from logging import getLogger, Formatter, FileHandler, StreamHandler
from logging import INFO, DEBUG, ERROR, WARN

# https://stackoverflow.com/questions/7173033/duplicate-log-output-when-using-python-logging-module
loggers = {}


def obtenerLogger(output, nombre='FI'):
    global loggers

    if loggers.get(nombre):
        return loggers.get(nombre)

    logger = getLogger('FI')

    formato = Formatter(
        "%(asctime)s | %(process)d | %(levelname)s | %(module)s | %(funcName)s | %(message)s",
        "%Y-%m-%d %H:%M:%S")

    # https://stackoverflow.com/questions/6333916/python-logging-ensure-a-handler-is-added-only-once
    if not len(logger.handlers):
        fh = FileHandler(output)
        fh.setLevel(INFO)
        fh.setFormatter(formato)
        logger.addHandler(fh)

    loggers.update(dict(nombre=logger))

    return logger
