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
    logger.setLevel(INFO)

    formato = Formatter(
        "%(asctime)s | %(process)d | %(levelname)s | %(module)s | %(funcName)s | %(message)s",
        "%Y-%m-%d %H:%M:%S")

    fh = FileHandler(output)
    fh.setLevel(DEBUG)
    fh.setFormatter(formato)

    ch = StreamHandler()
    ch.setLevel(INFO)
    ch.setFormatter(formato)

    logger.addHandler(fh)
    logger.addHandler(ch)

    loggers.update(dict(nombre=logger))

    return logger
