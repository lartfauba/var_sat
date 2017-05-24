#!/env python

from logging import getLogger, Formatter, FileHandler, StreamHandler
from logging import INFO, DEBUG, ERROR, WARN

def obtenerLogger(output):
    logger = getLogger('FI')
    logger.setLevel(DEBUG)

    formato = Formatter('%(asctime)s\t%(levelname)s\t%(module)s\t%(funcName)s\t%(message)s',
                        "%Y-%m-%d %H:%M:%S")

    fh = FileHandler(output)
    fh.setLevel(DEBUG)
    fh.setFormatter(formato)

    ch = StreamHandler()
    ch.setLevel(INFO)
    ch.setFormatter(formato)

    logger.addHandler(fh)
    logger.addHandler(ch)

    return logger
