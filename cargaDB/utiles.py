#!/usr/bin/env python3

from logging import getLogger, Formatter, FileHandler, StreamHandler
from logging import INFO, DEBUG, ERROR, WARN

from os import makedirs, remove, getuid
from pwd import getpwuid
import errno


def borrarArchivo(archivo):
    try:
        remove(archivo)
    except OSError as e:  ## if failed, report it back to the user ##
        print ("Error: %s - %s." % (e.filename,e.strerror))


def verificarDirectorio(path):
    try:
        makedirs(path)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise

def obtenerUsuario():
        return getpwuid(getuid())[0]

def obtenerLogger(output):
    logger = getLogger('var_sat')
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
