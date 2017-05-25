#!/usr/bin/env python
# -*- coding: utf-8 -*-

########################################################################
#
# Filtrador/Intepolador
#
# author:Gonzalo Garcia Accinelli https://github.com/gonzalogacc
#
########################################################################

import FI_funciones
import utiles

from argparse import ArgumentParser
from multiprocessing import cpu_count

# from sys import exit

parser = ArgumentParser()
parser.add_argument("--usuario", default='postgres')
parser.add_argument("--clave", default='postgres')
parser.add_argument("--servidor", default='localhost')
parser.add_argument("--base", default='var_sat_new')
parser.add_argument("--esquema", required=True)
parser.add_argument("--tabla", required=True)
parser.add_argument("--c_pixel", default='id_pixel')
parser.add_argument("--c_calidad", default='q')
parser.add_argument("--c_afiltrar", required=True)

# Parmetros del script
parser.add_argument("--workers", type=int, default=cpu_count(),
                    help="""Número de hilos para realizar la carga de las imágenes.
                    Por defecto es el número de procesadores disponibles.""")

args = parser.parse_args()

# TODO: Debe haber un modo mas elegante de hacerlo...
logger = utiles.obtenerLogger('/tmp/FI.log')
FI_funciones.logger = logger


cont = 0

logger.debug("Conectando a la base")
conn, cur = FI_funciones.conexionBaseDatos(
    args.base, args.usuario, args.clave, args.servidor)

""" Warning: By default, any query execution, including a simple SELECT will
start a transaction: for long-running programs, if no further action is taken,
the session will remain “idle in transaction”, an undesirable condition for
several reasons (locks are held by the session, tables bloat...) For long lived
scripts, either ensure to terminate a transaction as soon as possible or use
an autocommit connection.
http://initd.org/psycopg/docs/connection.html#connection.autocommit
"""
conn.set_session(autocommit=True)

logger.debug("Filtrando")
c_filtrado, c_qflag = FI_funciones.filtradoIndice(
    cur, args.esquema, args.tabla, args.c_afiltrar, args.c_calidad)
# conn.commit()

logger.debug("Obteniendo IDs de pixeles a interpolar")
pixeles = FI_funciones.seriesInterpolar(
    cur, args.esquema, args.tabla, args.c_pixel, c_qflag)
total = len(pixeles)

logger.debug("Obtuve %d pixeles" % total)

pixeles = [pixel[0] for pixel in pixeles]  # Me quedo con el id solamente

FI_funciones.interpoladorSerie(conn, pixeles, c_filtrado, args.workers)

conn.close()
