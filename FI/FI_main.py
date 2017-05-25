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

from argparse import ArgumentParser
import utiles

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
for pixel in pixeles:
    # Aplico las interpolaciones para cada uno de los pixeles que lo necesitan
    id_pixel = pixel[0]
    logger.info("Interpolando %d de %d" % (cont, total))
    FI_funciones.interpoladorSerie(
        cur, args.esquema, args.tabla, c_filtrado, args.c_pixel, id_pixel)
    cont += 1
    # raw_input()
    # conn.commit()
conn.close()
