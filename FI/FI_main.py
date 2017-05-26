#!/usr/bin/env python3
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

from datetime import datetime

# from sys import exit

parser = ArgumentParser()
parser.add_argument("--usuario", default='postgres')
parser.add_argument("--clave", default='postgres')
parser.add_argument("--servidor", default='localhost')
parser.add_argument("--base", default='var_sat_new')
parser.add_argument("--esquema", required=True)
parser.add_argument("--tabla", required=True)
parser.add_argument("--c_pixel", default='id_pixel')
parser.add_argument("--c_qflag", default='q_malo')
parser.add_argument("--c_calidad", default='q')
parser.add_argument("--c_afiltrar", required=True)
parser.add_argument("--logfolder", default='/var/log/FI')

# Parmetros del script
parser.add_argument("--workers", type=int, default=cpu_count(),
                    help="""Número de hilos para realizar la carga de las imágenes.
                    Por defecto es el número de procesadores disponibles.""")

args = parser.parse_args()

log_file = '%s/FI-%s-%s.%s.%s.log' % (
    args.logfolder, datetime.now().strftime('%Y%m%d%H%M%S'),
    args.esquema, args.tabla, args.c_afiltrar)

print(log_file)  # Lo "imprimo" para que lo vea plsh
logger = utiles.obtenerLogger(log_file)

# TODO: Debe haber un modo mas elegante de hacerlo...
FI_funciones.logger = logger
FI_funciones.args = args

logger.debug("Conectando a la base")
conn, cur = FI_funciones.conexionBaseDatos(
    args.base, args.usuario, args.clave, args.servidor)

FI_funciones.dbConn = conn
FI_funciones.dbCurs = cur

logger.info("Filtrando")
FI_funciones.filtradoIndice(cur, args)

logger.info("Obteniendo IDs de series a interpolar")
series = FI_funciones.seriesInterpolar(cur, args)

logger.info("Interpolando %d series" % len(series))
series = [i[0] for i in series]  # Me quedo con el id solamente
FI_funciones.interpoladorSerie(cur, args, series)

conn.close()
