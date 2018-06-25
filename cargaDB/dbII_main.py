#!/usr/bin/env python3
# -*- coding: utf-8 -*-

########################################################################
##
## Gestion de carga de imagenes a la base de datos
##
## author:Gonzalo Garcia Accinelli https://github.com/gonzalogacc
## author:José Clavijo https://github.com/joseclavij/
## author:David Vinazza https://github.com/dvinazza/
########################################################################

from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
from datetime import datetime
# from textwrap import dedent
import json

from multiprocessing import cpu_count

import dbII_funciones
from utiles import *

PARSER = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
# Parametros de la DB
PARSER.add_argument("--base", default='var_sat',
                    help='Nombre de la base de datos destino de la carga.')
PARSER.add_argument("--usuario", default='postgres',
                    help='Nombre del usuario para conectarse a la base.')
PARSER.add_argument("--clave", default='postgres',
                    help='Clave del usuario para conectarse a la base.')
PARSER.add_argument("--servidor", default='localhost',
                    help='Hostname o IP del servidor al cual conectarse.')

# Parametros de las imagenes
PARSER.add_argument("--ruta", default="/modis",
                    help='Ruta al directorio donde estan las imágenes a cargar.')
PARSER.add_argument("--satelite", required=True)
PARSER.add_argument("--producto", required=True)

# TODO: Soportar listas, o si no se especifica, cargar todos los tiles que se encuentren
PARSER.add_argument("--tile", required=True)

# TODO: Armar la tabla destino a partir de los otros parametros: esquema.producto_dataset
PARSER.add_argument("--subdatasets", required=True, type=json.loads,
                    help='Array de JSON donde cada key es un dataset y su valor la tabla destino')
PARSER.add_argument("--version", default="006", help='Versión de las imágenes.')
PARSER.add_argument("--srid", default="96842", help='SRID de las imágenes.')
# TODO: No se puede detectar la proyeccion de la imagen? Seguro que si
PARSER.add_argument("--start-date", type=lambda d: datetime.strptime(d, "%d-%m-%Y"),
                    help='Fecha minima en formato DD-MM-YYYY')
PARSER.add_argument("--end-date", type=lambda d: datetime.strptime(d, "%d-%m-%Y"),
                    help='Fecha maxima en formato DD-MM-YYYY')

# Parmetros del script
PARSER.add_argument("--workers", type=int, default=cpu_count(),
                    help='Número de hilos para realizar la carga de las imágenes. Por defecto es el número de procesadores disponibles.')
PARSER.add_argument("--logfile", default="/tmp/cargaDB-%s.log" % obtenerUsuario(),
                    help='Archivo donde escribir los logs.') #
PARSER.add_argument("--dryrun", default=False, action='store_true',
                    help='Ejecutar todas las operaciones excepto la carga misma.')

ARGS = PARSER.parse_args()

# TODO: Debe haber un modo mas elegante de hacerlo...
dbII_funciones.args = ARGS
dbII_funciones.logger = obtenerLogger(ARGS.logfile)

## listar imagenes
#lista_imagenes = []
#lista_imagenes = np.array( [lista_imagenes + listarImagenes(i) for i in ARGS.ruta] ).flatten().tolist()

#print ARGS.subdatasets
imagenes = dbII_funciones.buscarImagenes(ARGS.ruta, ARGS.satelite,
                                         ARGS.producto, ARGS.version, ARGS.tile)

#print dbII_funciones.listarInventario()
#print lista_imagenes

dbII_funciones.chequearTablas(ARGS.subdatasets.values())

# Carga las imagenes nuevas, también actualiza el inventario
dbII_funciones.chequearInventario(imagenes, ARGS.subdatasets, workers=ARGS.workers)

# Actualiza las columnas de geometria y fecha de las tablas destino
dbII_funciones.executeUpdates(ARGS.subdatasets.values(), ARGS.srid)

# TODO: Ver si conviene paralelizar tambien el executeUpdates()
dbII_funciones.limpiaDuplicadas()

"""
## actualizar columnas de fechas y de indices luego de la carga de datos
conexion, cursor = conexionDatabase()
executeUpdates(conexion, cursor, [subdataset_tabla[i] for i in subdataset_tabla])
"""
