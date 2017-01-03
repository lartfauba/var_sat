#!/usr/bin/env python
# -*- coding: utf-8 -*-

########################################################################
##
## Gestion de carga de imagenes a la base de datos
##
## author:Gonzalo Garcia Accinelli https://github.com/gonzalogacc
## author:José Clavijo https://github.com/joseclavij/
## author:David Vinazza https://github.com/dvinazza/
########################################################################

import dbII_funciones
from utiles import *

from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
from textwrap import dedent
import json

from multiprocessing import cpu_count

parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
# Parametros de la DB
parser.add_argument(	"--base", default='var_sat',
			help='Nombre de la base de datos destino de la carga.')
parser.add_argument(	"--usuario", default='postgres',
			help='Nombre del usuario para conectarse a la base.')
parser.add_argument("--clave", default='postgres',
			help='Clave del usuario para conectarse a la base.')
parser.add_argument("--servidor", default='localhost',
			help='Hostname o IP del servidor al cual conectarse.')

# Parametros de las imagenes
parser.add_argument("--ruta", default="/modis",
			help='Ruta al directorio donde estan las imágenes a cargar.')
parser.add_argument("--satelite", required=True)
parser.add_argument("--producto", required=True)
parser.add_argument("--version", default="005",
			help='Versión de las imágenes.')
parser.add_argument("--tile", required=True)  # TODO: Soportar listas, o si no se especifica, cargar todos los tiles que se encuentren
parser.add_argument("--subdatasets", required=True, type=json.loads,
			help='Array de JSON donde cada key es un dataset y su valor la tabla destino')
parser.add_argument("--srid", default="96842",
			help='SRID de las imágenes.')
# TODO: No se puede detectar la proyeccion de la imagen? Seguro que si

# Parmetros del script
parser.add_argument("--workers", type=int, default=cpu_count(),
			help='Número de hilos para realizar la carga de las imágenes. Por defecto es el número de procesadores disponibles.')
parser.add_argument("--logfile", default="/tmp/cargaDB.log",
			help='Archivo donde escribir los logs.') #
parser.add_argument("--dryrun", default=False, action='store_true',
			help='Ejecutar todas las operaciones excepto la carga misma.')

args = parser.parse_args()

# TODO: Debe haber un modo mas elegante de hacerlo...
dbII_funciones.args = args
dbII_funciones.logger = obtenerLogger(args.logfile)

## listar imagenes
#lista_imagenes = []
#lista_imagenes = np.array( [lista_imagenes + listarImagenes(i) for i in args.ruta] ).flatten().tolist()

#print args.subdatasets
imagenes = dbII_funciones.buscarImagenes(args.ruta, args.satelite, args.producto, args.version, args.tile)

#print dbII_funciones.listarInventario()
#print lista_imagenes

dbII_funciones.chequearTablas(args.subdatasets.values())

# Carga las imagenes nuevas, también actualiza el inventario
dbII_funciones.chequearInventario(imagenes, args.subdatasets, workers=args.workers)

# Actualiza las columnas de geometria y fecha de las tablas destino
dbII_funciones.executeUpdates(args.subdatasets.values(),args.srid)

# TODO: Ver si conviene paralelizar tambien el executeUpdates()
dbII_funciones.limpiaDuplicadas()

"""
## actualizar columnas de fechas y de indices luego de la carga de datos
conexion, cursor = conexionDatabase()
executeUpdates(conexion, cursor, [subdataset_tabla[i] for i in subdataset_tabla])
"""
