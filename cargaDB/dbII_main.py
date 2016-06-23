#!/usr/bin/python
# -*- coding: utf-8 -*-
########################################################################
##
## Gestion de carga de imagenes a la base de datos
##
## author:Gonzalo Garcia Accinelli https://github.com/gonzalogacc
## author:José Clavijo https://github.com/joseclavij/
## author:David Vinazza https://github.com/dvinazza/
########################################################################
########################################################################

import dbII_funciones
from utiles import *

from argparse import ArgumentParser
import json


parser = ArgumentParser()
# Parametros de la DB
parser.add_argument("--base", default='var_sat')
parser.add_argument("--usuario", default='postgres')
parser.add_argument("--clave", default='postgres')
parser.add_argument("--servidor", default='localhost')

# Parametros de las imagenes
parser.add_argument("--ruta", default="/modis")
parser.add_argument("--satelite", required=True)
parser.add_argument("--producto", required=True)
parser.add_argument("--version", default="005")
parser.add_argument("--tile", required=True)
parser.add_argument("--subdatasets", required=True, type=json.loads)
parser.add_argument("--srid", default="96842")
# TODO: No se puede detectar la proyeccion de la imagen? Seguro que si

# Parmetros del script
parser.add_argument("--workers", type=int, default=4)
parser.add_argument("--logfile", default="/tmp/cargaDB.log") #

args = parser.parse_args()

# TODO: Debe haber un modo mas elegante de hacerlo...
dbII_funciones.args = args

## listar imagenes
#lista_imagenes = []
#lista_imagenes = np.array( [lista_imagenes + listarImagenes(i) for i in args.ruta] ).flatten().tolist()

#print args.subdatasets
imagenes = dbII_funciones.buscarImagenes(args.ruta, args.satelite, args.producto, args.version, args.tile)

#print dbII_funciones.listarInventario()
#print lista_imagenes
## Chequear inventario -> cargar imagenes nuevas
dbII_funciones.chequearInventario(imagenes, args.subdatasets, workers=args.workers)

## TODO: Ver si conviene paralelizar tambien el executeUpdates()

"""
## actualizar columnas de fechas y de indices luego de la carga de datos
conexion, cursor = conexionDatabase()
executeUpdates(conexion, cursor, [subdataset_tabla[i] for i in subdataset_tabla])
"""
