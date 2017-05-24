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

from sys import exit

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
FI_funciones.logger = utiles.obtenerLogger('/tmp/FI.log')

exit()

cont = 0
conn, cur = FI_funciones.conexionBaseDatos(
    args.base, args.usuario, args.clave, args.servidor)

# primero aplicar el filtro
c_filtrado, c_qflag = FI_funciones.filtradoIndice(
    cur, args.esquema, args.tabla, args.c_afiltrar, args.c_calidad)
conn.commit()

pixeles = seriesInterpolar(cur, args.esquema, args.tabla, args.c_pixel, c_qflag)
total = len(pixeles)
for pixel in pixeles:
    # Aplico las interpolaciones para cada uno de los pixeles que lo necesitan
    id_pixel = pixel[0]
    FI_funciones.interpoladorSerie(
        conn, cur, args.esquema, args.tabla, c_filtrado, args.c_pixel, id_pixel)
    print(cont, total)
    cont += 1
    # raw_input()
    conn.commit()
conn.close()
