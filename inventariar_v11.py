#!/usr/bin/python
########################################################################
##
## Inventario de rasters
##
## author:Gonzalo Garcia Accinelli https://github.com/gonzalogacc
## author:José Clavijo https://github.com/joseclavij/
## author:David Vinazza https://github.com/dvinazza/

"""
Mostrar archivos cargados en las tablas rasters., agrupando de diferentes
formas, para identificar imagenes faltantes o duplicadas

IMPORTANTE: este script consulta las tablas de imagenes "rasters.xxx". Se
podria hacer lo mismo, pero consultando rasters.inventario, una vez que 
el inventario funcione perfectamente
"""

########################################################################
###		ENTRADAS		################################################
########################################################################

if __name__ == '__main__':

	connection_params = {
		'database': 'var_sat_new', 
		#'database': 'var_sat_tmp', 
		'user': 'postgres', 
		'password': 'postgres', 
		'host': '10.1.1.109',
		#'host': '10.1.1.239',
		}

########################################################################
########################################################################
########################################################################

import numpy as np
import pandas as pd
import re
import psycopg2 as pg
from psycopg2.extras import DictCursor

def conexionDatabase ():
    """
    Realiza una conexion con la base de datos y devuelve el objeto conexion y el objeto cursor para hacer las consultas correspondientes

    Argumentos
    ----------
    None

    Devuelve
    ---------
    conn: el objeto conexion a la base de datos
    cursor: el cursor para hacer las consultas en la base de datos
    """
    conn = pg.connect(database=connection_params['database'], user = connection_params['user'], password = connection_params['password'], host = connection_params['host'])
    #conn = pg.connect(database = 'var_sat_new', user = 'postgres', password = 'postgres', host = '10.1.1.109')
    cur = conn.cursor(cursor_factory = DictCursor)
    return conn, cur


def modis_parse_tile_fecha_time(cadena):
	'''
	Seleccionar tile, fecha y fecha de procesamiento, parseando el nombre
	de la imagen MODIS. Generalizado para manejar productos con tile (ej: MOD13A2) 
	y sin tile (ej: MOD13C1)
		
	OUTPUT -> tabla inicial para GROUP BY posteriores
	'''
	
	#~ from IPython.Shell import IPShellEmbed; embed = IPShellEmbed(); embed()
	
	# Parsear nombre MODIS:
	parse = re.split('.hdf',cadena)[0] .split(':')[-1] .split('.')
	
	if len(parse) == 4:
		# Asume que es un producto SIN tile
		tmp, fecha, tmp, procesamiento = parse
		tile = ''
	elif len(parse) == 5:
		# Asume que es un producto CON tile
		tmp, fecha, tile, tmp, procesamiento = parse

	anyo = fecha[1:5]
	return anyo, fecha, tile, procesamiento

  
def modis_distinct_fecha_tile_procesamiento(tabla_raster, clausulas=''):
	'''
	Consultar combinaciones distintas de fecha-tile-fecha_de_procesamiento
	'''
	conn1, cur1 = conexionDatabase()
	cur1.execute('SELECT DISTINCT filename FROM ' + tabla_raster + ' ' + clausulas)

	#~ from IPython.Shell import IPShellEmbed; embed = IPShellEmbed(); embed()

	f = cur1.fetchall()
	
	if len(f) > 0:
		f = pd.Series(f).apply(lambda x: modis_parse_tile_fecha_time(x[0]))
		f = pd.DataFrame(f.values.tolist(), columns=('anyo', 'fecha', 'tile', 'procesamiento'))
	else:
		f = pd.DataFrame(columns=('anyo', 'fecha', 'tile', 'procesamiento'))

	return f


def modis_escenas_por_fecha_tile(tabla_raster):
	'''
	Devuelve las combinaciones fecha-tile para las cuales se cargo mas
	de una escena (mas de una fecha de procesamiento NASA)
	'''
	print '\n', 'modis_escenas_por_fecha_tile ||',  tabla_raster

	#~ from IPython.Shell import IPShellEmbed; embed = IPShellEmbed(); embed()
	
	cantidad = modis_distinct_fecha_tile_procesamiento(tabla_raster=tabla_raster).groupby(by=['fecha', 'tile']).agg({'procesamiento':'count'})
	multiples = cantidad [cantidad['procesamiento']>1]
	
	if len(multiples) == 0: print '\nNo hay escenas multiples por fecha-tile\n'
		
	return multiples


def modis_registros_por_tile_anyo(tabla_raster):
	'''
	Grueso, para detectar cosas raras a nivel tile-anyo
	'''
	print '\n', 'modis_registros_por_tile_anyo ||', tabla_raster
	
	ftp = modis_distinct_fecha_tile_procesamiento(tabla_raster)
	return ftp.pivot_table(rows='anyo', cols='tile', aggfunc='count', values='procesamiento')


def modis_registros_por_tile(pdataf):	
	pass


def modis_borrar_procesamientos_viejos(tabla_raster):
	'''
	Para las combinaciones producto-fecha-tile que hay mas de una escena,
	seleccionar la de fecha de procesamiento NASA mas nueva y eliminar las mas
	viejas
	'''
	print '\n', 'modis_borrar_procesamientos_viejos ||', tabla_raster
	
	def executeDelete(tabla_raster, fecha, tile, procesamiento):

		tabla_producto = {'rasters.mod13q1_ndvi': 'NDVI',
							'rasters.mod13q1_evi': 'EVI',
							'rasters.mod13q1_qa': 'VI Quality'}
		
		# Borrar de la tabla rasters.:
		sentencia1 = 'DELETE FROM ' + tabla_raster + " WHERE filename LIKE '%" + str(fecha) + "%" + str(tile) + "%" + str(procesamiento) + "%" + tabla_producto[tabla_raster] + "%'"		
		print sentencia1
		cur1.execute(sentencia1)
		
		# Borrar de la tabla rasters.inventario:
		sentencia2 = "DELETE FROM rasters.inventario WHERE imagen LIKE '%" + str(fecha) + "%" + str(tile) + "%" + str(procesamiento) + "%" + tabla_producto[tabla_raster] + "%'"		
		print sentencia2
		cur1.execute(sentencia2)
		
		return conn1.commit()


	# Ver escenas para cada fecha-tile:
	fecha_tile_proc = modis_distinct_fecha_tile_procesamiento(tabla_raster=tabla_raster)
	cantidad = fecha_tile_proc.groupby(by=['fecha', 'tile']).agg({'procesamiento':'count'})
	
	# Seleccionar las escenas con redundancia:
	idx = cantidad [cantidad['procesamiento']>1] .index
	redundantes = fecha_tile_proc[['fecha','tile','procesamiento']].set_index(['fecha', 'tile'], drop=False) .ix[idx]
	
	# Extraer la fecha de procesamiento mas nueva -> las demas seran eliminadas
	redundantes = redundantes.sort(['procesamiento'], ascending=True).sort_index()
	redundantes['fecha'] = redundantes.index.get_level_values('fecha')
	redundantes['tile'] = redundantes.index.get_level_values('tile')
	
	seleccion = redundantes.groupby(by=['fecha','tile'], as_index=False).last()
	seleccion['seleccion'] = 1
	
	# Substraer las escenas de fecha de procesamiento mas reciente -> las que quedan seran eliminadas:
	borrar = pd.DataFrame.merge(redundantes, seleccion, on=['fecha', 'tile', 'procesamiento'], how='left')
	borrar = borrar [borrar['seleccion']!=1]
		
	# Borrar fechas imagenes de rasters.xxx y rasters.inventario:
	
	conn1, cur1 = conexionDatabase()
	for fecha, tile, proc in borrar[['fecha','tile','procesamiento']].values:
		executeDelete(tabla_raster, fecha, tile, proc)


########################################################################
########################################################################
########################################################################

if __name__ == '__main__':

	### Borrar escenas redundantes y viejas para cada fecha-tile MODIS:
	"""
	#modis_borrar_procesamientos_viejos(tabla_raster='rasters.mod13q1_qa')
	#modis_borrar_procesamientos_viejos(tabla_raster='rasters.mod13q1_ndvi')
	#modis_borrar_procesamientos_viejos(tabla_raster='rasters.mod13q1_evi')

	modis_borrar_procesamientos_viejos(tabla_raster='rasters.mod16a2_1km_8d_qa')
	modis_borrar_procesamientos_viejos(tabla_raster='rasters.mod16a2_1km_8d_et')
	modis_borrar_procesamientos_viejos(tabla_raster='rasters.mod16a2_1km_8d_pet')
	"""

	#~ """
	### Ver escenas MODIS distintas por tile y por anyo:

	#print modis_registros_por_tile_anyo(tabla_raster='rasters.mod13q1_qa')
	#print modis_registros_por_tile_anyo(tabla_raster='rasters.mod13q1_evi')
	#print modis_registros_por_tile_anyo(tabla_raster='rasters.mod13q1_ndvi')
	
	#print modis_registros_por_tile_anyo(tabla_raster='rasters.mod11a2_lstd')
	#print modis_registros_por_tile_anyo(tabla_raster='rasters.mod11a2_lstn')
	
	#print modis_registros_por_tile_anyo(tabla_raster='rasters.mod13a2_qa')
	#print modis_registros_por_tile_anyo(tabla_raster='rasters.mod13a2_evi')
	#print modis_registros_por_tile_anyo(tabla_raster='rasters.mod13a2_ndvi')
	
	#~ print modis_registros_por_tile_anyo(tabla_raster='rasters.mod16a2_1km_8d_qa')
	#~ print modis_registros_por_tile_anyo(tabla_raster='rasters.mod16a2_1km_8d_et')
	#~ print modis_registros_por_tile_anyo(tabla_raster='rasters.mod16a2_1km_8d_pet')
	
	#~ print modis_registros_por_tile_anyo(tabla_raster='rasters.mod11c2_lstd')
	#~ print modis_registros_por_tile_anyo(tabla_raster='rasters.mod11c2_lstn')
	
	print modis_registros_por_tile_anyo(tabla_raster='rasters.mod13c1_ndvi')
	
	#~ """

	#"""
	### Ver combinaciones fecha-tile MODIS para los cuales se cargo mas 
	###		de una escena (distintas fechas de procesamiento NASA):
	
	# MOD13Q1
	#print modis_escenas_por_fecha_tile(tabla_raster='rasters.mod13q1_qa')
	#print modis_escenas_por_fecha_tile(tabla_raster='rasters.mod13q1_evi')
	#print modis_escenas_por_fecha_tile(tabla_raster='rasters.mod13q1_ndvi')
	
	# MOD11A2
	#print modis_escenas_por_fecha_tile(tabla_raster='rasters.mod11a2_lstd')
	#print modis_escenas_por_fecha_tile(tabla_raster='rasters.mod11a2_lstn')
	
	# MOD16A2
	#~ print modis_escenas_por_fecha_tile(tabla_raster='rasters.mod16a2_1km_8d_qa')
	#~ print modis_escenas_por_fecha_tile(tabla_raster='rasters.mod16a2_1km_8d_et')
	#~ print modis_escenas_por_fecha_tile(tabla_raster='rasters.mod16a2_1km_8d_pet')
	
	# MOD11C2
	#~ print modis_escenas_por_fecha_tile(tabla_raster='rasters.mod11c2_lstd')
	#~ print modis_escenas_por_fecha_tile(tabla_raster='rasters.mod11c2_lstn')
	
	# MOD13C1
	print modis_escenas_por_fecha_tile(tabla_raster='rasters.mod13c1_ndvi')


	#"""
	

