#!/usr/bin/python
########################################################################
##
## Actualizar campo the_geom en tablas raster
##
## author:Gonzalo Garcia Accinelli https://github.com/gonzalogacc
## author:José Clavijo https://github.com/joseclavij/
## author:David Vinazza https://github.com/dvinazza/
########################################################################
###		ENTRADAS ################################################
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

	# Calcular solo geometrias NULL:
	solo_null = True

########################################################################
########################################################################
########################################################################

import numpy as np
import pandas as pd
import re
import psycopg2 as pg
from psycopg2.extras import DictCursor
from datetime import datetime
from copy import copy

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


def modis_distinct_fecha_tile_procesamiento(tabla_raster, clausulas):
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


def modis_distinct_anyo_tile(tabla_raster, clausulas):

	#~ from IPython.Shell import IPShellEmbed; embed = IPShellEmbed(); embed()
	
	ftp = modis_distinct_fecha_tile_procesamiento(tabla_raster, clausulas)

	anyo_tile = ftp[['anyo', 'tile']].drop_duplicates().sort(['anyo', 'tile'])
	anyo_tile.reset_index(drop=True, inplace=True)
	
	return anyo_tile

####

def execute_UPDATE_the_geom(tabla, anyo, tile, srid):
	'''
	Calcular geometrias para un anyo y un tile
	'''

	## WHERE filename LIKE '%2000%h12v12%'
	
	t0 = datetime.now()
	print '\n', 'execute_UPDATE_the_geom ||', t0, '||', tabla, anyo, tile, srid
	
	conn1, cur1 = conexionDatabase()

	## actualizar geometrias
	cur1.execute("UPDATE "+tabla+" SET the_geom = st_setsrid(cuad.poligono, "
					+srid+") FROM (SELECT rid, 'POLYGON(('||ux::varchar||' '||uy::varchar||','||lx::varchar||' '||uy::varchar||','||lx::varchar||' '||ly::varchar||','||ux::varchar||' '||ly::varchar||','||ux::varchar||' '||uy::varchar||'))' As poligono FROM (SELECT rid, (tr.md).upperleftx as ux, (tr.md).upperleftx + (tr.md).scalex * (tr.md).width as lx, (tr.md).upperlefty as uy, (tr.md).upperlefty + (tr.md).scaley * (tr.md).height as ly FROM (SELECT rid, ST_MetaData(rast) As md FROM "+tabla+") As tr) As tab) as cuad"
					+ clausula_geometrias					
					+" AND " + tabla + ".rid = cuad.rid"
					+" AND filename LIKE '%" + str(anyo) + "%" + str(tile) + "%';")
	conn1.commit()

	t1 = datetime.now()
	deltat = t1 - t0
	print '--> procesado en', deltat
	
	return t0, t1, 'OK!'
	
def update_geometrias(tabla_raster, srid):
	'''
	Calcular geometrias ("the_geom") para una tabla rasters.xxx
	'''

	anyo_tile = modis_distinct_anyo_tile(tabla_raster, clausulas=clausula_geometrias)

#	from IPython.Shell import IPShellEmbed; embed = IPShellEmbed(); embed()

	if len(anyo_tile) > 0:
		
		tini = copy(datetime.now())
		
		cont = 0
		for anyo, tile in anyo_tile.values:			
			execute_UPDATE_the_geom(tabla=tabla_raster, anyo=anyo, tile=tile, srid=str(srid))
			
			cont = cont + 1
			tfin = (datetime.now() - tini) / cont * (len(anyo_tile)-cont) + datetime.now()

			#~ print 'tini', tini
			print '\n', 'update_geometrias || Tiempo de proceso:', datetime.now()-tini, '||', cont, '/', len(anyo_tile), 'procesados', '||', 'Hora de finalizacion estimada:', tfin
	else:
		print '\n', 'update_geometrias ||', tabla_raster, '|| No hay registros con geometria NULL', '\n','--> no se hace nada'
####

def execute_UPDATE_fecha(tabla, anyo, tile):
	'''
	Calcular fechas para un anyo y un tile
	'''

	## WHERE filename LIKE '%2000%h12v12%'
	
	t0 = datetime.now()
	print '\n', 'execute_UPDATE_fecha ||', t0, '||', tabla, anyo
	
	conn1, cur1 = conexionDatabase()

	## actualizar fechas:
	cur1.execute("UPDATE "+tabla+" SET fecha = date ('1-1-'||substring(split_part(filename, '.', 2),2,4)) + (substring(split_part(filename, '.', 2),6,3)::int-1)"
					+clausula_fechas
					+" AND filename LIKE '%" + str(anyo) + "%" + str(tile) + "%';")
	conn1.commit()

	t1 = datetime.now()
	deltat = t1 - t0
	print '--> procesado en', deltat
	
	return t0, t1, 'OK!'
	
def update_fechas(tabla_raster):
	'''
	Calcular fechas ("fecha") para una tabla rasters.xxx
	'''
		
	anyo_tile = modis_distinct_anyo_tile(tabla_raster, clausulas=clausula_fechas)

	tini = copy(datetime.now())

	cont = 0
	for anyo, tile in anyo_tile.values:
		execute_UPDATE_fecha(tabla=tabla_raster, anyo=anyo, tile=tile)
		
		cont = cont + 1
		tfin = (datetime.now() - tini) / cont * (len(anyo_tile)-cont) + datetime.now()

		print '\n', 'update_fechas || Tiempo de proceso:', datetime.now()-tini, '||', cont, '/', len(anyo_tile), 'procesados', '||', 'Hora de finalizacion estimada:', tfin
	
########################################################################
########################################################################
########################################################################

if __name__ == '__main__':
	
	if solo_null: 
		clausula_geometrias = ' WHERE the_geom ISNULL'
		clausula_fechas = ' WHERE fecha ISNULL'
		'''
		# OJO! si se eleimina la clausula, hay que modificar clausulas "WHERE" y "AND"
		# en las funciones update...() y execute_UPDATE...()
		'''
	
	#~ """
	# Calcular columna de geometrias para una tabla rasters.xxx:
	'''		(lo hace muuuucho mas rapido que intentando actualizar toda la tabla de una vez)	'''
	'''		(tambien funciona mejor para productos SIN tile (Ej: MOD11C2)	'''
	
	#~ update_geometrias(tabla_raster='rasters.mod11a2_lstd', srid=96842)
	
	update_geometrias(tabla_raster='rasters.mod13a2_ndvi', srid=96842)
	update_geometrias(tabla_raster='rasters.mod13a2_evi', srid=96842)
	update_geometrias(tabla_raster='rasters.mod13a2_qa', srid=96842)
	
	#~ update_geometrias(tabla_raster='rasters.mod16a2_1km_8d_qa', srid=96842)
	#~ update_geometrias(tabla_raster='rasters.mod16a2_1km_8d_et', srid=96842)
	#~ update_geometrias(tabla_raster='rasters.mod16a2_1km_8d_pet', srid=96842)
	
	#~ update_geometrias(tabla_raster='rasters.mod11c2_lstd', srid=7008)
	#~ update_geometrias(tabla_raster='rasters.mod11c2_lstn', srid=7008) 
	
	#~ update_geometrias(tabla_raster='rasters.mod13c1_ndvi', srid=7008)

	#~ """
	
	"""
	# Calcular columna de fechas para una tabla rasters.xxx:
	#	(no parece mejorar el tiempo de proceso respecto a actualizar toda la tabla de una vez)
	
	#~ update_fechas(tabla_raster='rasters.mod13q1_qa')
	#~ update_fechas(tabla_raster='rasters.mod13q1_ndvi')
	#~ update_fechas(tabla_raster='rasters.mod13q1_evi')
	"""

	
