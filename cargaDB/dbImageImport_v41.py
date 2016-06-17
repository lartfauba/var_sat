#!/usr/bin/python
########################################################################
##
## Gestion de carga de imagenes a la base de datos
##
## author:Gonzalo Garcia Accinelli https://github.com/gonzalogacc
## author:José Clavijo https://github.com/joseclavij/
## author:David Vinazza https://github.com/dvinazza/
########################################################################
########################################################################

# IMPORTANTE: ejecutar:

# python dbImageImport_...py > nombre_archivo_de_texto.log

# ... para guardar alertas y mensajes de error en el archivo nombre_archivo_de_texto.log

########################################################################
## 		INPUTS			################################################
########################################################################
if __name__=="__main__":

	#ruta_imagenes = ['/tmp/prueba_dbimport/']
	#ruta_imagenes = ['/biasatti/raid6/modis/MOLT/MOD13Q1/h13v12/']
	#ruta_imagenes = ['/biasatti/raid6/modis/MOLT/MOD13A2/']
	#ruta_imagenes = ['/biasatti/raid6/modis/MOLT/MOD11A2/]
	#ruta_imagenes = ['/biasatti/raid6/lartproc/imagenes/descargas/TRMM_3B42_daily_v7_netcdf/georref/']
	#ruta_imagenes = ['/biasatti/raid6/lartproc/imagenes/descargas/MOD16/']
	ruta_imagenes = ['/biasatti/raid6/modis/MOLT/MOD11C2/']
	#ruta_imagenes = ['/biasatti/raid6/modis/MOLT/MOD13C1/']

	#~ srid = '96842'	# Proyeccion MODIS tile (Sinusoidal)
	srid = '7008'	# Proyeccion MODIS CMG (latlon Clarke66)
	
	#~ subdataset_tabla = {'SUBDATASET_1_NAME': 'rasters.mod13q1_ndvi', 
								#~ 'SUBDATASET_2_NAME': 'rasters.mod13q1_evi', 
								#~ 'SUBDATASET_3_NAME': 'rasters.mod13q1_qa'}
	#subdataset_tabla = {'SUBDATASET_1_NAME': 'rasters.mod13a2_ndvi', 
								#'SUBDATASET_2_NAME': 'rasters.mod13a2_evi', 
								#'SUBDATASET_3_NAME': 'rasters.mod13a2_qa'}
	#~ subdataset_tabla = {'SUBDATASET_1_NAME': 'rasters.mod11a2_lstd', 
								#~ 'SUBDATASET_5_NAME': 'rasters.mod11a2_lstn'}
	subdataset_tabla = {'SUBDATASET_1_NAME': 'rasters.mod11c2_lstd', 
								'SUBDATASET_6_NAME': 'rasters.mod11c2_lstn'}
	#~ subdataset_tabla = {'SUBDATASET_1_NAME': 'rasters.mod16a2_1km_8d_et', 
								#~ 'SUBDATASET_3_NAME': 'rasters.mod16a2_1km_8d_pet', 
								#~ 'SUBDATASET_5_NAME': 'rasters.mod16a2_1km_8d_qa'}
	#subdataset_tabla = {'SUBDATASET_1_NAME': 'rasters.mod13c1_ndvi'}

	#subdataset_tabla = {'': 'rasters.trmm3b42_daily_pp'}
		# Las TRMM son archivos tif, sin subdataset => ''

	connection_params = {
		'database': 'var_sat_new', 
		#~ 'database': 'var_sat_tmp', 
		#~ 'database': 'dev_var_sat', 
		#~ 'database': 'VAR_SAT', 
		'user': 'postgres', 
		'password': 'postgres', 
		'host': '10.1.1.109',
		#~ 'host': '10.1.1.239',
		}
		
	n_trabajos_paralelos = 3
	
########################################################################
########################################################################
########################################################################

import os
import psycopg2 as pg
from psycopg2.extras import DictCursor
from datetime import datetime
import numpy as np
from osgeo import gdal

from tempfile import NamedTemporaryFile
from subprocess import Popen

import multiprocessing

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

def listarImagenes (path):
    """
    Lista archivos de las imagenes que se encuetran en un arbol de directorios indicado y devuelve una lista de las imagenes

    Argumento
    -----------
    path: ruta al directorio del cual se quiere listar las imagenes

    Devuelve
    ---------
    lista_archivos: lista con los path completos a las imagenes del arbol de directorios
    """
    
    lista_archivos = []
    ## chequear que sea una imagen sino es lo mismo que usar la otra funcion directamente
    for (path, directorios, archivos) in os.walk(path):
        for archivo in archivos:
            #~ if archivo.split('.')[-1] in ['hdf', 'HDF']:
            if archivo.split('.')[-1] in ['hdf', 'HDF', 'nc', 'NC']:
                lista_archivos.append(os.path.join(path, archivo))

    return lista_archivos

def listarInventario():
	"""
	Devuelve una lista de las imagenes que ya estan importadas
	"""	
	conexion, cursor = conexionDatabase()
	# OJO!!! En multiprocessing no puedo traer la conexion y el cursor como inputs de la funcion listarInventario()
	#			-> Necesita crear las instancias aqui adentro

	cursor.execute('SELECT imagen FROM rasters.inventario')
	imagenes_inventario = cursor.fetchall()

	try:
		## si hay imagenes las agrega a la variable
		imagenes_inventario = np.array(imagenes_inventario)[:,0]
	except:
		imagenes_inventario = []

	return imagenes_inventario


def chequearInventario (lista_argumentos):
	imagen = lista_argumentos[0]
	subdataset_tabla = lista_argumentos[1]

	"""
	Consulta la tabla en la base de datos que tiene la lista de imagenes cargadas en el servidor

	Argumentos
	-----------

	Devuelve
	---------
	"""

	imagenes_inventario = listarInventario()

	#~ from IPython.Shell import IPShellEmbed; embed = IPShellEmbed(); embed()

	## se queda solo con el nombre y no con el path
	base_path, nombre_imagen = os.path.split(imagen)
	os.chdir(base_path)

	subdatasets = obtenerSubdatasets(nombre_imagen)
	
	# Si es un TIFF o cualquier imagen in subdatasets, utiliza solo el nombre del archivo:
	if subdatasets == dict({}): subdatasets[''] = nombre_imagen

	for sds in subdatasets:
		if sds not in subdataset_tabla.keys():
			print '%s: no fue seleccionada por el usuario' %(subdatasets[sds])
			continue

		if subdatasets[sds] in imagenes_inventario:

		### DEPURAR: manejar ditintas fechas de procesamiento NASA para una misma scena ###

			print '%s: ya esta cargada en la base de datos' %(subdatasets[sds])
			continue

		print '%s: no esta cargada y se va a cargar' %(subdatasets[sds],)

		if importarImagen(subdatasets[sds], subdataset_tabla, sds) != 0:
			print '%s: se cargo correctamente' %(subdatasets[sds])
		else:
			print '%s: no se cargo' %(subdatasets[sds])

	return None

def importarImagen (path_imagen, subdataset_tabla, sds):
    """
    Dado el path a una imagen la importa en la base de datos en la tabla correspondiente

    Argumentos
    -----------
    path_imagen: path a la imagen que se quiere cargar en la base de datos
    flags: flags que se le van a pasar a la funcion para activar sus opciones, \
            tienen que ser en la forma '-a -F', aca se pone el srid, el tamanio de bloque etc

    Devuelve
    ----------
        0 = Error
        1 = Ok
    """

    conexion, cursor = conexionDatabase()

    tabla = subdataset_tabla[sds]

    try:
        temporal = NamedTemporaryFile(dir='/tmp')

    except Exception as e:
        print "\nError creando el archivo temporal: %s", e
        return 0

    try:
		comando = ['/usr/local/bin/raster2pgsql','-a','-F','-t','100x100','-s',str(srid),path_imagen,tabla]
		Popen(comando,stdout=temporal).wait()

    except Exception as e:
        print "\nFallo el comando raster2pgsql: %s", e
        conexion.rollback()
        return 0

    try:
        temporal.seek(0)
        cursor.execute(temporal.read()) #ejecuto el script

        cursor.execute(cursor.mogrify('insert into rasters.inventario (imagen, fecha, tabla_destino) values (%s, %s, %s)',\
            (path_imagen, datetime.now(), tabla))) #actualizo el inventario de imagenes

    except Exception as e:
        print "\nFallo ejecutar el script temporal: %s", e

        conexion.rollback()
        return 0

    conexion.commit()
	
    return 1

def selectorTabla (nombre_imagen, tablas):
    """
    Dada una imagen se encarga de dirigirla a la tabla correspondiente
    hay que hacer un descompresor de las imagenes MODIS que le agrege un prefijo que indique el subdataset
    seguido de un punto, por ejemplo:
    ##imagen original
    MOD13Q1.A2009049.h13v12.005.2009068231424.hdf
    ##imagen con ndvi
    NDVI_MOD13Q1.A2009049.h13v12.005.2009068231424.hdf
    ##imagen evi
    EVI_MOD13Q1.A2009049.h13v12.005.2009068231424.hdf
    ##imagen calidad
    QA_MOD13Q1.A2009049.h13v12.005.2009068231424.hdf

    Argumentos
    ----------
    nombre_imagen:nombre de la imagen a procesar

    Devuelve
    ---------
    nombre_tabla: nombre de la tabla donde debe ubicarse la imagen

    """
    #tablas = {'NDVI_MOD13Q1': 'mod13q1_ndvi', 'EVI_MOD13Q1': 'mod13q1_evi', 'QA_MOD13Q1': 'mod13q1_qa'}
    return tablas[os.path.split(nombre_imagen)[1].split('.')[0]]

def obtenerSubdatasets(nombre_imagen, saltear = False):
    """
    Dado un nombre de archivo construye los nombres de los subdatasets correspondientes

    Argumentos
    -----------
    nombre_imagen: nombre de la imagen de la cual se queiren obtener los subdatasets
    subdatasets: iterable con los sufijos de los subdatasets que se quieren cargar en la base de datos tal como aparecen en el archivo
    saltear: flag para que la imagen pase de largo, si esta seteado en True se devolveran como subdatasets el nombre de la imagen tal como esta

    Devuelve
    ----------
    nombres_subdatasets: diccionario con los nombres de los subdatasets que tiene la imagen
    """

    dataset = gdal.Open(nombre_imagen)
    return dataset.GetMetadata('SUBDATASETS')



def executeUpdates (conexion, cursor, tablas):
    """
    Ejecuta los updates de las columnas de fecha y geometria, actualiza los indices

    Argumentos
    ----------
    conexion:
    cursor:

    Devuelve
    ---------
    None
    """

    for tabla in tablas:
        ## actualizar geometrias
        cursor.execute("UPDATE "+tabla+" SET the_geom = st_setsrid(cuad.poligono, "+srid+") FROM (SELECT rid, 'POLYGON(('||ux::varchar||' '||uy::varchar||','||lx::varchar||' '||uy::varchar||','||lx::varchar||' '||ly::varchar||','||ux::varchar||' '||ly::varchar||','||ux::varchar||' '||uy::varchar||'))' As poligono FROM (SELECT rid, (tr.md).upperleftx as ux, (tr.md).upperleftx + (tr.md).scalex * (tr.md).width as lx, (tr.md).upperlefty as uy, (tr.md).upperlefty + (tr.md).scaley * (tr.md).height as ly FROM (SELECT rid, ST_MetaData(rast) As md FROM "+tabla+") As tr) As tab) as cuad WHERE "+tabla+".rid = cuad.rid AND the_geom ISNULL;")
        ## actualizar fechas
        cursor.execute("UPDATE "+tabla+" SET fecha = date ('1-1-'||substring(split_part(filename, '.', 2),2,4)) + (substring(split_part(filename, '.', 2),6,3)::int-1) WHERE fecha ISNULL;")
        conexion.commit()

        return None


def multip_chequearInventario(lista_imagenes, subdataset_tabla, n_process=n_trabajos_paralelos):

	args = ( (ima, subdataset_tabla) for ima in lista_imagenes )
	
	#~ from IPython.Shell import IPShellEmbed; embed = IPShellEmbed(); embed()
	
	#~ """
	## PROCESAMIENTO EN PARALELO:
	cola = multiprocessing.Pool(processes=n_process)
	cola.map_async(chequearInventario, args)
	cola.close()
	cola.join()
	#~ """

	## PROCESAMIENTO SECUENCIAL:
	##map(chequearInventario, args)

########################################################################
########################################################################
########################################################################

if __name__ == '__main__':

	## exporta el pass asi no hay que ponerlo todas las veces
	os.system('export PGPASSWORD=postgres')

	## hay que manejar la opcion d eque reconozca el tipo de imagen, a que se va a cargar (ahora hay que ir cambiando segun sea ndvi o temp y esta mal)

	## listar imagenes
	lista_imagenes = []; lista_imagenes = np.array( [lista_imagenes + listarImagenes(i) for i in ruta_imagenes] ).flatten().tolist()

	## Chequear inventario -> cargar imagenes nuevas
	multip_chequearInventario(lista_imagenes, subdataset_tabla)

	## Ver si conviene paralelizar tambien el executeUpdates()

	"""
	## actualizar columnas de fechas y de indices luego de la carga de datos
	conexion, cursor = conexionDatabase()
	executeUpdates(conexion, cursor, [subdataset_tabla[i] for i in subdataset_tabla])
	"""
