#!/usr/bin/env python
# -*- coding: utf-8 -*-

########################################################################
#
# Gestion de carga de imagenes a la base de datos
#
# author:Gonzalo Garcia Accinelli https://github.com/gonzalogacc
# author:José Clavijo https://github.com/joseclavij/
# author:David Vinazza https://github.com/dvinazza/
#
########################################################################

# IMPORTANTE: ejecutar:
# python dbImageImport_...py > nombre_archivo_de_texto.log
# ... para guardar alertas y mensajes de error en el archivo nombre_archivo_de_texto.log

import os
import psycopg2 as pg
from psycopg2.extras import DictCursor
from datetime import datetime
import numpy as np
from osgeo import gdal

from tempfile import NamedTemporaryFile
from subprocess import Popen

import multiprocessing

from os import walk  # para buscarimagenes

from utiles import obtenerLogger

args = None
logger = None

def conexionBaseDatos(database, user, password, host):
    """
    Se conecta a la base de datos y devuelve un cursor que es que se va a usar para operar con la base de datos

    Argumentos
    -------------

    Devuelve
    ------------
    """

    try:
        conn = pg.connect(database=database, user=user, password=password, host=host)
    except Exception as e:
        logger.error("%s", e)
        raise e

    try:
        cursor = conn.cursor(cursor_factory=DictCursor)
    except Exception as e:
        logger.error("%s", e)
        raise e

    return conn, cursor


def listarImagenes(path):
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


def buscarImagenes(ruta, satelite, producto, version, tile):
    imagenes = []
    extensiones_validas = ['hdf','nc']
    path = "%s/%s/%s.%s" % (ruta, satelite, producto, version)
    logger.info("Buscando %s en %s" % (tile, path))
    for root, dirs, files in walk(path):
        for f in files:
            if tile not in f:
                logger.debug("%s no concuerda con el tile %s" % (f, tile))
                continue
            if f.split('.')[-1].lower() not in extensiones_validas:
                logger.debug("%s no tiene una extensión válida (%s)" % (f, f[-3:]))
                continue
            imagen = "%s/%s" % (root, f) # FIXME no es cross-platform
            imagenes.append(imagen)
    logger.info("Encontre %s imagenes" % len(imagenes))
    return sorted(imagenes, reverse=True) # en reversa para procesar las mas nuevas primero


def listarInventario():
    """
    Devuelve una lista de las imagenes que ya estan importadas
    """
    conexion, cursor = conexionBaseDatos(args.base, args.usuario, args.clave, args.servidor)
    # OJO!!! En multiprocessing no puedo traer la conexion y el cursor como inputs de la funcion listarInventario()
    #            -> Necesita crear las instancias aqui adentro
    # dvz | Ver -> http://initd.org/psycopg/docs/usage.html#thread-safety

    sql = """
        SELECT imagen FROM rasters.inventario
        WHERE imagen like '%{0}%'
        AND imagen like '%{1}%'
        AND imagen like '%.{2}.%'
    """.format(args.producto, args.tile, args.version)
    cursor.execute(sql)
    imagenes_inventario = cursor.fetchall()

    try:
        ## si hay imagenes las agrega a la variable
        imagenes_inventario = np.array(imagenes_inventario)[:,0]
    except:
        imagenes_inventario = []

    logger.info("Encontre %d imagenes de %s %s %s cargadas en la base" % (len(imagenes_inventario), args.producto, args.tile, args.version))
    return imagenes_inventario


def _chequearInventario(lista_argumentos):
    """
    Consulta la tabla en la base de datos que tiene la lista de imagenes cargadas en el servidor

    Argumentos
    -----------

    Devuelve
    ---------
    """
    imagen = lista_argumentos[0]
    subdataset_tabla = lista_argumentos[1]

    # TODO: Hace falta consultar el inventario por cada imagen a cargar?
    # quiza podemos hacer un objeto con un lock para actualizarlo
    # cada vez que se carga una imagen nueva. si no hay cargas, no hay cambios
    imagenes_inventario = listarInventario()

    ## se queda solo con el nombre y no con el path
    base_path, nombre_imagen = os.path.split(imagen)
    os.chdir(base_path)

    subdatasets = obtenerSubdatasets(nombre_imagen)

    # Si es un TIFF o cualquier imagen in subdatasets, utiliza solo el nombre del archivo:
    if subdatasets == dict({}): subdatasets[''] = nombre_imagen

    try:
        for key in subdatasets.keys():
            if key not in subdataset_tabla.keys():
                logger.debug('%s: no fue seleccionada por el usuario' %(subdatasets[key]))
                continue

            if subdatasets[key] in imagenes_inventario:
                # TODO: manejar distintas fechas de procesamiento NASA para una misma scena
                logger.debug('%s: ya esta cargada en la base de datos' %(subdatasets[key]))
                continue

            logger.info('%s: no esta cargada y se va a cargar' %(subdatasets[key]))

            if importarImagen(subdatasets[key], subdataset_tabla, key) != 0:
                logger.warn('%s: se cargo correctamente' %(subdatasets[key]))
            else:
                logger.error('%s: no se cargo' %(subdatasets[key]))
    except Exception as e:
        print "Error: %s", e

    return

def importarImagen(path_imagen, subdataset_tabla, sds, dryrun=None):
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
    if dryrun is None:
	dryrun = args.dryrun


    # conexion, cursor = conexionDatabase()
    conexion, cursor = conexionBaseDatos(args.base, args.usuario, args.clave, args.servidor)
    tabla = subdataset_tabla[sds]

    # FIXME esto es un fix horrible por el path a las imagenes.
    # hay que ver si se puede sacar y usar siempre rutas absolutas
    i = imagen.split(':')
    imgDS = ":".join([ i[0], i[1], "%s/%s" % (path, i[2]), i[3], i[4] ])

    try:
        # print "Creando un archivo temporal"
        temporal = NamedTemporaryFile(dir='/tmp')
    except Exception as e:
        logger.error("Error creando el archivo temporal: %s" % e)
        return 0

    try:
        raster2pgsql = "/usr/bin/raster2pgsql"
        comando = [raster2pgsql,'-a','-F','-t','100x100','-s', args.srid, imgDS, tabla]
        Popen(comando,stdout=temporal).wait()
    except Exception as e:
        logger.error("Fallo el comando raster2pgsql: %s" % e)
        return 0

    try:
        temporal.seek(0)
        if not dryrun:
            cursor.execute(temporal.read()) #ejecuto el script
            sql = """
                INSERT INTO rasters.inventario (imagen, fecha, tabla_destino)
                VALUES ('%s','%s', '%s')
                """ % (path_imagen, datetime.now(), tabla)
            cursor.execute(sql) #actualizo el inventario de imagenes

    except Exception as e:
        logger.error("Fallo ejecutar el script temporal: %s" % e)

        if not dryrun:
            conexion.rollback()
        return 0

    conexion.commit()
    temporal.close()

    return 1

def selectorTabla(nombre_imagen, tablas):
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
    # TODO hay terminar de implementar esto para poder usar solo el dataset en
    # el argumento en lugar del dict con el dataset y la tabla destino
    #tablas = {'NDVI_MOD13Q1': 'mod13q1_ndvi', 'EVI_MOD13Q1': 'mod13q1_evi', 'QA_MOD13Q1': 'mod13q1_qa'}
    return tablas[os.path.split(nombre_imagen)[1].split('.')[0]]

def obtenerSubdatasets(nombre_imagen, saltear=False):
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


def verDatasets(archivo):
    ds = obtenerSubdatasets(archivo)
    print archivo
    for key, value in ds.iteritems():
        print "%s\t%s" % (key, value.replace(archivo,''))


def executeUpdates(tablas,srid):
    """
    Ejecuta los updates de las columnas de fecha y geometria, actualiza los indices

    Argumentos
    ----------

    Devuelve
    ---------
    None
    """
    # TODO deducir el srid mediante postgres
    conexion, cursor = conexionBaseDatos(args.base, args.usuario, args.clave, args.servidor)

    sql_geom = """
    UPDATE {0}
    SET the_geom = st_setsrid(cuad.poligono, {1})
    FROM (
        SELECT  rid,
                'POLYGON(('||ux::varchar||' '||uy::varchar||','||lx::varchar||' '||uy::varchar||','||lx::varchar||' '||ly::varchar||','||ux::varchar||' '||ly::varchar||','||ux::varchar||' '||uy::varchar||'))' as poligono
        FROM (
            SELECT  rid,
                    (tr.md).upperleftx as ux,
                    (tr.md).upperleftx + (tr.md).scalex * (tr.md).width as lx,
                    (tr.md).upperlefty as uy,
                    (tr.md).upperlefty + (tr.md).scaley * (tr.md).height as ly
            FROM (
                SELECT rid, ST_MetaData(rast) As md FROM {0}
            ) as tr
        ) as tab
    ) as cuad
    WHERE
        {0}.rid = cuad.rid
        AND the_geom ISNULL """

    sql_fechas = """
    UPDATE {0}
    SET fecha = date ('1-1-'||substring(split_part(filename, '.', 2),2,4)) + (substring(split_part(filename, '.', 2),6,3)::int-1)
    WHERE fecha ISNULL """

    for tabla in tablas:
        try:
            logger.info("Actualizando la columna de geometrias de %s (%s)" % (tabla,srid))
            cursor.execute(sql_geom.format(tabla, srid))
        except Exception as e:
            logger.error("Error al actualizar la columna de geometrías: %s" % e)
            logger.debug("SQL: %s" % sql_geom.format(tabla,srid))
            continue

        try:
            logger.info("Actualizando la de fechas de %s (%s)" % (tabla, srid))
            cursor.execute(sql_fechas.format(tabla))
        except Exception as e:
            logger.error("Error al actualizar la columna de fechas: %s" % e)
            logger.debug("SQL: %s" % sql_fechas.format(tabla))
            continue

        conexion.commit()

    return None


def chequearInventario(imagenes, subdatasets, workers=1):
    logger.info("Preparandose para procesar %d imagenes, %d datasets con %d workers" % (len(imagenes), len(subdatasets), workers))
    argumentos=[(i, subdatasets) for i in imagenes]

    # PROCESAMIENTO EN PARALELO
    logger.debug("Iniciando Pool")
    cola = multiprocessing.Pool(processes=workers)
    logger.debug("Cargando tareas")
    cola.map_async(_chequearInventario, argumentos)
    #cola.map_async(fPrueba, argumentos)
    cola.close()
    logger.info("Esperando a que las tareas terminen")
    cola.join()
    logger.info("Terminaron todas las tareas")
    #~ """

    ## PROCESAMIENTO SECUENCIAL:
    ##map(chequearInventario, args)
