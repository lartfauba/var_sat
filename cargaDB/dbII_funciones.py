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

import re
from IPython import embed

from os import walk  # para buscarimagenes

from utiles import obtenerLogger


args = None
logger = None


def conexionBaseDatos(database, user, password, host):
    """
    Se conecta a la base de datos y devuelve un cursor que es que se va a usar
    para operar con la base de datos

    Argumentos
    -------------

    Devuelve
    ------------
    """

    try:
        conn = pg.connect(database=database,
                          user=user, password=password, host=host)
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
    Lista archivos de las imagenes que se encuetran en un arbol de directorios
    indicado y devuelve una lista de las imagenes

    Argumento
    -----------
    path: ruta al directorio del cual se quiere listar las imagenes

    Devuelve
    ---------
    lista_archivos: lista con los path completos a las imagenes del arbol
    de directorios
    """

    lista_archivos = []
    # chequear que sea una imagen sino es lo mismo que usar la otra funcion
    # directamente
    for (path, directorios, archivos) in os.walk(path):
        for archivo in archivos:
            # ~ if archivo.split('.')[-1] in ['hdf', 'HDF']:
            if archivo.split('.')[-1] in ['hdf', 'HDF', 'nc', 'NC']:
                lista_archivos.append(os.path.join(path, archivo))
    return lista_archivos


def buscarImagenes(ruta, satelite, producto, version, tile):
    imagenes = []
    extensiones_validas = ['hdf', 'nc']
    path = "%s/%s/%s.%s" % (ruta, satelite, producto, version)
    logger.info("Buscando %s en %s" % (tile, path))
    for root, dirs, files in walk(path):
        for f in files:
            if tile not in f:
                logger.debug("%s no concuerda con el tile %s" % (f, tile))
                continue
            if f.split('.')[-1].lower() not in extensiones_validas:
                logger.debug(
                    "%s no tiene una extensión válida (%s)" % (f, f[-3:]))
                continue
            imagen = "%s/%s" % (root, f)  # FIXME no es cross-platform
            imagenes.append(imagen)
    logger.info("Encontre %s imagenes" % len(imagenes))
    return sorted(imagenes, reverse=True)  # reversa p/procesar +nuevas primero


def listarInventario():
    """
    Devuelve una lista de las imagenes que ya estan importadas
    """
    conexion, cursor = conexionBaseDatos(args.base,
                                         args.usuario, args.clave,
                                         args.servidor)
    # OJO!!! En multiprocessing no puedo traer la conexion y el cursor como
    # inputs de la funcion listarInventario()
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
        # si hay imagenes las agrega a la variable
        imagenes_inventario = np.array(imagenes_inventario)[:, 0]
    except:
        imagenes_inventario = []

    logger.debug("Encontre %d imagenes de %s %s %s cargadas en la base" %
                 (len(imagenes_inventario),
                  args.producto, args.tile, args.version))
    return imagenes_inventario


def compruebaInventario():
    # conexion, cursor = conexionBaseDatos("var_sat_new",
    #                                      "postgres", "postgres", "10.1.18.24")
    conexion, cursor = conexionBaseDatos(args.base,
                                         args.usuario, args.clave,
                                         args.servidor)

    cursor.execute("SELECT distinct(tabla_destino) FROM rasters.inventario")
    tablas = [t[0] for t in cursor.fetchall()]

    logger.info("El inventario referencia a imagenes en %d tablas" %
                len(tablas))

    for t in tablas:
        cursor.execute("SELECT DISTINCT(filename) FROM %s" % t)
        im_tabla = tuple([i[0] for i in sorted(cursor.fetchall())])

        cursor.execute("""
                       SELECT * FROM rasters.inventario
                       WHERE tabla_destino = %s
                       AND imagen NOT IN %s""", (t, im_tabla,))
        if cursor.rowcount != 0:
            logger.error(
                "%s ERROR: Algunas de las imagenes que referencia el inventario no estan en la tabla." % t)
        else:
            logger.info(
                "%s %d OK: Todas imagenes que referencia el inventario estan en la tabla." % (t, len(im_tabla)))

        # Comprobacion inversa: imagenes en el inventario que
        # no estan en la tabla?
        cursor.execute(""""
                       SELECT imagen
                       FROM rasters.inventario
                       WHERE tabla_destino = '%s'""" % t)
        im_inventario = tuple([i[0] for i in sorted(cursor.fetchall())])

        cursor.execute("""
                       SELECT *
                       FROM {0}
                       WHERE filename NOT IN %s""".format(t), (im_inventario,))
        if cursor.rowcount != 0:
            logger.error(
                "%s ERROR: Algunas de las imagenes no estan inventariadas." % t)
        else:
            logger.info(
                "%s %d OK: Todas las imagenes de la tabla estan en el inventario" % (t, len(im_tabla)))


def limpiaDuplicadas():
    conexion, cursor = conexionBaseDatos(args.base,
                                         args.usuario, args.clave,
                                         args.servidor)

    sql = """
        SELECT
            SUBSTRING(imagen FROM '"(.*).{14}.hdf') as pdtv,
            SUBSTRING(imagen FROM '(MODIS.*)') as dataset
        FROM rasters.inventario
        WHERE imagen LIKE '%MODIS%'
        AND imagen LIKE '%.""" + args.version + """.%'
        GROUP BY pdtv, dataset
        HAVING count(*) > 1"""

    cursor.execute(sql)
    duplicadas = cursor.fetchall()

    logger.error("Encontre %d imagenes duplicadas" % len(duplicadas))

    for imagen in duplicadas:
        # busco fecha de procesamiento mas reciente de la imagen
        sql = """
        SELECT
            (SUBSTRING(imagen FROM '(.{{13}}).hdf'))::bigint as procesamiento
        FROM rasters.inventario
        WHERE imagen LIKE '%{0}%{1}%'
        ORDER BY procesamiento DESC
        """.format(imagen[0], imagen[1])

        logger.debug(sql)
        cursor.execute(sql)
        mas_reciente = cursor.fetchone()

        logger.warn(
            "%s %s Eliminando procesamientos anteriores a %d" % (imagen[0], imagen[1], mas_reciente[0]))

        sql = """
            SELECT
                imagen,
                tabla_destino
            FROM rasters.inventario
            WHERE
                imagen LIKE '%{0}%{1}%'
                AND (SUBSTRING(imagen FROM '(.{{13}}).hdf'))::bigint != '{2}'
        """.format(imagen[0], imagen[1], mas_reciente[0])

        logger.debug(sql)
        cursor.execute(sql)
        deprecadas = cursor.fetchall()

        for im in deprecadas:
            sql = """
                DELETE FROM {1}
                WHERE filename = '{0}'
            """.format(im[0], im[1])
            logger.debug(sql)
            cursor.execute(sql)

            sql = """
                DELETE FROM rasters.inventario
                WHERE imagen = '{0}'
                AND tabla_destino = '{1}'
            """.format(im[0], im[1])
            logger.debug(sql)
            cursor.execute(sql)

    conexion.commit()


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
                logger.debug('%s: ya esta cargada en la base de datos' %(subdatasets[key]))
                continue


            # TODO: manejar distintas fechas de procesamiento NASA para una misma scena
            if chequearDuplicada(subdatasets[key]):
                continue

            logger.info('%s: no esta cargada y se va a cargar' %(subdatasets[key]))

            if importarImagen(base_path, subdatasets[key], subdataset_tabla[key]) != 0:
                logger.warn('%s: se cargo correctamente' %(subdatasets[key]))
            else:
                logger.error('%s: no se cargo' %(subdatasets[key]))
    except Exception as e:
        logger.error("Error: %s", e)

    return


def chequearDuplicada(imagen):
    # Extraigo la fecha de procesamiento de la imagen
    busqueda = re.search('{0}.(.*).hdf'.format(args.version), imagen)
    proc_imagen = busqueda.group(1)  # La dejo como STR, la convierto más tarde

    # Genero una expresion para buscarla en postgres
    wildcard = imagen.replace(proc_imagen, '%')

    sql = """
                SELECT (regexp_matches(imagen, '{0}.(.*).hdf'))[1]::bigint AS procesamiento
                FROM rasters.inventario
                WHERE imagen like '{1}'
                ORDER BY procesamiento DESC
                LIMIT 1
    """.format(args.version, wildcard)
    logger.debug(sql)

    conexion, cursor = conexionBaseDatos(args.base, args.usuario, args.clave, args.servidor)
    cursor.execute(sql.format(args.version, wildcard))

    if cursor.rowcount != 0:
        proc_db = cursor.fetchone()[0]
        if proc_db > int(proc_imagen):
            logger.warn('%s: la base de datos contiene una version mas reciente (%d)' % (imagen, proc_db))
            return 1

    return 0


def importarImagen(base_path, dataset, tabla, dryrun=None):
    """
    Dado el path a una imagen la importa en la base de datos en la tabla correspondiente

    Argumentos
    -----------
    base_path:  path a la imagen que se quiere cargar en la base de datos
    dataset:        dataset a cargar
    tabla:      tabla destino de la carga

    Devuelve
    ----------
        0 = Error
        1 = Ok
    """
    if dryrun is None:
        dryrun = args.dryrun

    raster2pgsql = "/usr/bin/raster2pgsql"

    # conexion, cursor = conexionDatabase()
    conexion, cursor = conexionBaseDatos(args.base, args.usuario, args.clave, args.servidor)

    try:
        temporal = NamedTemporaryFile(dir='/tmp')
    except Exception as e:
        logger.error("Error creando el archivo temporal: %s" % e)
        return 0

    try:
        comando = [raster2pgsql,'-a','-F','-t','100x100','-s', args.srid, dataset, tabla]
        p = Popen(comando,cwd=base_path,stdout=temporal)
        p.wait()
    except Exception as e:
        logger.error("Fallo %s %d %s" % comando, p.returncode, e)
        return 0

    try:
        temporal.seek(0)
        if not dryrun:
            cursor.execute(temporal.read()) #ejecuto el script
            sql = """
                INSERT INTO rasters.inventario (imagen, fecha, tabla_destino)
                VALUES ('%s','%s', '%s')
                """ % (dataset, datetime.now(), tabla)
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
    print(archivo)
    for key, value in ds.iteritems():
        print("%s\t%s" % (key, value.replace(archivo,'')))


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

    if workers > 1: # PROCESAMIENTO EN PARALELO
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
    else: # PROCESAMIENTO SECUENCIAL:
        map(_chequearInventario, argumentos)


def chequearTablas(tablas):

    conexion, cursor = conexionBaseDatos(args.base, args.usuario, args.clave, args.servidor)

    for t in tablas:
        esquema, tabla = t.split('.')

        cursor.execute("""
                        SELECT exists(
                            SELECT *
                            FROM information_schema.tables
                            WHERE table_schema=%s AND table_name=%s)""", (esquema, tabla,))

        if cursor.fetchone()[0] is False:
            sql = """
                CREATE TABLE {0}.{1} (
                      rid serial NOT NULL,
                      rast raster,
                      filename text,
                      the_geom geometry(Geometry,{2}),
                      fecha date,
                      CONSTRAINT {1}_pkey PRIMARY KEY (rid)
                ) WITH ( OIDS=FALSE )""".format(esquema, tabla, args.srid)
            cursor.execute(sql)
            conexion.commit()
