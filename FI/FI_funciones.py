#!/usr/bin/env python3
# -*- coding: utf-8 -*-

########################################################################
#
# Filtrador/Intepolador
#
# author:Gonzalo Garcia Accinelli https://github.com/gonzalogacc
#
########################################################################

import utiles

import psycopg2 as pg
from psycopg2.extras import DictCursor
from scipy import interpolate as it
import numpy as np
import multiprocessing

# from IPython import embed  # Debug

args = None
logger = None

dbConn = None
dbCurrs = None


# https://stackoverflow.com/questions/20640840/how-to-efficiently-have-multiproccessing-process-read-immutable-big-data
def worker_init(args):
    global dbConn, dbCurs, logger
    logger = utiles.obtenerLogger('/tmp/FI.log')
    dbConn, dbCurs = conexionBaseDatos(
        args.base, args.usuario, args.clave, args.servidor)


def conexionBaseDatos(database, user, password, host):
    """
    Se conecta a la base de datos y devuelve un cursor que es que se va a usar
    para operar con la base de datos

    Argumentos
    -------------

    Devuelve
    ------------
    """

    conn = pg.connect(database=database, host=host,
                      user=user, password=password)

    cursor = conn.cursor(cursor_factory=DictCursor)

    """ Warning: By default, any query execution, including a simple SELECT will
    start a transaction: for long-running programs, if no further action is
    taken, the session will remain “idle in transaction”, an undesirable
    condition for several reasons (locks are held by the session, tables
    bloat...) For long lived scripts, either ensure to terminate a transaction
    as soon as possible or use an autocommit connection.
    http://initd.org/psycopg/docs/connection.html#connection.autocommit
    """
    conn.set_session(autocommit=True)

    return conn, cursor


def seriesInterpolar(cursor, args):
    """
    Dada una tabla genera una lista de id_pixels que necesitan interpolacion
    (sin repetir)

    Argumentos
    ------------
    cursor: Cursor con el que se van a realizar las consultas
    tabla: Tabla de la cual se quieren hacer las interpolaciones
    columna_pixel: columna que tiene los identificadores de pixel
    columna_calidad: columna que tiene el flag de calidad

    Devuelve
    ------------
    pixels_a_interpolar: lista de identificadores de pixel que se tienen que
    interpolar
    """

    sql = "SELECT DISTINCT {0} FROM {1}.{2} WHERE {3}".format(
        args.c_pixel, args.esquema, args.tabla, args.c_qflag)

    logger.debug("Ejecutando SQL: %s" % sql.rstrip())
    cursor.execute(sql)
    pixels_a_interpolar = cursor.fetchall()
    return pixels_a_interpolar


def interpoladorSerie(cursor, args, pixeles):
    logger.info("%s.%s.%s: Interpolando %d series con %d workers" %
                (args.esquema, args.tabla, args.c_afiltrar,
                 len(pixeles), args.workers))

    c_original = "%s_original" % args.c_afiltrar
    crearColumna(cursor, args.esquema, args.tabla, c_original, "float")
    # Siempre float?

    sql = """
    UPDATE {0}.{1}
    SET {2} = {3}
    WHERE {4}
    AND {2} IS NULL
    """.format(
        args.esquema, args.tabla, c_original, args.c_afiltrar, args.c_qflag)

    try:
        logger.debug("Ejecutando SQL: %s" % sql.rstrip())
        cursor.execute(sql)
        logger.info('%s.%s.%s: Se copio a %s cuando la calidad es mala')
    except Exception as e:
        logger.error("Error: %s" % e.pgerror)

    c_seinterpolo = "%s_seinterpolo" % args.c_afiltrar
    crearColumna(cursor, args.esquema, args.tabla, c_seinterpolo, "boolean")

    sql = """
    UPDATE {0}.{1}
    SET {2} = FALSE
    """.format(args.esquema, args.tabla, c_seinterpolo)
    logger.debug("Ejecutando SQL: %s" % sql.rstrip())
    cursor.execute(sql)
    # logger.info('%s.%s.%s: Se copio a %s cuando la calidad es mala')

    # TODO: Cambiar args por un dict?
    tareas = [(args, c_seinterpolo, i) for i in pixeles]

    if args.workers > 1:  # PROCESAMIENTO EN PARALELO
        logger.debug("Iniciando Pool")
        cola = multiprocessing.Pool(
            processes=args.workers,
            initializer=worker_init,
            initargs=(args,)  # Cada worker va a levantar su propia conexion
        )
        logger.debug("Cargando tareas")
        cola.map_async(_interpoladorSerie, tareas)
        # cola.map_async(fPrueba, argumentos)
        cola.close()
        logger.info("Esperando a que las tareas terminen")
        cola.join()
        logger.info("Terminaron todas las tareas")

    else:  # PROCESAMIENTO SECUENCIAL:
        map(_interpoladorSerie, tareas)


def _interpoladorSerie(tarea):
    """
    Dado un id de pixel genera las interpolaciones necesarias para completar
    la serie de datos y realiza los update de los datos en los lugares
    correspondientes

    Argumentos
    ----------

    Devuelve
    ----------

    """
    args, c_seinterpolo, id_serie = tarea

    sql = """
    SELECT extract(epoch from fecha), {0}, {1}
    FROM {2}.{3}
    WHERE {4} = '{5}'
    ORDER BY fecha
    """.format(
        args.c_ainterpolar, args.c_qflag, args.esquema, args.tabla,
        args.c_pixel, id_serie)

    logger.debug("Ejecutando SQL: %s" % sql.rstrip())
    dbCurs.execute(sql)
    serie_focal = dbCurs.fetchall()

    lista = np.array(serie_focal)
    buenos = lista[lista[:, 2] != True]  # Solo pixeles buenos

    logger.debug("La serie de id_pixel = %d tiene %d pixeles buenos" %
                 (id_serie, len(buenos)))

    if len(buenos) > 2:
        x = buenos[:, 0].astype(int)    # x -> fecha
        y = buenos[:, 1].astype(float)  # y -> c_ainterpolar
        f = it.interp1d(x, y,
                        copy=False,
                        assume_sorted=True  # Lo hice en postgres ya
                        )

        malos = lista[lista[:, 2] == True]  # Solo pixeles malos
        logger.info("La serie de id_pixel = %d tiene %d pixeles malos" %
                    (id_serie, len(malos)))

        for m in malos:
            try:
                interpolado = f(m[0])  # Floor? Int?
                # print dias, dia[0], id_serie, interpolado
            except:
                logger.error("Error interpolando el dia %s de %s" %
                             (m[0], id_serie))
                continue

            sql = """
            UPDATE {0}.{1}
            SET {2} = {3}, {4} = TRUE
            WHERE {4} = '{5}'
            AND fecha = to_timestamp({6})::date+1
            """.format(args.esquema, args.tabla,
                       args.c_ainterpolar, str(interpolado), c_seinterpolo,
                       args.c_pixel, id_serie,
                       m[0])

            try:
                logger.debug("Ejecutando SQL: %s" % sql.rstrip())
                dbCurs.execute(sql)
            except Exception as e:
                logger.error("Error: %s" % e.pgerror)
            # conn.commit()


def crearColumna(cursor, esquema, tabla, columna, tipo, indexar=False):
    sql = """
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = '{0}'
    AND table_name = '{1}'
    AND column_name = '{2}' """.format(esquema, tabla, columna)
    logger.debug("Ejecutando SQL: %s" % sql.rstrip())
    cursor.execute(sql)

    if cursor.fetchone() is None:
        sql = "ALTER TABLE {0}.{1} add column {2} {3}".format(
            esquema, tabla, columna, tipo)
        logger.debug("Ejecutando SQL: %s" % sql.rstrip())
        cursor.execute(sql)
        logger.info('%s.%s: Se creó la columna %s (%s)' %
                    (esquema, tabla, columna, tipo))

        if indexar:
            sql = "CREATE INDEX ON {0}.{1} ({2})".format(
                args.esquema, args.tabla, args.c_qflag)
            logger.debug("Ejecutando SQL: %s" % sql.rstrip())
            cursor.execute(sql)
            logger.info('%s.%s.%s: Se indexó' % (esquema, tabla, columna))

    else:
        logger.warn("%s.%s.%s: Ya existe" % (esquema, tabla, columna))


def filtradoIndice(cursor, args):
    """
    Filtra la tabla que se le pasa como argumento y le agrega las columnas

    Argumentos:
    -------------

    Devuelve:
    -------------

    """
    crearColumna(cursor, args.esquema, args.tabla, args.c_qflag,
                 "boolean", indexar=True)

    # criterios de calidad revisar la documentacion del documento VAR_SAT,
    # consultar Camilo Bagnato
    # update <tabla> set q_flag = 'malo' where q::int & 32768 = 32768 or
    # q::int & 16384 = 16384 or q::int & 1024 = 1024 or q::int & 192 != 64

    sql = """
    UPDATE {0}.{1}
    SET {2} = TRUE
    WHERE {3}::int & 32768 = 32768
    OR {3}::int & 16384 = 16384
    OR {3}::int & 1024 = 1024
    OR {3}::int & 192 != 64
    """.format(args.esquema, args.tabla, args.c_qflag, args.c_calidad)
    logger.debug("Ejecutando SQL: %s" % sql.rstrip())
    cursor.execute(sql)
    logger.info('(%s = TRUE) para los pixeles malos' % args.c_qflag)

    sql = """
    UPDATE {0}.{1}
    SET {2} = FALSE
    WHERE NOT {2}
    """.format(args.esquema, args.tabla, args.c_qflag)
    logger.debug("Ejecutando SQL: %s" % sql.rstrip())
    cursor.execute(sql)
    logger.info('(%s = FALSE) para los pixeles buenos' % args.c_qflag)
