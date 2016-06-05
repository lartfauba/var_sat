#!/usr/bin/python
# -*- coding: utf-8 -*- 

import psycopg2 as pg
from psycopg2.extras import DictCursor
from scipy import interpolate as it
import numpy as np


def conexionBaseDatos(database, user, password, host):
	"""
	Se conecta a la base de datos y devuelve un cursor que es que se va a usar para operar con la base de datos
	
	Argumentos
	-------------
	
	Devuelve
	------------
	"""
	
	conn = pg.connect(database=database, user=user, password=password, host=host)
	cursor = conn.cursor(cursor_factory=DictCursor)
	
	return conn, cursor


def seriesInterpolar (cursor, esquema, tabla, c_pixel, c_qflag):
    """
	Dada una tabla genera una lista de id_pixels que necesitan interpolacion (sin repetir)
	
	Argumentos
	------------
    cursor: Cursor con el que se van a realizar las consultas
    tabla: Tabla de la cual se quieren hacer las interpolaciones
    columna_pixel: columna que tiene los identificadores de pixel
    columna_calidad: columna que tiene el flag de calidad

	Devuelve
	------------
    pixels_a_interpolar: lista de identificadores de pixel que se tienen que interpolar

	"""
    sql = "SELECT DISTINCT {0} FROM {1}.{2} WHERE {3} = 'malo'".format(c_pixel, esquema, tabla, c_qflag)
    #cursor.execute("select distinct "+columna_pixel+" from "+tabla+" where "+columna_flag_calidad+" = 'malo';")
    cursor.execute(sql)
    pixels_a_interpolar = cursor.fetchall()
    return pixels_a_interpolar
	
def interpoladorSerie (conn, cursor, esquema, tabla, c_filtrado, c_pixel, id_serie):
    """
    Dado un id de pixel genera las interpolaciones necesarias para completar la serie de datos
    y realiza los update de los datos en los lugares correspondientes

    Argumentos
    ----------

    Devuelve
    ----------

    """
    sql = """	SELECT extract(epoch from fecha), {0}, q_flag
		FROM {1}.{2}
		WHERE {3} = '{4}'""".format(c_filtrado, esquema, tabla, c_pixel, id_serie)

    cursor.execute(sql)
    serie_focal = cursor.fetchall()

    lista = np.array(serie_focal)
    l_lista = lista[lista[:,2] != 'malo']

    if len(l_lista) > 2:
        s_lista = l_lista[l_lista[:,0].argsort()]

        x = s_lista[:,0]
        y = s_lista[:,1]
        f = it.interp1d(x, y)

        dias = lista[lista[:,2] == 'malo']

        for dia in dias:
            try:
                interpolado = f(dia[0]) 
                #print dias, dia[0], id_serie, interpolado
            except:
                interpolado = -9999

		

	    sql = """	UPDATE {0}.{1}
			SET {2} = {3}
			WHERE {4} = '{5}'
			AND fecha = to_timestamp({6})::date+1
			""".format(esquema, tabla, c_filtrado, str(interpolado), c_pixel, id_serie, dia[0])

	    try:
		cursor.execute(sql)
    	    except Exception, e:
		print sql
		print e.pgerror
		
            #conn.commit()

def filtradoIndice (cursor, esquema, tabla, c_afiltrar, c_calidad):
    """
    Filtra la tabla que se le pasa como argumento y le agrega las columnas

    Argumentos:
    -------------

    Devuelve:
    -------------

    """

    c_filtrado = "%s_filtrado" % c_afiltrar
    c_qflag = 'q_flag'
    ## SECUENCIA DE PASOS NECESARIA PARA GENERAR UNA SERIE FILTRADA, HAY QUE PASARLO A CODIGO PYTHON ASI LO INTEGRO AL PROGRAMA ##
    ## Cosas que hay que correr para preparar la tabla para interpolarla
    ## agrego la columna de flag de calidad y se pone malo donde el filtro deteta un mal valor
    ## alter table <tabla> add column q_flag varchar;

    sql = """	SELECT 1 FROM information_schema.columns
		WHERE table_schema = '{0}'
		AND table_name = '{1}'
		AND column_name = '{2}' """.format(esquema, tabla, c_qflag)
    cursor.execute(sql)
    if cursor.fetchone() is None:
	sql = "ALTER TABLE {0}.{1} add column {2} varchar".format(esquema, tabla, c_qflag)
	cursor.execute(sql)
        print 'Se creo la columna de flag de calidad'


    ## criterios de calidad revisar la documentacion del documento VAR_SAT, consultar Camilo Bagnato
    ## update <tabla> set q_flag = 'malo' where q::int & 32768 = 32768 or q::int & 16384 = 16384 or q::int & 1024 = 1024 or q::int & 192 != 64;a

    sql = """	UPDATE {0}.{1}
		SET {2} = 'malo'
		WHERE {3}::int & 32768 = 32768
		OR {3}::int & 16384 = 16384
		OR {3}::int & 1024 = 1024
		OR {3}::int & 192 != 64 """.format(esquema, tabla, c_qflag, c_calidad)

    #cursor.execute("UPDATE "+ tabla +" set "+ columna_flag_calidad +" = 'malo' WHERE "+columna_calidad+"::int & 32768 = 32768 OR "+columna_calidad+"::int & 16384 = 16384 OR "+columna_calidad+"::int & 1024 = 1024 OR "+columna_calidad+"::int & 192 != 64;")

    ## crear una columna iv_filtrado
    ## alter table <tabla> add column evi_filtrado float;_flag_calidad
    sql = """	SELECT 1 FROM information_schema.columns
		WHERE table_schema = '{0}'
		AND table_name = '{1}'
		AND column_name = '{2}' """.format(esquema, tabla, c_filtrado)
    cursor.execute(sql)
    if cursor.fetchone() is None:
	sql = "ALTER TABLE {0}.{1} ADD COLUMN {2} float".format(esquema, tabla, c_filtrado)
	cursor.execute(sql)
        print 'Se creo la columna de indice filtrado'

    ## copiar las filas que no tengan valor 'malo'
    ## update <tabla> set <iv>_filtrado = <iv> where q_flag is null;

    sql = """	UPDATE {0}.{1}
		SET {2} = {3}
		WHERE {4} is null """.format(esquema, tabla, c_filtrado, c_afiltrar, c_qflag)
    try:
    	cursor.execute(sql)
    except Exception, e:
	print sql
	print e.pgerror

    return c_filtrado, c_qflag
