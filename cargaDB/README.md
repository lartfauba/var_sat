# Carga de las imagenes MODIS en la base de datos VAR_SAT

## TODO

* Consultar las imagenes del inventario una sola vez (después de cada actualización)
* Detectar automáticamente la proyección de las imagenes

## Instalación

### Requerimientos

Si bien vamos a instalar las librerias de python mediante PIP, necesitamos asegurarnos antes de tener en el sistema ciertas aplicaciones/liberías.

sudo apt-get install python-dev python-pip # Lo básico para compilar librerias de python 
sudo apt-get install libpq-dev # Para compilar psycopg2

#### GDAL 2

A principios del 2016 salió la primera versión estable de la librería GDAL 2. Lamentablemente todavía no está en los repositorios de debian/ubuntu así que hay que compilarla a mano
Armé un script que lo hace: setup-libgdal2.sh. No hace falta correrlo como root pero en un momento utiliza sudo.

#### Virtual Environment

Para instalar las librerias necesarias para el script sin perturbar el resto del sistema, vamos utilizar la libreria virtualenv.

sudo pip install virtualenv 

### Instalacion propiamente dicha

virtualenv venv # Creamos el environment virtual 
pip install gdal psycopg2 numpy # Instalamos las librerias necesarias 

## Utilización 

source venv/bin/activate 

./dbII_main.py --satelite MOLT --producto MOD13Q1 --version 006 --subdatasets '{"SUBDATASET_1_NAME":"rasters.mod13q1_006_ndvi","SUBDATASET_2_NAME":"rasters.mod13q1_006_evi", "SUBDATASET_3_NAME":"rasters.mod13q1_006_qa"}' --tile h13v12
