# Carga de las imagenes MODIS en la base de datos VAR_SAT

## TODO

* Permitir la carga por tile, tiles o todos.
* Consultar las imagenes del inventario una sola vez (después de cada actualización)
* Permitir cargar varios tiles a la vez
* Detectar automáticamente la proyección de las imagenes
* Desarrollar cuan especifica tiene que ser la ruta a las imagenes

## Instalación

### Requerimientos

Si bien vamos a instalar las librerias de python mediante PIP, necesitamos asegurarnos antes de tener en el sistema ciertas aplicaciones/liberías.

```bash
sudo apt-get install python-dev python-pip # Lo básico para compilar librerias de python 
sudo apt-get install libpq-dev # Para compilar psycopg2
```

#### GDAL 2

A principios del 2016 salió la primera versión estable de la librería GDAL 2. Lamentablemente todavía no está en los repositorios de debian/ubuntu así que hay que compilarla a mano
Armé un script que lo hace: setup-libgdal2.sh. No hace falta correrlo como root pero en un momento utiliza sudo.

#### Virtual Environment

Para instalar las librerias necesarias para el script sin perturbar el resto del sistema, vamos utilizar la libreria virtualenv.

```bash
sudo pip install virtualenv
```

### Instalacion propiamente dicha

```bash
virtualenv venv # Creamos el environment virtual 
pip install gdal psycopg2 numpy # Instalamos las librerias necesarias 
```

## Utilización 

### Parámetros

El script requiere necesariamente ciertos parámetros como el satélite, el producto y el tile a cargar, a la vez de permitir variar otros aspectos predeterminados como el usuario o la base a la que conectarse. Puede obtenerse información más detallada mediante la ejecución del script con el parámetro --help.

EL parámetro más importante es el de --dataset. El script espera un array de JSON donde cada key es el dataset a extraer de la imagen y cargar en la tabla destino, especificada por el valor de esa key.

Ejemplo:

```bash
./dbII_main.py \
--base var_sat_new \
--ruta /imagenes/e4ftl01.cr.usgs.gov \
--satelite MOLT --producto MOD13Q1 --version 006 --tile h13v12
--subdatasets '{"SUBDATASET_1_NAME":"rasters.mod13q1_006_ndvi","SUBDATASET_2_NAME":"rasters.mod13q1_006_evi", "SUBDATASET_3_NAME":"rasters.mod13q1_006_qa"}'
```
Nota: Por cuestiones de python, no es posible cortar la linea de los datasets.
