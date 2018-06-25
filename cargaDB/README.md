# Carga de las imagenes MODIS en la base de datos VAR_SAT

## TODO

* Implementar un inventario con mas campos, como fecha de carga
* Separar las funciones de los archivos MODIS/HDF en una libreria aparte
* Simplificar la selección de datasets utilizando un formato predeterminado en el nombre de la tabla destino (ie: modis006.mod13q1_ndvi).
* Consultar las imagenes del inventario una sola vez (después de cada actualización)
* Limpiar las tablas (vacuum&reindex) después de una modificación (ie: eliminación de duplicadas)
* Desarrollar cuan especifica tiene que ser la ruta a las imagenes
* Detectar automáticamente la proyección de las imagenes

## Instalación

### Requerimientos

Si bien vamos a instalar las librerias de python mediante PIP, necesitamos asegurarnos antes de tener en el sistema ciertas aplicaciones/liberías.

```bash
sudo apt-get install python-dev python-pip  # Lo básico para compilar librerias de python 
sudo apt-get install libpq-dev  # Para compilar psycopg2
sudo apt-get install libgdal20 libgdal-dev  # Para compilar gdal
```

#### Virtual Environment

Para instalar las librerias necesarias para el script sin perturbar el resto del sistema, vamos utilizar la libreria virtualenv.

```bash
sudo pip install virtualenv
```

### Instalacion propiamente dicha

```bash
mkdir ~/venvs  # Creo un directorio donde guardar los environments
virtualenv ~/venvs/var_sat --python=$(which python3) # Creamos el environment virtual particular para esto
source ~/venvs/var_sat/bin/activate  # Entramos al environment
pip install gdal psycopg2 numpy ipython  # Instalamos las librerias necesarias 
```

## Utilización 

```bash
source ~/venvs/var_sat/bin/activate  # Entramos al environment
cd var_sat/cargaDB  # Vamos al directorio del script
./dbII_main.py [PARAMETROS]  # Lo ejecutamos con los argumentos precisos
```

### Parámetros

El script requiere necesariamente ciertos parámetros como el satélite, el producto y el tile a cargar, a la vez de permitir variar otros aspectos predeterminados como el usuario o la base a la que conectarse. Puede obtenerse información más detallada mediante la ejecución del script con el parámetro --help.

EL parámetro más importante es el de --dataset. El script espera un array de JSON donde cada key es el dataset a extraer de la imagen y cargar en la tabla destino, especificada por el valor de esa key.

Ejemplo:

```bash
./dbII_main.py \
--base var_sat_new \
--ruta /imagenes/e4ftl01.cr.usgs.gov \
--satelite MOLT --producto MOD13Q1 --version 006 --tile h13v12 \
--subdatasets '{"SUBDATASET_1_NAME":"rasters.mod13q1_006_ndvi","SUBDATASET_2_NAME":"rasters.mod13q1_006_evi", "SUBDATASET_3_NAME":"rasters.mod13q1_006_qa"}'
```
Nota: Por cuestiones de python, no es posible cortar la linea de los datasets.
