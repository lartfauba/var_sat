# Descarga Automática de Imágenes MODIS

## TODO

* Documentar inline los parametros internos del script
* Agregar cuenta de las imagenes prexistentes


## Crear una configuración para el script en general

Para configurar el directorio de descarga, la verbosidad de la aplicacion de descarga y el directorio para los logs, renombrrrrrrrrrrar el archivo ejemplo.configuracion.sh a configuracion.sh y editar de forma acorde.

El script de descarga.sh carga estas variables al iniciar.

Ver "ejemplo.configuracion.sh"


## Crear una configuracion de descarga de determinadas imágenes

Para crear una configuracion de descarga que el script tome como parametro, crear un archivo de texto con el siguiente formato:

```
[satelite] [producto].[version] [tile1,tile2,...]
```

Ejemplo:

```
MOLT MOD13Q1.005 h13v12,h12v12,h13v13,h12v13,h12v11,h14v14,h11v11,h11v12,h11v10,h12v10,h13v11,h13v09,h13v14
```

Ver "ejemplo.imagenes"

Luego, ejecutar el script descarga.sh con el archivo generado como primer y unico parámetro.

Ejemplo:
```
./descarga.sh ejemplo.imagenes
```


## Configurar el Crontab

La búsqueda/descarga se automatiza facilmente mediante el crontab.

```
@weekly cd [directorio del script]; ./descarga.sh [archivo configuracion de descarga] >> [archivo de log]
```
