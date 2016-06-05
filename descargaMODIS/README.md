# Descarga Automática de Imágenes MODIS

## TODO

* Documentar inline los parametros internos del script
* Agregar cuenta de las imagenes prexistentes

## Crear una configuracion de descarga

Para crear una configuracion de descarga que el script tome como parametro, crear un archivo de texto con el siguiente formato:

```
[satelite] [producto].[version] [tile1,tile2,...]
```

Ejemplo:

```
MOLT MOD13Q1.005 h13v12,h12v12,h13v13,h12v13,h12v11,h14v14,h11v11,h11v12,h11v10,h12v10,h13v11,h13v09,h13v14
```

## Configurar el Crontab

La búsqueda/descarga se automatiza mediante el crontab.

```
@weekly cd [directorio del script]; ./descarga.sh [archivo de configuracion] >> [archivo de log]
```
