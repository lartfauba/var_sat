#!/bin/bash

# Variables de Configuracion

conf_file=configuracion.sh
if [ ! -f $conf_file ]; then
	echo "Debe crear el archivo $conf_file con la configuracion necesaria para este script."
	exit
fi
source $conf_file

# Variables Generales
servidor=e4ftl01.cr.usgs.gov
tmp=$(mktemp --suffix=`basename $0`)
fecha=$(date +%Y%m%d)
wget_log="$log_dir/logs/wget.$1.$fecha"

if [ $# -ne 1 ]; then
    echo "Debe proveer un archivo de configuración de descarga como argumento"
    exit
fi

if [ ! -f $1 ]; then
    echo "Debe proveer un archivo de configuración de descarga existente!"
    exit
fi

exit

nro=0
while IFS='' read -r line || [[ -n "$line" ]]; do
    nro=$[$nro + 1]

    if [ $(echo $line | wc -w) -ne 4 ]; then
        echo "La linea #$nro del archivo $1 no es una configuracion de descarga correcta:"
        echo $line
        exit
    fi

    satelite=$(echo $line | awk '{ print $1 }')
    producto=$(echo $line | awk '{ print $2 }')
    version=$(echo $line | awk '{ print $3 }')
    escenas=$(echo $line | awk '{ print $4 }')

    date
    echo "Descargando $satelite/$producto.$version/[$escenas]"

    ./cuenta.sh $satelite $producto $version $escenas

    aceptar=""
    for e in $(echo $escenas | sed 's/,/ /g'); do
    	aceptar+="*$e*.hdf,"
    done
    aceptar=${aceptar%?}
    
    echo $aceptar

    echo "Entrando en $directorio"
    cd $directorio
    comando="wget $wget_modo $wget_userpw --no-clobber -r -l2 --no-parent --accept "$aceptar" http://$servidor/$satelite/$producto.$version/"
    echo "$comando &> $wget_log"
    $comando &> $wget_log
    cd -
    
    ./cuenta.sh $satelite $producto $version $escenas

    date
done < "$1"
