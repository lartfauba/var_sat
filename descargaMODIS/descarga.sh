#!/bin/bash

servidor=e4ftl01.cr.usgs.gov
directorio=/biasatti/raid10a
tmp=/tmp/tmpDescargMODIS
modo="-q"
#modo="-nq" debug

if [ $# -ne 1 ]; then
    echo "Debe proveer un archivo de configuracion como argumento"
    exit
fi

nro=0
while IFS='' read -r line || [[ -n "$line" ]]; do
    nro=$[$nro + 1]

    if [ $(echo $line | wc -w) -ne 3 ]; then
        echo "La linea #$nro del archivo $1 no es una configuracion de descarga correcta:"
        echo $line
        exit
    fi

    satelite=$(echo $line | awk '{ print $1 }')
    producto=$(echo $line | awk '{ print $2 }')
    escenas=$(echo $line | awk '{ print $3 }')

    date
    echo "Descargando $satelite/$producto/[$escenas]"

    ./cuenta.sh $servidor $satelite $producto $escenas

    aceptar=""
    for e in $(echo $escenas | sed 's/,/ /g'); do
        aceptar+="*$e*.hdf,"
    done
    aceptar=${aceptar%?}

    echo $aceptar

    cd $directorio
    wget $modo --no-clobber -r -l2 --no-parent --accept "$aceptar" http://$servidor/$satelite/$producto/
    cd -

    ./cuenta.sh $servidor $satelite $producto $escenas

    date
done < "$1"
