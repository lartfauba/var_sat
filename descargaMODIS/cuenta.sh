#!/bin/bash

# Variables de Configuracion
conf_file=configuracion.sh
if [ ! -f $conf_file ]; then
	echo "Debe crear el archivo $conf_file con la configuracion necesaria para este script."
	exit
fi
source $conf_file


# Variables Generales
s=","
servidor=e4ftl01.cr.usgs.gov
tmp=$(mktemp --suffix=`basename $0`)

if [ $# -ne 4 ]; then 
	echo "Usar $0 satelite producto version escenas"
	exit
fi

satelite=$1
producto=$2
version=$3
escenas=$4

d="$directorio/$servidor/$satelite/$producto.$version"
if [ ! -d $d ]; then
	echo "El directorio esperado ($d) no existe."
	exit
fi

cd $d

find -name *.hdf > $tmp

echo -e "año$s\c" 
for e in $(echo $escenas | sed 's/,/ /g'); do
	echo -e "$e$s\c"
done
echo

for i in $(seq -w 00 $(date +%y)); do  # Valido hasta el 2099
	echo -e "20$i$s\c"
	for e in $(echo $escenas | sed 's/,/ /g'); do
	    echo -e "$(cat $tmp | grep A20$i | grep $e | wc -l)$s\c"
	done
	echo
done
