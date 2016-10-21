#!/bin/bash 

# Utilizo CASI todos los procesadores para compilar
cpus=`nproc`
if [ $cpus -gt 1 ]; then
    cpus=$((`nproc`-1))
fi

cd /tmp
wget http://download.osgeo.org/gdal/2.1.1/gdal211.zip
unzip gdal211.zip 
cd gdal-2.1.1
./configure prefix=/usr/
make -j $cpus 
sudo make install -j $cpus
