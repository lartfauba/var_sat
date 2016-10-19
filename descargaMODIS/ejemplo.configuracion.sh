#!/bin/bash

# Variables de Configuracion

directorio=/directorio/raiz
wget_modo="-nv" # -q: silencioso; -v debug; -nv intermedio
log_dir=/tmp

# A partir de ahora el servidor de la nasa requiere registro
wget_userpw="--user=[USUARIO REGISTRADO] --password=[CLAVE]"
