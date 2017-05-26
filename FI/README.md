# Script de Filtrado/Interpolado

## Descripción del Algoritmo

El algoritmo se divide en dos pasos: filtrado y luego interpolado.

### Filtrado

1. Se crea las columnas `qmalo` (indexada)
2. Se detectan los pixeles con mala calidad comparando la columna `q` (parametrizable).
3. Se marca como `True` la columna `qmalo` en los pixeles con mala calidad, y `False` en caso contrario.

### Interpolado

Por cada serie distinta (Identificada por `id_pixel`):

1. Preparación
    1. Se crea la columna `x_original`. (donde `x` es la columna a filtrar/interpolar.)
    2. Se copia el valor de `x` a la columna `x_original` en los pixeles malos.
    3. Se crea una columna `x_seinterpolo` para marcar los datos interpolados.

2. Interpolado
    1. Se obtiene la serie completa de la base.
    2. Con los pixeles buenos, se genera una funcion de interpolado (ver [scipy.interpolate.interp1d](https://docs.scipy.org/doc/scipy-0.19.0/reference/generated/scipy.interpolate.interp1d.html))
    3. Se itera por los pixeles malos, intentando interpolar su valor con la función creada.
        - Si falla, deja el valor original.
        - Si se logra, se reemplaza el valor en `x` y se marca como `TRUE` la columna `x_seinterpolo`.

> Importante: Los pixeles malos cuyo valor no pueda interpolarse quedan con el valor original y la columna `x_seinterpolo` como `False` para poder darle otro tratamiento.

## Instalación del script de Filtrado/Interpolado

### Requerimientos

```bash
apt-get install postgresql-9.5-plsh  # Para ejecutar sh desde postgres
apt-get install python3 python3-pip  # Para ejecutar el script de python 
```

### Instalación

Para permitirnos descargar los archivos como `root` pero que los utilice el usuario `postgres`:
```bash
cd /var/lib/postgres
chmod g+s
umask 002
```

Luego descargamos el repositorio (todavía en el home de postgres).
```bash
git clone https://github.com/lartfauba/var_sat
```

Generamos un ambiente para no tener problemas con las versiones de las python y sus modulos.
```
cd var_sat/FI
pip install --requirement requirements.txt
```

También creamos una carpeta para los logs.
```bash
mkdir /var/log/FI
chown postgres. /var/log/FI
```

### Configuración

Nos conectamos a la base donde precisamos la función.

```bash
su postgres -c ‘psql [BASE DE DATOS]’
```

Y desde la consola de SQL, primero activamos la extensión plsh
```sql
CREATE EXTENSION plsh;
```

Luego eliminamos la función, por precaución en caso de que ya exista
```sql
DROP FUNCTION FiltrareInterpolar (esquema text, tabla text, c_afiltrar text);
```

Para finalizar creamos la función en cuestión.
```sql
CREATE FUNCTION FiltrareInterpolar (esquema text, tabla text, c_afiltrar text) RETURNS text
LANGUAGE plsh AS $$
#!/bin/sh
log_folder=/var/log/FI
timelog=$log_folder/timelog.csv  # Estadisticas de ejecuciones
cd /var/lib/postgresql/var_sat/FI
. venv/bin/activate  # No hay source en sh
log_file=$(time --format="%E,%K,%P,%C" --append --output=$timelog ./FI_main.py --esquema $1 --tabla $2 --c_afiltrar $3)
echo "Se guardo el log del filtrado/interpolado en $log_file"
$$;
```

## Uso del script de FI dentro de Postgres

### Preparación

1. Se recomienda que la columna `id_pixel` este indexada.
2. 

### Ejecución del Filtrado/Interpolado

```sql
SELECT FiltrareInterpolar('esquema', 'tabla', 'columna_a_filtrar'); 
```


## TODO

### Fixes

- [ ] No andan los logs...
- [x] Mejorar la documentación
- [ ] Separar las funciones de la base de datos (conexion, ejecucion de sql, creacion de columnas)
- [ ] Incluir la variable de base de datos en el script de plsh (Esta en el environment como PGDATABASE)
- [ ] Utilizar un archivo de configuración para el usuario/clave del script de python (Está hardcodeado)
- [ ] Excluir las series perfectas (sin pixeles malos)

### Features

- [x] Pasar a Python3
- [x] Cambiar la instalación para utilizar un clon del repositorio y un `virtualenv` (Simplifica las actualizaciones)
- [x] Optimizar el algoritmo: Usar una columna booleana e indexada para filtrar
- [x] Optimizar el algoritmo: Copiar los datos se los pixeles malos (Son menos...)
- [x] Interpolar series en paralelo
- [ ] Agregar parametro para el nivel de logging
- [x] Guardar un log de las corridas y tu walltime (Estadisticas de ejecuciones)
- [ ] Usar un threading.manager para monitorear el progreso
