
# Script de Filtrado/Interpolado

## TODO

* Documentar el uso 

## Instalación del script de Filtrado/Interpolado

### Requerimientos

#### Librerías de Python
```bash
apt-get install python-psycopg2 python-scipy
```

#### Extensión de PostgreSQL
```bash
apt-get install postgresql-9.5-plsh
```

### Instalación

#### Descargar los scripts del repositorio

Clonar la capeta FI en /var/log/FI

Aplicar los permisos para el usuario postgres

#### Crear una carpeta para los logs

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

Luego eliminamos la función, por precaución en caso de que exista
```sql
DROP FUNCTION FiltrareInterpolar (esquema text, tabla text, c_afiltrar text);
```

Para finalizar creamos la función en cuestión.
```sql
CREATE FUNCTION FiltrareInterpolar (esquema text, tabla text, c_afiltrar text) RETURNS text
LANGUAGE plsh AS $$
#!/bin/sh
log=/var/log/FI/FI_$1_$2_$3_$(date +%s).log
python /var/lib/postgresql/FI/FI_main.py --esquema $1 --tabla $2 --c_afiltrar $3 > $log
echo "Se guardo el log del filtrado/interpolado en $log"
$$;
```

## Uso del script de FI dentro de Postgres


