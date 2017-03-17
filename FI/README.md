
# Script de Filtrado/Interpolado

## TODO

## Uso del script de FI dentro de Postgres

### Instalación del script de Filtrado/Interpolado

#### Requerimientos

```bash
apt-get install python-psycopg2 python-scipy postgresql-9.5-plsh
```

#### Instalación de los scripts

Clonar la capeta FI en /var/log/FI
Aplicar los permisos para el usuario postgres

#### Crear una carpeta para los logs

```bash
mkdir /var/log/FI
chown postgres. /var/log/FI
```

#### Configuración de la función de Filtrado/Interpolado en la base de datos

```bash
su postgres -c ‘psql [BASE DE DATOS]’
```

Primero, activamos la extensión para ejecutar comandos de shell en la base determinada
```sql
CREATE EXTENSION plsh;
```

Por precaución eliminamos la función primero, para poder rescribirla en caso de que exista.
```sql
DROP FUNCTION FiltrareInterpolar (esquema text, tabla text, c_afiltrar text);
```

Luego creamos la función en cuestión.
```sql
CREATE FUNCTION FiltrareInterpolar (esquema text, tabla text, c_afiltrar text) RETURNS text
LANGUAGE plsh AS $$
#!/bin/sh
log=/var/log/FI/FI_$1_$2_$3_$(date +%s).log
python /var/lib/postgresql/FI/FI_main.py --esquema $1 --tabla $2 --c_afiltrar $3 > $log
echo "Se guardo el log del filtrado/interpolado en $log"
$$;
```
