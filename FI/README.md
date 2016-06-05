
# Script de Filtrado/Interpolado

## TODO

* Agregar manejo de log.


## Uso del script de FI dentro de Postgres

### Instalación del script de Filtrado/Interpolado

Clonar la capeta FI en /var/log/FI

### Configuración de la función de Filtrado/Interpolado en la base de datos

```bash
su postgres -c ‘psql [BASE DE DATOS]’
```

```
DROP FUNCTION FiltrareInterpolar (esquema text, tabla text, c_afiltrar text); CREATE FUNCTION FiltrareInterpolar (esquema text, tabla text, c_afiltrar text) RETURNS text
LANGUAGE plsh AS $$
#!/bin/sh
log=/var/log/FI/FI_$1_$2_$3_$(date +%s).log
python /var/lib/postgresql/FI/FI_main.py --esquema $1 --tabla $2 --c_afiltrar $3 > $log
echo "Se guardo el log del filtrado/interpolado en $log"
$$;
```
