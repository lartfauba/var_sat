-- Actualizar campo fecha en tablas rasters

-- author:Gonzalo Garcia Accinelli https://github.com/gonzalogacc
-- author:José Clavijo https://github.com/joseclavij/
-- author:David Vinazza https://github.com/dvinazza/

UPDATE rasters.mod13q1_evi
SET fecha =
date ('1-1-'||substring(split_part(filename, '.', 2),2,4))
        + (substring(split_part(filename, '.', 2),6,3)::int-1)
WHERE fecha ISNULL
;


UPDATE rasters.mod13q1_ndvi
SET fecha =
date ('1-1-'||substring(split_part(filename, '.', 2),2,4))
        + (substring(split_part(filename, '.', 2),6,3)::int-1)
WHERE fecha ISNULL
;

-- Calcular columna fecha

UPDATE rasters.mod13q1_qa
SET fecha =
date ('1-1-'||substring(split_part(filename, '.', 2),2,4))
        + (substring(split_part(filename, '.', 2),6,3)::int-1)
WHERE fecha ISNULL
;

VACUUM ANALYZE;
;

