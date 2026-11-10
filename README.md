# Lena EOU

## Captura de regiones de ESI

El workflow **Capture ESI regions** consulta `GET /universe/regions/` cada 12 horas y también se puede lanzar manualmente. Respeta `Expires` y `Cache-Control`; cuando vence la caché, envía `If-None-Match` y aplica `Retry-After` y los límites de error de ESI.

La base `data/esi/uni/regions.db` contiene la tabla de datos `regions(region_id)`, sin nombres ni otros campos por región. Una tabla `metadata` conserva ETag y expiración para respetar la caché entre ejecuciones del workflow.
