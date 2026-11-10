# Lena EOU

## Captura de regiones de ESI

El workflow **Capture ESI regions** consulta `GET /universe/regions/` cada 12 horas y también se puede lanzar manualmente. Respeta `Expires` y `Cache-Control`; cuando vence la caché, envía `If-None-Match` y aplica `Retry-After` y los límites de error de ESI.

La base `data/esi/uni/regions.db` contiene la tabla de datos `regions(region_id)`, sin nombres ni otros campos por región. Una tabla `metadata` conserva ETag y expiración para respetar la caché entre ejecuciones del workflow.

## Captura de órdenes de mercado por región

El workflow manual **Capture ESI market orders by region** primero obtiene las páginas 1 de las regiones que no tienen una copia local dentro de la caché ESI. Lee `X-Pages` y reparte las regiones completas entre cuatro workers con balanceo por páginas estimadas (LPT). Cada worker procesa sus regiones de una en una y solicita las páginas de cada región secuencialmente; una región nunca se divide entre workers. Reutiliza la página 1 obtenida en la fase de descubrimiento y verifica `X-Pages` y `Last-Modified` a través de la paginación.

Cada región se deduplica por `order_id` y se prepara como SQLite; solo después de que terminen los cuatro workers se publican juntas las bases `data/esi/mkt/[region_id].db.zst`. Se comprimen con Zstandard nivel 22. `data/esi/mkt/headers.json` conserva los headers de cada página y el número de páginas de cada región. Las peticiones incluyen `User-Agent`, respetan `Retry-After` y los límites de error de ESI; las capturas locales con caché aún vigente no vuelven a solicitarse.

Las bases guardan todos los campos de las órdenes. Para reducir espacio, `issued` se representa como segundos Unix UTC, `is_buy_order` como `0/1` y `range_code` usa `0=station`, `1=solarsystem`, `2=region`, `3+n=distancia numérica n`. La captura/encabezados conservan `X-Pages` y el número de página para cada respuesta.
