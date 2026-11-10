# Lena EOU

## Captura de regiones de ESI

El workflow **Capture ESI regions** consulta `GET /universe/regions/` cada 12 horas y también se puede lanzar manualmente. Respeta `Expires` y `Cache-Control`; cuando vence la caché, envía `If-None-Match` y aplica `Retry-After` y los límites de error de ESI.

La base `data/esi/uni/regions.db` contiene la tabla de datos `regions(region_id)`, sin nombres ni otros campos por región. Una tabla `metadata` conserva ETag y expiración para respetar la caché entre ejecuciones del workflow.

## Captura de órdenes de mercado por región

El workflow manual **Capture ESI market orders by region** primero obtiene las páginas 1 de las regiones cuya caché local de ESI ya venció. Lee `X-Pages` y reparte las regiones completas entre cuatro workers con balanceo por páginas estimadas (LPT); worker 1 recibe la región más grande. Cada worker procesa sus regiones de más a menos páginas y solicita sus páginas secuencialmente; una región nunca se divide entre workers. Reutiliza la página 1 obtenida en la fase de descubrimiento y verifica que `Last-Modified` se mantenga igual en todas las páginas. Si cambia, reinicia esa región hasta tres intentos, esperando el vencimiento indicado por ESI antes de repetir. Tras el tercer fallo, no actualiza esa región; las demás regiones completadas sí se publican.

Cada región se deduplica por `order_id` y se prepara como SQLite. Cuando terminan los workers se publican las bases completadas en `data/esi/mkt/[region_id].db.zst`, comprimidas con Zstandard nivel 22. Cada base contiene una sola fila en `capture(region_id, last_modified, order_count)` y todas las órdenes en `orders`, con los nombres de campo de ESI. `issued` y `range` se conservan como texto; `is_buy_order` se guarda como entero booleano `0/1`. Una tabla técnica `esi_cache` conserva `Expires` y `X-Pages` para respetar la caché entre ejecuciones.

`data/esi/mkt/headers.json` tiene una sola entrada por región: `last_modified` ISO 8601 UTC de la última página y `last_page` (el `X-Pages` final). Las peticiones incluyen `User-Agent`, respetan `Retry-After` y los límites de error de ESI; las regiones cuya caché siga vigente no se vuelven a solicitar.
