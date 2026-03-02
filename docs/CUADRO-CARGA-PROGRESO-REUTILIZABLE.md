# Cuadro de carga con progreso reutilizable

## Qué es el "cuadro de carga"

Overlay que tapa la pantalla (fondo oscuro semitransparente) y modal centrado con:

- **Título y descripción**
- **Barra de progreso** (ancho = percent %)
- **Texto** tipo "X de Y (Z%)" y **tiempo transcurrido**
- **Mensaje final** al terminar
- **Log de actividad**: líneas que envía el servidor (stream o polling)

## Estructura de datos (estado)

Un solo objeto controla todo:

| Campo         | Tipo     | Uso                                              |
|---------------|----------|--------------------------------------------------|
| `active`      | boolean  | Si el overlay se muestra                         |
| `processed`   | number   | Pasos completados                                |
| `total`       | number   | Total de pasos                                   |
| `percent`     | number   | 0–100 para la barra                              |
| `elapsed`     | number   | Segundos transcurridos                            |
| `message`     | string   | Mensaje final (éxito/error)                       |
| `activityLog` | string[] | Cada línea del log                               |

## Dos formas de recibir el progreso

### Stream (NDJSON)

El backend hace POST y responde con un flujo de líneas; cada línea es un JSON con `processed`, `total`, `percent`, `elapsed`, `log`, y al final uno con `done: true` y `message`.

En el frontend: `fetch` → `res.body.getReader()` → leer por chunks, partiendo por `\n`, parsear cada línea con `JSON.parse` y actualizar el estado. Si llega `done: true`, se pone `active = false` y se muestra el mensaje.

### Modo asíncrono (job + polling)

El backend responde rápido con `{ job_id: "..." }`. El frontend cada 2 s llama a `GET /api/.../status?job_id=...` y recibe el mismo tipo de datos (`processed`, `total`, `percent`, `log`, `done`, `message`). Cuando `done === true`, se deja de hacer polling, se cierra el overlay y se muestra el mensaje.

## Implementación en esta app (Streamlit)

En esta app no hay overlay real; se simula con:

- Un **contenedor** con estilo tipo “modal” (fondo oscuro, borde naranja, sombra).
- **Barra**: `st.progress(percent)`.
- **Mensaje de paso**: `st.caption("Paso X de Y — ...")`.
- **Tiempo**: `st.caption("⏱ Tiempo transcurrido: N s")`.
- **Log**: lista en sesión `activity_log`; cada paso hace `append` y se re-renderiza un `st.caption` por línea.

El botón **"🔥 Dale con todo!"** ejecuta en una sola corrida: Paso 1 (Alfred) → Paso 2 (Reginald) → Paso 3 (Propuesta General 1+2), actualizando progreso y log entre llamadas. Al terminar se hace `st.rerun()` para mostrar resultados.

Para reutilizar en Vue/React o vanilla JS: usar la misma estructura de estado y, según el backend, conectar por **stream NDJSON** o **polling** como arriba.
