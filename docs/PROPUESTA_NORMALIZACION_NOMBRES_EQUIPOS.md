# Propuesta final: normalización de nombres de equipos (Opción B con control humano)

## Alcance

- **Objetivo:** Un solo nombre canónico por equipo (por liga), para que la lista no muestre duplicados y el análisis reconozca siempre al equipo en la BD.
- **Enfoque:** Detección de variantes + **resolución manual por el admin** (control humano). No se busca escalar: el procedimiento se hace **una vez** para homologar el parque actual; los casos futuros serán **pocos** (las mismas ligas y equipos la mayor parte del tiempo).

---

## Problema que resuelve

Cuando el mismo equipo tiene dos nombres (ej. "Blackburn Rovers FC" y "Blackburn", "1. FSV Mainz 05" y "FSV Mainz 05"):

1. La lista de partidos muestra **dos filas** para el mismo partido.
2. El **análisis** (forma reciente, H2H) solo reconoce una variante en la BD; la otra no encuentra datos.

La normalización une todas las variantes a un **canónico** vía `entity_aliases`, y ese canónico es el que se usa en lista, deduplicación y consultas a historical_matches.

---

## Flujo (Opción B con control humano)

### 1. Detección (una vez, o cuando haya partidos nuevos)

- **Dónde:** Sobre la **lista de partidos** que ya se construye para Partidos del día (y, si se desea, Lista combinada / V2): misma fuente que hoy (`get_matches_by_local_date` + `get_upcoming_matches`), **antes** de normalizar y deduplicar.
- **Qué se detecta:** Pares de filas que son el **mismo partido**:
  - Misma **fecha** (día),
  - Misma **liga** (league_code),
  - Mismo **par de equipos** en contenido: o bien un nombre está contenido en el otro (ej. "Mainz" en "1. FSV Mainz 05"), o son iguales salvo una variante que ya existe en `entity_aliases`. Se consideran “mismo partido” cuando, tras una comparación razonable (ej. normalizar espacios, quitar sufijos tipo "FC"), local con local y visitante con visitante coinciden o uno es variante del otro.
- **Salida:** Lista de **candidatos a alias**: por cada par (nombre_variante_A, nombre_variante_B) en una liga, se registra que “estos dos nombres deben ser el mismo equipo” y se guarda para que el humano elija el canónico.

### 2. Persistencia (BD)

- **Tabla nueva:** `pending_team_aliases` (o reutilizar `data_discrepancies` con `entity_type = 'team_alias'`).
- **Campos mínimos (si tabla nueva):**
  - `league_id` (código liga),
  - `name_variant_1`, `name_variant_2` (los dos nombres detectados para el mismo equipo),
  - `status` ('pending' | 'resolved'),
  - `resolved_canonical` (nombre canónico elegido por el admin, si resuelto),
  - `created_at`, `resolved_at`, `resolved_by`.
- **Regla:** No insertar duplicado: si ya existe un pending con el mismo par (liga, nombre1, nombre2) —en cualquier orden— no volver a insertar. Así el script de detección se puede ejecutar varias veces sin llenar la tabla de repetidos.

### 3. Script de detección (una vez por tanda)

- **Entrada:** Partidos tal como vienen de BD/API (o de la lista pre-normalización).
- **Lógica:**
  - Agrupar por (fecha_día, league_code).
  - Dentro de cada grupo, buscar filas donde el par (local, visitante) sea “el mismo” que otro: por ejemplo mismo fixture_id, o misma fecha+hora y (local similar, visitante similar) con criterio conservador (uno contiene al otro o iguales ignorando mayúsculas/espacios).
  - Para cada par de filas “duplicadas”, extraer los dos nombres de local y los dos de visitante; si difieren, crear un candidato (name_variant_1, name_variant_2, league_id). Inserción en `pending_team_aliases` solo si no existe ya ese par para esa liga.
- **Ejecución:** Script `run_detect_team_name_duplicates.py` (o similar) que lea partidos (por ejemplo de la tabla `matches` o de la misma función que usa la app para “Partidos del día”), ejecute la detección y rellene `pending_team_aliases`. Se ejecuta **una vez** para homologar el parque actual; más adelante solo cuando se añadan partidos/ligas nuevas y se quiera revisar de nuevo.

### 4. UI Admin: control humano

- **Ubicación:** Administración → Verificación de datos, bloque **“Nombres de equipos a normalizar”** (debajo o junto a discrepancias y chatbot).
- **Contenido:**
  - Listado de registros con `status = 'pending'`: liga, nombre variante 1, nombre variante 2.
  - Para cada uno: el admin **elige el nombre canónico** (el que debe quedar en BD y en la UI). Opciones claras: “Usar «Nombre A» como canónico” (entonces B es alias de A) o “Usar «Nombre B» como canónico” (A es alias de B). Un solo botón tipo “Resolver: canónico = X”.
- **Acción al resolver:**
  - Llamar a `add_entity_alias('team', canonical_name, alias_name, league_id)` para el par (canónico, variante).
  - Marcar el registro como `status = 'resolved'`, guardar `resolved_canonical`, `resolved_at`, `resolved_by`.
- **Efecto:** En la siguiente carga de partidos, `normalize_team_name` unifica las dos variantes al canónico elegido, la deduplicación deja una sola fila por partido y el análisis encuentra al equipo en la BD.

### 5. Uso de la normalización (ya existente, solo verificar)

- **Lista de partidos:** Seguir usando `_normalize_and_dedupe_matches`, que ya aplica `normalize_team_name` con `league_id`. Así los nombres que el admin homologó se unifican y se deduplica por (fecha, local_norm, visitante_norm, liga).
- **Contexto para Alfred/Reginald y consultas a historical_matches:** Seguir usando siempre el nombre ya normalizado y `league_id` donde exista, para que forma reciente y H2H usen el mismo canónico que la BD.

No hace falta escalar este flujo: se usa una vez para cerrar el parque actual y ocasionalmente cuando aparezcan pocos casos nuevos (mismas ligas, mismos equipos en el 90% de los casos).

---

## Resumen de implementación

| Pieza | Descripción |
|-------|-------------|
| **BD** | Tabla `pending_team_aliases` (o reutilizar `data_discrepancies` con `entity_type = 'team_alias'`) con: liga, name_variant_1, name_variant_2, status, resolved_canonical, fechas y resolved_by. |
| **Script** | `run_detect_team_name_duplicates.py`: lee partidos (matches / Partidos del día), detecta mismos partidos con distinto nombre de equipo, inserta candidatos en `pending_team_aliases` sin duplicar. |
| **Admin UI** | Bloque “Nombres de equipos a normalizar”: lista pendientes, el admin elige canónico y confirma → `add_entity_alias` + marcar resuelto. |
| **App (lista)** | Sin cambios de flujo: `_normalize_and_dedupe_matches` ya usa `normalize_team_name`; los nuevos aliases pasan a aplicarse en la siguiente carga. |
| **App (análisis)** | Verificar que contexto y consultas a historical usen nombre normalizado y league_id. |

---

## Orden sugerido

1. Crear tabla `pending_team_aliases` (o esquema en `data_discrepancies`) y funciones en `db.py` para listar pendientes, resolver y evitar duplicados.
2. Implementar script de detección que rellene pendientes a partir de partidos actuales.
3. Añadir en la UI de Administración el bloque de “Nombres de equipos a normalizar” y enlazarlo a `add_entity_alias` y al marcado de resuelto.
4. Ejecutar una vez el script, resolver desde la UI todos los pendientes (control humano), y a partir de ahí usar solo cuando haya casos nuevos puntuales.

Esta es la **propuesta final** para la Opción B con control humano: procedimiento puntual, pocos casos futuros, sin requisito de escalabilidad.
