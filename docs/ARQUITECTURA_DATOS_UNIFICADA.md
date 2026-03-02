# Arquitectura de Datos Unificada – ForgeWin

**Objetivo:** Una sola base de datos como fuente de verdad. Las APIs (football-data.co.uk, API-Sports) completan la tabla principal. Las discrepancias se subsanan manualmente vía UI de admin.

---

## 1. Modelo de Datos

### 1.1 Tabla principal: `historical_matches`

Se mantiene como única fuente para partidos históricos. Se añaden columnas de control:

| Columna nueva | Tipo | Descripción |
|---------------|------|-------------|
| `data_source` | TEXT | Origen del registro: `football_data_csv`, `api_sports`, `manual` |
| `validation_status` | TEXT | `pending`, `confirmed`, `disputed`, `manual_override` |
| `validated_at` | TEXT | ISO timestamp de última validación |
| `validated_by` | INTEGER | user_id del admin que validó (NULL si automático) |
| `override_reason` | TEXT | Comentario cuando `validation_status = manual_override` |

**Convención única de temporada:** `season` = año fin (ej. 2022 = 2021/22). Toda la app usa esta convención.

### 1.2 Tabla: `league_standings` → se elimina o se unifica

**Opción A – Eliminar tabla:** Las clasificaciones se calculan siempre desde `historical_matches` (ya implementado en `_compute_standings_from_historical_matches`).

**Opción B – Mantener como vista materializada:** Se usa solo para ligas donde no hay partidos (ej. CL grupo fase actual). Se añaden columnas de validación:

| Columna nueva | Tipo | Descripción |
|---------------|------|-------------|
| `data_source` | TEXT | `api_sports`, `manual` |
| `validation_status` | TEXT | `pending`, `confirmed`, `manual_override` |
| `validated_at` | TEXT | ISO timestamp |
| `validated_by` | INTEGER | user_id admin |

**Recomendación:** Opción A (eliminar). Todo se deriva de `historical_matches`. Si una liga no tiene partidos, se indica "sin datos" en lugar de rellenar desde otra tabla.

### 1.3 Nueva tabla: `data_discrepancies`

Registro de discrepancias detectadas (para que el admin las revise):

| Columna | Tipo | Descripción |
|---------|------|-------------|
| `id` | INTEGER PK | |
| `entity_type` | TEXT | `match`, `standings_row` |
| `entity_id` | TEXT | fixture_id o `{league_id}:{season}:{rank}` |
| `field` | TEXT | Campo en disputa: `home_goals`, `away_goals`, `champion`, etc. |
| `value_source_a` | TEXT | Valor de la fuente A |
| `value_source_b` | TEXT | Valor de la fuente B |
| `source_a` | TEXT | football_data_csv, api_sports |
| `source_b` | TEXT | football_data_csv, api_sports |
| `status` | TEXT | `pending`, `resolved`, `dismissed` |
| `resolved_value` | TEXT | Valor final si `status = resolved` |
| `resolved_at` | TEXT | ISO timestamp |
| `resolved_by` | INTEGER | user_id admin |
| `created_at` | TEXT | ISO timestamp |
| `notes` | TEXT | Comentario del admin |

---

## 2. Pipelines de Carga (APIs como enriquecimiento)

### 2.1 Fuente primaria por liga

| Liga | Fuente primaria | Fuente secundaria (completar huecos) |
|------|-----------------|--------------------------------------|
| PL, PD, SA, BL1, FL1, DED, PPL, ELC, EL1 | football-data.co.uk (CSV) | — |
| CL, EL | API-Sports | — |

### 2.2 Flujo de carga

```
1. Cargar CSV football-data (ligas domésticas)
   → INSERT/UPDATE historical_matches con data_source='football_data_csv', validation_status='confirmed'

2. Cargar API-Sports (CL, EL)
   → INSERT/UPDATE historical_matches con data_source='api_sports', validation_status='pending'
   → Si el partido ya existe (por match_key: date+home+away+league), COMPARAR valores
   → Si hay diferencia: INSERT en data_discrepancies, marcar registro como disputed

3. Backfill tarjetas (API-Sports /statistics)
   → UPDATE home_yellow, away_yellow, etc. solo si actualmente NULL
   → No sobrescribir datos ya confirmados
```

### 2.3 Match key para detectar mismo partido

Para saber si football-data y API-Sports hablan del mismo partido:

- `match_key = (date, league_id, home_team_normalized, away_team_normalized)`
- Normalizar nombres: lowercase, quitar "FC", "CF", "AC " → mapeo de alias (Milan ↔ AC Milan, etc.)

### 2.4 Reglas de upsert

| Situación | Acción |
|-----------|--------|
| Partido no existe | INSERT con `data_source` de la fuente que lo aporta, `validation_status='pending'` (API) o `'confirmed'` (CSV primario) |
| Partido existe, mismos datos | No hacer nada |
| Partido existe, datos distintos | INSERT en `data_discrepancies`, SET `validation_status='disputed'` en el registro, no sobrescribir |

---

## 3. Reglas de Detección de Discrepancias

### 3.1 Partidos (historical_matches)

| Campo | Condición discrepancia |
|-------|------------------------|
| `home_goals` | Valor A ≠ Valor B |
| `away_goals` | Valor A ≠ Valor B |
| `ftr` | Valor A ≠ Valor B (derivable de goles, pero se registra si hay conflicto) |
| `home_team_name` | Normalizado diferente (Milan vs AC Milan → warning, no bloqueante) |

### 3.2 Clasificaciones (si se mantiene league_standings)

| Campo | Condición discrepancia |
|-------|------------------------|
| Campeón (rank=1) | Calculado desde partidos ≠ league_standings |
| Puntos del campeón | Calculado ≠ league_standings |

### 3.3 Cuándo ejecutar detección

1. **Tras carga CSV:** No (es fuente primaria).
2. **Tras carga API-Sports:** Sí, comparar con registros existentes por `match_key`.
3. **Job periódico:** Opcional, comparar `_compute_standings` vs `league_standings` si se mantiene.

---

## 4. UI de Administración

### 4.1 Integración en modo "Administración"

El panel admin actual (`mode == "Administración"`) tendrá una nueva sección: **"Validación de datos"**.

### 4.2 Pantallas

#### 4.2.1 Lista de discrepancias

- **Ruta:** Administración → Validación de datos → Discrepancias
- **Contenido:**
  - Tabla: tipo de entidad, liga, temporada/partido, campo, valor A vs valor B, estado, fecha
  - Filtros: estado (pending/resolved/dismissed), tipo (match/standings), liga
  - Acciones por fila: Resolver | Descartar

#### 4.2.2 Detalle de discrepancia (Resolver)

- **Layout:**
  - Entidad: partido X o clasificación Liga Y temporada Z
  - Campo en disputa
  - Valor fuente A (football-data) | Valor fuente B (API-Sports)
  - Selector: "Usar valor A" | "Usar valor B" | "Editar manualmente"
  - Campo de texto: motivo/notas (opcional)
  - Botón: Confirmar
- **Acción:** Actualizar registro en `historical_matches` (o `league_standings`) con el valor elegido, SET `validation_status='manual_override'`, `validated_by`, `validated_at`, `override_reason`. INSERT/UPDATE en `data_discrepancies` con `status='resolved'`, `resolved_value`, `resolved_by`.

#### 4.2.3 Descartar discrepancia

- **Acción:** UPDATE `data_discrepancies` SET `status='dismissed'`. No cambiar el registro original.
- **Uso:** Cuando se considera que la diferencia no es relevante (ej. alias de equipo).

#### 4.2.4 Vista previa de clasificación (opcional)

- **Ruta:** Administración → Validación de datos → Clasificaciones
- **Contenido:** Selector liga + temporada → tabla calculada desde `historical_matches`
- **Uso:** Verificar que la clasificación calculada es correcta antes de usarla en modelos.

### 4.3 Wireframe (texto)

```
┌─────────────────────────────────────────────────────────────────┐
│ Administración                                                   │
├─────────────────────────────────────────────────────────────────┤
│ [Usuarios] [Validación de datos ▼]                               │
│            ├ Discrepancias                                       │
│            └ Clasificaciones (vista previa)                      │
├─────────────────────────────────────────────────────────────────┤
│ Discrepancias pendientes (3)                                     │
│                                                                  │
│ Filtros: [Estado: Pendiente ▼] [Tipo: Todos ▼] [Liga: SA ▼]      │
│                                                                  │
│ ┌────────┬──────────┬──────────────┬─────────┬─────────┬────────┐│
│ │ Tipo   │ Entidad  │ Campo        │ Valor A │ Valor B │ Acción ││
│ ├────────┼──────────┼──────────────┼─────────┼─────────┼────────┤│
│ │ match  │ SA 2022  │ away_goals   │ 1       │ 2       │[Resolver][Descartar]│
│ │ match  │ PL 2024  │ home_goals   │ 3       │ 2       │[Resolver][Descartar]│
│ └────────┴──────────┴──────────────┴─────────┴─────────┴────────┘│
└─────────────────────────────────────────────────────────────────┘

--- Resolver discrepancia (modal o sección expandida) ---

┌─────────────────────────────────────────────────────────────────┐
│ Resolver: SA 2022 - Lazio vs Napoli (2022-05-15)                 │
├─────────────────────────────────────────────────────────────────┤
│ Campo: away_goals (goles visitante)                              │
│                                                                  │
│ football-data.co.uk:  1                                          │
│ API-Sports:           2                                          │
│                                                                  │
│ ○ Usar valor de football-data (1)                                │
│ ○ Usar valor de API-Sports (2)                                   │
│ ○ Editar manualmente: [___]                                      │
│                                                                  │
│ Notas (opcional): [________________________________]             │
│                                                                  │
│                              [Cancelar]  [Confirmar]             │
└─────────────────────────────────────────────────────────────────┘
```

---

## 5. Plan de Migración

### Fase 1 – Schema (sin romper producción)

1. Añadir columnas a `historical_matches`: `data_source`, `validation_status`, `validated_at`, `validated_by`, `override_reason`
2. Crear tabla `data_discrepancies`
3. Migración de datos: SET `data_source='football_data_csv'` y `validation_status='confirmed'` para filas con `fixture_id` en rango CSV; SET `data_source='api_sports'` y `validation_status='pending'` para filas con `api_sports_fixture_id` NOT NULL

### Fase 2 – Pipelines

1. Modificar `data_downloader.py`: al insertar/actualizar, SET `data_source`, `validation_status`
2. Modificar carga API-Sports: antes de UPSERT, comparar con registro existente por `match_key`; si hay diferencia, INSERT en `data_discrepancies` y marcar `validation_status='disputed'`
3. Definir `match_key` y lógica de normalización de nombres

### Fase 3 – Eliminar league_standings como fuente

1. `get_league_standings` ya prioriza `historical_matches` (calculado)
2. Opcional: eliminar o vaciar `league_standings`, o dejarla solo para ligas sin partidos (CL fase grupos) con flag `use_standings_table_only`

### Fase 4 – UI Admin

1. Nueva sección "Validación de datos" en modo Administración
2. Página "Discrepancias": listar, filtrar, resolver, descartar
3. Página "Clasificaciones": vista previa por liga/temporada

### Orden sugerido

| Orden | Fase | Esfuerzo |
|-------|------|----------|
| 1 | Fase 1 – Schema | Bajo |
| 2 | Fase 2 – Pipelines (detección) | Medio |
| 3 | Fase 4 – UI Admin | Medio |
| 4 | Fase 3 – Deprecar league_standings | Bajo |

---

## 6. Uso por los Modelos (Gemini, etc.)

- Toda consulta de partidos o clasificaciones usa **solo** `historical_matches` (y clasificación calculada).
- No hay lógica de prioridad entre fuentes en el código: la fuente de verdad es la tabla consolidada.
- Si un registro está `disputed`, el admin debe resolverlo antes de que se considere definitivo; hasta entonces, se puede usar el valor más reciente o el de la fuente primaria según regla de negocio.

---

## 7. Resumen

| Antes | Después |
|-------|---------|
| Dos fuentes: football-data + API-Sports con prioridades en código | Una tabla: `historical_matches` como fuente de verdad |
| `league_standings` vs cálculo desde partidos | Solo cálculo desde `historical_matches` |
| Discrepancias resueltas por prioridades fijas | Discrepancias detectadas y resueltas manualmente por admin |
| Sin trazabilidad de quién validó | `validated_by`, `validated_at`, `override_reason` |
