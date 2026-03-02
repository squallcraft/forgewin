# Arquitectura Tabla Maestra – ForgeWin

**Objetivo:** Una sola fuente de verdad para los agentes IA. Los datos solo son usables cuando han pasado doble verificación (API-Sports + humano). Los modelos **solo** leen de `master_table_checked`.

---

## 0. Orden de operaciones (normalización antes de master)

1. **entity_aliases** debe estar poblada antes de copiar a master_table.
2. Al copiar `historical_matches` → `master_table`, se aplica normalización de nombres (Milan → AC Milan, Man Utd → Man United, etc.) usando `entity_aliases`.
3. La master_table recibe datos ya normalizados; las comparaciones (verificación 1 y 2) usan nombres canónicos.

---

## 1. Flujo General

```
┌─────────────────────┐     ┌─────────────────────┐     ┌─────────────────────────┐
│ historical_matches  │────▶│   master_table      │────▶│  master_table_checked   │
│ (datos crudos)      │     │ (staging, 2 checks) │     │ (solo datos verificados)│
└─────────────────────┘     └─────────────────────┘     └─────────────────────────┘
         │                              │                              │
         │ Copia inicial                │ Verificación 1 (API-Sports)   │
         │                              │ Verificación 2 (Humano)       │
         │                              │                               │
         │                              │  ✓ Ambas OK → INSERT aquí     │
         │                              │                               │
         └──────────────────────────────┴───────────────────────────────┘
                                                      │
                                                      ▼
                                          ┌─────────────────────────────┐
                                          │  Agentes IA (Gemini, Grok)  │
                                          │  LEEN SOLO DE AQUÍ          │
                                          └─────────────────────────────┘
```

**Regla de oro:** Ningún agente IA usa datos hasta que estén en `master_table_checked`. Si la tabla está vacía, los modelos no se ejecutan o indican "sin datos verificados".

---

## 2. Modelo de Datos

### 2.1 Tabla: `master_table`

Copia de `historical_matches` + columnas de verificación. Área de staging.

| Columnas | Descripción |
|----------|-------------|
| *Todas las de historical_matches* | fixture_id, date, league_id, home_team_name, away_team_name, home_goals, away_goals, ftr, season, etc. |
| `verificado_1` | BOOLEAN. True = coincide con API-Sports (dos fuentes coinciden). |
| `verificado_1_at` | TEXT. ISO timestamp de la verificación 1. |
| `verificado_2` | BOOLEAN. True = humano verificó (vía chatbot Grok). |
| `verificado_2_at` | TEXT. ISO timestamp de la verificación 2. |
| `verificado_2_by` | INTEGER. user_id del admin que verificó. |
| `source` | TEXT. Origen: `football_data_csv`, `api_sports`. |

**Lógica:** Un registro solo puede pasar a `master_table_checked` si `verificado_1 = 1` Y `verificado_2 = 1`.

### 2.2 Tabla: `master_table_checked`

Solo datos con doble verificación. **Es la única fuente para los agentes IA.**

| Columnas | Descripción |
|----------|-------------|
| *Mismas columnas de datos que master_table* | Sin verificado_1, verificado_2 (ya están verificados). |
| `checked_at` | TEXT. ISO timestamp cuando pasó a esta tabla. |
| `checked_by` | INTEGER. user_id del admin que completó verificación 2. |

**Sin columnas de estado:** Todo lo que está aquí está verificado.

### 2.3 Tabla: `data_discrepancies`

Registro de discrepancias (master_table vs API-Sports o vs dato humano). Para auditoría y admin.

---

## 3. Proceso de Verificación

### 3.1 Paso 0: Carga inicial

1. Copiar `historical_matches` → `master_table`.
2. SET `verificado_1 = 0`, `verificado_2 = 0` para todos.
3. SET `source` según origen (football_data_csv / api_sports).

### 3.2 Paso 1: Verificación automática (API-Sports)

1. Para cada registro en `master_table` sin `verificado_1`:
   - Buscar partido equivalente en API-Sports (match_key: date + league + home + away).
   - Comparar: home_goals, away_goals (y otros campos críticos).
2. Si coinciden → SET `verificado_1 = 1`, `verificado_1_at = now()`.
3. Si no coinciden o no existe en API-Sports → INSERT en `data_discrepancies`, dejar `verificado_1 = 0` (admin puede forzar después).

### 3.3 Paso 2: Verificación humana (chatbot Grok)

1. Admin entra en "Verificación de admin".
2. Selecciona liga y año.
3. El chatbot Grok muestra:
   - Nuestra tabla (master_table para esa liga/año).
   - Pide al admin subir o describir la fuente oficial (imagen, texto, PDF).
4. Grok analiza lo que el admin aporta y extrae la clasificación/partidos.
5. Grok muestra comparación: nuestra tabla vs lo detectado.
6. Grok pide al admin validar (confirmar o corregir).
7. Admin confirma → SET `verificado_2 = 1`, `verificado_2_at`, `verificado_2_by`.
8. Si hay correcciones → UPDATE master_table con los valores validados.

### 3.4 Paso 3: Promoción a master_table_checked

1. Job o trigger: cuando un registro tiene `verificado_1 = 1` Y `verificado_2 = 1`:
   - INSERT en `master_table_checked` (o UPSERT por fixture_id si ya existe).
   - Opcional: marcar en master_table como "promoted" para no duplicar.

**Alternativa:** La promoción ocurre en el momento en que el admin confirma en el chatbot (Paso 2), si ya tenía verificado_1.

---

## 4. Uso por los Agentes IA

- **get_league_standings_for_ai(league_id, season):** Lee de `master_table_checked` (o calcula clasificación desde partidos en master_table_checked).
- **get_historical_matches_for_ai(...):** Lee de `master_table_checked`.
- **Gemini / Grok / Alfred:** Todas las funciones que construyen contexto para IA usan **solo** `master_table_checked`.
- Si `master_table_checked` está vacía para una liga/temporada → respuesta: "No hay datos verificados para esa liga/temporada. Completa la verificación en el panel de admin."

---

## 5. Página "Verificación de admin"

### 5.1 Ubicación

- Modo Administración → Verificación de admin.
- Solo accesible por role=admin.

### 5.2 Layout

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ Verificación de admin                                                        │
├─────────────────────────────────────────────────────────────────────────────┤
│ Liga:   [Premier League ▼]    Temporada: [2022 ▼]                            │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │                         CHATBOT (Grok)                                  │ │
│  │                                                                         │ │
│  │  [Mensajes del chat...]                                                 │ │
│  │                                                                         │ │
│  │  Grok: "Sube una imagen de la tabla oficial o escribe los datos..."     │ │
│  │                                                                         │ │
│  │  Usuario: [imagen subida]                                               │ │
│  │                                                                         │ │
│  │  Grok: "Detecté la siguiente clasificación. Comparando con nuestra BD:" │ │
│  │  [Tabla nuestra vs Tabla detectada]                                     │ │
│  │  "¿Confirmas que los datos son correctos? [Sí] [Corregir]"              │ │
│  │                                                                         │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
│  [Input de texto]                                    [Enviar] [Adjuntar 📎]  │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 5.3 Flujo del chatbot

1. **Inicio:** Grok recibe liga + año. Carga master_table para esa liga/año.
2. **Primer mensaje:** "Para verificar la temporada {liga} {año}, sube una imagen de la tabla oficial o pega los datos en texto."
3. **Usuario sube imagen o texto:** Grok analiza (visión si imagen, texto si texto) y extrae clasificación/partidos.
4. **Grok muestra comparación:** Tabla nuestra (master_table) vs tabla detectada. Resalta diferencias.
5. **Grok pide validar:** "¿Los datos coinciden? Si hay errores, indícalos."
6. **Usuario confirma o corrige:** Si corrige, Grok actualiza master_table con los valores indicados.
7. **Grok marca verificado_2:** SET verificado_2=1 para esos registros. Si verificado_1 ya estaba → INSERT en master_table_checked.
8. **Grok cierra:** "Verificación guardada. Los datos pasan a estar disponibles para los agentes IA."

### 5.4 Modelo Grok

- Usar la API más potente de Grok (según configuración).
- Solo disponible en esta sección (no en el chat general de Alfred).

---

## 6. Resumen de Tablas

| Tabla | Propósito | Quién escribe | Quién lee |
|-------|-----------|---------------|-----------|
| `historical_matches` | Datos crudos (APIs, CSV) | Pipelines de carga | Nadie para IA. Solo para alimentar master_table. |
| `master_table` | Staging. Doble verificación. | Pipelines, chatbot, job verificación 1 | Admin, chatbot |
| `master_table_checked` | Datos verificados (doble check) | Job/trigger al completar ambas verificaciones | **Solo agentes IA** |
| `data_discrepancies` | Auditoría de discrepancias | Pipelines, chatbot | Admin |

---

## 7. Plan de Implementación

### Fase 1 – Schema

1. Crear `master_table` (copia de historical_matches + verificado_1, verificado_2, etc.).
2. Crear `master_table_checked` (mismas columnas de datos, sin flags de verificación).
3. Crear `data_discrepancies` (si no existe).

### Fase 2 – Carga inicial

1. Script: copiar historical_matches → master_table.
2. SET source según fixture_id (CSV vs API-Sports).

### Fase 3 – Verificación 1 (API-Sports)

1. Job/script: comparar master_table con API-Sports por liga/season.
2. SET verificado_1=1 donde coincida.
3. INSERT en data_discrepancies donde no coincida.

### Fase 4 – UI Verificación de admin

1. Nueva página "Verificación de admin" en modo Administración.
2. Selectores: liga, temporada.
3. Chatbot Grok: integración con API, soporte imagen + texto, flujo de comparación y validación.
4. Al confirmar → SET verificado_2=1, promoción a master_table_checked si verificado_1=1.

### Fase 5 – Integración con agentes IA ✓

1. Crear `get_matches_from_master_checked()` y `get_standings_from_master_checked()` ✓
2. Sustituir llamadas a historical_matches por master_table_checked en flujos de IA ✓
   - Usuarios Plus/admin: forma reciente y H2H desde master_table_checked (fallback a historical_matches si vacío)
   - Usuarios base: siguen usando historical_matches
3. Si master_table_checked vacía → fallback a historical_matches (transparente para el usuario)

---

## 8. Acceso por tier (para fases futuras)

| Tier | Fuente de datos | Uso |
|------|-----------------|-----|
| **Base / gratuito** | `historical_matches` o `master_table` (sin doble check) | Análisis con datos no verificados |
| **Plus / de pago** | `master_table_checked` | Análisis con datos verificados |

---

## 9. Tabla entity_aliases (normalización)

- Normaliza nombres de equipos y ligas (Milan ↔ AC Milan, Man Utd ↔ Man United, etc.).
- Se usa **antes** de copiar a master_table.
- Añadir aliases: `add_entity_alias("team", "AC Milan", "Milan", "SA")`.
- Seed por defecto: `seed_default_entity_aliases()`.

---

## 10. Ventajas de esta arquitectura

| Aspecto | Beneficio |
|---------|-----------|
| **Una sola tabla para IA** | Los agentes no filtran por flags; leen solo de master_table_checked. |
| **Cero ambigüedad** | Si está en master_table_checked, está verificado. |
| **Escalable** | Se puede añadir verificación 3 (otra fuente) sin cambiar la tabla de IA. |
| **Auditable** | master_table conserva el historial de verificaciones; data_discrepancies registra conflictos. |
| **Control humano** | Ningún dato llega a IA sin verificación humana (para ligas/años que la requieran). |
