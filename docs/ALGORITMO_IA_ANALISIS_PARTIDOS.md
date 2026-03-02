# Algoritmo y scripts que usan las IA para analizar partidos

Este documento describe **cómo** se analizan los partidos: datos numéricos (Poisson), luego Alfred (Grok) y Reginald (Gemini), y por último el consenso (Propuesta General 1+2).

---

## 1. Resumen del flujo

```
Partidos seleccionados (fixture_id, equipos, liga, fecha)
         │
         ▼
┌─────────────────────────────────────────────────────────┐
│  analyzer.analyze_matches()                              │
│  Por cada partido: stats, H2H, lesiones, odds → Poisson │
│  Salida: lista de dicts con probs (1-X-2, xG, BTTS, …)   │
└─────────────────────────────────────────────────────────┘
         │
         ▼  match_data (partidos + nuestras probs)
         │
    ┌────┴────┐
    ▼         ▼
 Alfred    Reginald
 (Grok)    (Gemini)
    │         │
    │  ask_grok_proposal_analysis(match_data, db_context)
    │  ask_gemini_proposal_analysis(match_data, db_context)
    │         │
    ▼         ▼
 análisis   análisis
 + JSON    + JSON
 (stats)   (stats)
    │         │
    └────┬────┘
         ▼
┌─────────────────────────────────────────────────────────┐
│  consensus.run_consensus_flow(grok_analysis, gemini_analysis, match_data)
│  Pasos 1–6: opiniones cruzadas → Alfred consultor → Reginald opinión final │
└─────────────────────────────────────────────────────────┘
         │
         ▼
  Propuesta General 1+2 (análisis markdown + stats_by_fixture)
```

---

## 2. Cálculo de probabilidades (sin IA): `analyzer.py`

### 2.1 Entrada

- Lista de partidos con `home_team_id`, `away_team_id`, `league_id`, `fixture_id`, nombres, fecha.

### 2.2 Por cada partido: `_analyze_one_match()`

1. **Stats de equipos**  
   `get_team_stats(home_id, league_code)` y análogo para visitante (football-data.org o API-Sports según config). Se usan para promedios de goles a favor y en contra.

2. **H2H**  
   `get_h2h(fixture_id, limit=5)` — últimos enfrentamientos.

3. **Lesiones**  
   `get_injuries(fixture_id)` — impacto estimado por equipo (hasta ~25% de reducción de lambda).

4. **Opcional: historial rolling**  
   Si existe `historical_analyzer`, se enriquece con forma reciente y se calcula `lambda_bias` por liga.

5. **Opcional: datos externos (xG)**  
   Si existe `data_integrators`, se puede ajustar lambda con xG externo.

6. **Cuotas de casas**  
   `get_match_odds(fixture_id, ...)` — cuotas 1-X-2 para mezclar con Poisson.

7. **Cálculo final**  
   `calculate_probabilities(home_stats, away_stats, h2h, injuries, ..., bookmaker_odds, external_data, lambda_bias)`.

### 2.3 Modelo Poisson: `calculate_probabilities()`

- **Lambdas** (goles esperados por equipo):
  - `lambda_home = f(goals_avg_for_local, goals_avg_against_visitante)`  
    (fórmula tipo: `goals_avg_for * (goals_avg_against_opponent / 1.5)`).
  - `lambda_away` análogo.
- **Ajustes**:
  - **H2H**: mezcla 70% lambda base, 30% media de goles en H2H.
  - **xG**: si hay `external_data` (xG_home, xG_away), se escalan lambdas.
  - **Lesiones**: multiplicador `(1 - injury_impact)` sobre lambda (hasta ~25%).
  - **lambda_bias**: factor por liga (historial).
- **Probabilidades**:
  - 1-X-2: suma de `P(home=i)*P(away=j)` para i>j, i=j, i<j (Poisson con `lambda_home`, `lambda_away`).
  - BTTS: `1 - P(home=0) - P(away=0) + P(0,0)`.
  - Over 2.5: suma de P(i+j>2.5).
  - Clean sheet local/visitante: P(visitante=0) y P(local=0).
  - xG total: `lambda_home + lambda_away`.
- **Mezcla con cuotas** (si hay `bookmaker_odds`):
  - Probabilidades implícitas desde cuotas.
  - Prob final = `blend_weight_poisson * prob_poisson + (1 - blend) * implied` (por defecto 60% Poisson, 40% casas).
  - Value bet: nuestra prob > implícita + 2%.

Salida: diccionario con `prob_home_win`, `prob_draw`, `prob_away_win`, `expected_goals`, `clean_sheet_home`, `clean_sheet_away`, `prob_btts`, `prob_over25`, y opcionalmente `value_*`, `implied_*`.

---

## 3. Alfred (Grok): `grok_client.py`

### 3.1 Función principal: `ask_grok_proposal_analysis(match_data, db_context=None)`

- **Entrada**:
  - `match_data`: lista de dicts por partido (fixture_id, equipos, liga, fecha, **nuestras probs** del analyzer).
  - `db_context`: texto opcional con forma reciente, H2H, etc., construido en la app.

- **Construcción del prompt**:
  - Fecha actual.
  - Bloque de datos por partido: `_build_match_data_context(match_data)` → una línea por partido con equipos, liga, fecha, probs 1-X-2, xG, clean sheet, BTTS, Over 2.5 (y lista de `fixture_id` para el JSON).
  - Si hay `db_context`, se añade como “Datos adicionales disponibles en la app”.
  - Se añade el texto fijo `PROPOSAL_USER_INSTRUCTIONS`.

- **Mensajes a la API**:
  - **System**: `PROPOSAL_SYSTEM_PROMPT`.
  - **User**: el bloque anterior.

- **Llamada**: `_call_grok(messages, model=GROK_MODEL_FAST, timeout=180)`.

- **Salida**: `_parse_grok_proposal_response(raw, match_data)` → `{"analysis": str, "stats_by_fixture": {fixture_id: {...}}}`.  
  El análisis es markdown; las stats se extraen del JSON que Alfred debe devolver entre `GROK_STATS_JSON` y `END_GROK_STATS` (o de un bloque ```json).

### 3.2 Prompts fijos (fragmentos)

**PROPOSAL_SYSTEM_PROMPT** (resumen):  
Eres un analista experto en fútbol. Respondes en español. Analizas con el contexto dado. En el JSON de stats das **tu propia** estimación; no copies los números del contexto. Analizas todos los partidos. Verificas plantillas/trasferencias antes de citar jugadores; si no puedes verificarlo, no afirmes el equipo.

**PROPOSAL_USER_INSTRUCTIONS** (resumen):  
Usar contexto (partidos, probs, xG, clean sheet, BTTS, Over 2.5). Incluir por partido: forma reciente (5), H2H (3), probs, xG, clean sheet, BTTS, Over 2.5; si aplica: efecto Europa/FIFA, Biscotto, árbitro. Responder en markdown por partido. Formato obligatorio: línea `GROK_STATS_JSON`, luego JSON (array de objetos con `fixture_id`, `prob_home_win`, `prob_draw`, `prob_away_win`, `expected_goals`, `clean_sheet_home`, `clean_sheet_away`, `prob_btts`, `prob_over25`, `value_bet`), línea `END_GROK_STATS`, después análisis en markdown.

---

## 4. Reginald (Gemini): `gemini_client.py`

### 4.1 Función principal: `ask_gemini_proposal_analysis(match_data, db_context=None)`

- **Mismo contrato que Alfred**: misma entrada (`match_data`, `db_context`), mismo formato de respuesta (análisis + JSON de stats).
- **Mismos textos de prompt**: usa `PROPOSAL_SYSTEM_PROMPT`, `PROPOSAL_USER_INSTRUCTIONS` y `_build_match_data_context()` importados desde `grok_client`.
- **Llamada**: `_call_gemini(PROPOSAL_SYSTEM_PROMPT, user_content, timeout=180)` (modelo por defecto, p. ej. Flash).
- **Parseo**: mismo `_parse_grok_proposal_response(raw, match_data)` para extraer `analysis` y `stats_by_fixture`.

Reginald recibe exactamente el mismo contexto que Alfred (partidos + probs del analyzer + db_context) y las mismas instrucciones; la diferencia es solo el modelo (Gemini vs Grok).

---

## 5. Consenso (Propuesta General 1+2): `consensus.py`

### 5.1 Función: `run_consensus_flow(grok_analysis, gemini_analysis, match_data)`

Entrada: texto de análisis de Alfred, texto de análisis de Reginald, y lista `match_data` (para orden de partidos y fallback de stats).

**Paso 1**  
Reginald opina sobre el análisis de Alfred: “Desacuerdos y puntos clave por partido en 1–2 frases.”  
→ `ask_gemini_custom(SYSTEM_CONSENSUS, prompt1)` (Flash).

**Paso 2**  
Alfred opina sobre el análisis de Reginald: mismo tipo de instrucción.  
→ `ask_grok_custom(SYSTEM_CONSENSUS, prompt2)`.

**Paso 5**  
Alfred como “consultor senior”: recibe su análisis inicial y las críticas de Reginald; debe resumir consenso, discrepancias y recomendación final (resumen ejecutivo).  
→ `ask_grok_custom(SYSTEM_CONSENSUS, prompt5)`.

**Paso 6**  
Reginald recibe todo: Opción 1 (Alfred), Opción 2 (Reginald), opinión de Reginald sobre Alfred, opinión de Alfred sobre Reginald, recomendación final de Alfred. Debe producir la **Propuesta General 1+2**: análisis de consenso en markdown y un JSON de stats (mismo esquema: fixture_id, prob_home_win, prob_draw, prob_away_win, expected_goals, clean_sheet_*, prob_btts, prob_over25, value_bet).  
→ `ask_gemini_custom(..., model=GEMINI_MODEL_CONSENSUS, temperature=0.1)` (Pro).  
La respuesta se parsea con `_parse_grok_proposal_response()`; el markdown va a `result["analysis"]` y el diccionario por fixture_id a `result["stats_by_fixture"]`.

Salida: `{"analysis": str, "stats_by_fixture": {...}, "gemini_opinion", "grok_opinion", "grok_final"}`.

---

## 6. Dónde se orquesta en la app: `app.py`

- **Partidos del día / liga**:  
  Se obtienen partidos, se llama `analyze_matches(selected_match_dicts, use_mock=...)` → `match_data` con probs.
- **“Dale con todo” (Opción 1 + Opción 2 + Consenso)**:
  - Se construye `db_context` (forma, H2H, etc.) a partir de la BD.
  - En paralelo (o secuencial):  
    - Alfred: `ask_grok_proposal_analysis(match_data, db_context)` → análisis + stats.  
    - Reginald: `ask_gemini_proposal_analysis(match_data, db_context)` → análisis + stats.  
  - Consenso: `run_consensus_flow(v2_grok_analysis, gemini_full_analysis, match_data)` → Propuesta General 1+2 (texto + stats por fixture).
- **Flujo V2 (solo Alfred + Reginald sobre Alfred)**  
  Misma idea: `match_data` desde `analyze_matches`, luego Alfred, luego Reginald analizando la tabla y el análisis de Alfred (sin pasos 2–6 del consenso).

---

## 7. Archivos clave

| Archivo | Qué hace |
|--------|-----------|
| `analyzer.py` | `analyze_matches()`, `_analyze_one_match()`, `calculate_probabilities()` — Poisson, H2H, lesiones, odds, xG opcional. |
| `grok_client.py` | `ask_grok_proposal_analysis()`, `build_match_context()`, `_build_match_data_context()`, `_parse_grok_proposal_response()`, prompts de propuesta. |
| `gemini_client.py` | `ask_gemini_proposal_analysis()`, `ask_gemini_custom()`, `_call_gemini()` — mismos prompts que Alfred, otro modelo. |
| `consensus.py` | `run_consensus_flow()` — 6 pasos (opiniones cruzadas + Alfred consultor + Reginald opinión final con JSON). |
| `app.py` | Orquesta: partidos → `analyze_matches` → match_data → Alfred + Reginald → consenso → UI y PDF. |

Así es el algoritmo/script que utilizan las IA para analizar los partidos: primero números (Poisson + casas) en `analyzer.py`, luego Alfred y Reginald con el mismo contexto y formato en `grok_client.py` y `gemini_client.py`, y por último el consenso en `consensus.py` para la Propuesta General 1+2.
