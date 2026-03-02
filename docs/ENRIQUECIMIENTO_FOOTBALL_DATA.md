# Enriquecimiento desde football-data.org (sin duplicar)

## Regla

- **Una sola fila por partido** en `historical_matches` (identificada por `fixture_id`).
- Los datos que falten se intentan obtener primero desde **API-Sports**; si no hay datos (0-0, null, o no tenemos `api_sports_fixture_id`), se intenta **football-data.org**.
- **Solo UPDATE**: nunca se insertan filas nuevas desde FD. Si el partido no existe en BD, no se crea.
- **Homologación de nombres**: el emparejamiento con FD se hace por liga + fecha + nombres de equipos, usando la misma lógica que en `data_fetcher` (`_normalize_team_name_for_match`, `_team_names_match`), para que "Inter" coincida con "FC Internazionale Milano", "Bodo/Glimt" con "FK Bodø/Glimt", etc.

## Flujos

### 1. Estadísticas (tiros, córners, faltas, tarjetas)

- **Backfill API-Sports** (`run_backfill_statistics_from_apisports.py`): rellena estadísticas desde API-Sports. Si devuelve 0-0 en tiros a puerta, hace **fallback a football-data.org** para ese mismo partido (mismo `fixture_id`), usando `get_match_statistics_football_data_org(league_id, date, home_team_name, away_team_name)` y homologación de nombres.
- **Enricher FD** (`run_enrich_from_football_data.py`): partidos con estadísticas vacías o 0-0 en ligas FD (CL, PL, PD, SA, BL1, FL1, DED, PPL, EL), **con o sin** `api_sports_fixture_id`. Por cada candidato llama a FD; si encuentra el partido por nombre, actualiza la **misma** fila (estadísticas y tarjetas). No crea filas nuevas.

### 2. Champions League: actualizar goles / equipos desde FD

- **Actualizar CL desde FD** (`run_update_cl_football_data.py`): obtiene partidos CL en un rango de fechas desde football-data.org. **Solo actualiza filas ya existentes**: busca en BD por `league_id = 'CL'`, misma fecha y equipos (homologación). Si encuentra la fila, la actualiza (goles, status, team_ids, nombres). Si no encuentra, **no inserta** (evita duplicados).

## Ligas soportadas por FD en el enricher

Por defecto: CL, PL, PD, SA, BL1, FL1, DED, PPL, EL.  
Europa League (EL) puede devolver 403 en plan gratuito de FD.

## Pausas (rate limit)

- **API-Sports** (de pago): pausa corta por defecto (ej. 0,25 s). Configurable con `API_FOOTBALL_REQUEST_DELAY` en `.env`.
- **football-data.org** (gratuita): pausa **mayor** por defecto (6 s entre peticiones) para no superar la cuota y evitar 429. Configurable con `FOOTBALL_DATA_ORG_REQUEST_DELAY` en `.env` (mínimo recomendado 6).

Todas las llamadas a football-data.org (data_fetcher, enricher, backfill fallback) usan `config.REQUEST_DELAY_SECONDS` antes de cada GET.

## Uso

```bash
# Enriquecer hasta 50 partidos (pausa entre partidos = config FD, por defecto 6 s)
python run_enrich_from_football_data.py

# Solo Champions League, dry-run
python run_enrich_from_football_data.py --league CL --dry-run

# Actualizar CL 2025–2026 solo en filas existentes (sin insertar)
python run_update_cl_football_data.py --dry-run
python run_update_cl_football_data.py
```

## Resumen

| Origen datos | Acción |
|--------------|--------|
| API-Sports   | Primera fuente; backfill estadísticas/árbitro/tarjetas. |
| football-data.org | Fallback cuando API-Sports no da datos; **solo UPDATE** en la fila existente; emparejo por **homologación de nombres**. |
| Inserción desde FD | No se usa: no se crean filas nuevas desde FD para no duplicar. |
