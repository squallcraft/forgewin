# Tabla histórica: Champions League (CL)

## Diagnóstico (revisión Feb 2026)

- **Partidos recientes:** Sí. La tabla `historical_matches` con `league_id = 'CL'` tiene partidos hasta fechas recientes (ej. 2026-02-18), con resultados (goles, equipos, temporada).
- **Estadísticas para análisis V3:** No. Las columnas que alimentan **Índice de Asedio**, **Alerta de Fricción** y **Dinámica HT/FT** están vacías en CL:
  - `home_shots_target`, `away_shots_target`, `home_corners`, `away_corners` → 0 filas con dato
  - `referee`, `hthg`, `htag`, `htr` → 0 filas con dato
  - `home_fouls`, `away_fouls`, `home_yellow`, `away_yellow` → 0 filas con dato

## Causa

Los partidos de CL (y EL) se cargan desde API-Sports. Para rellenar estadísticas, los scripts de backfill usan `api_sports_fixture_id` para llamar a la API (p. ej. `/fixtures?id=...` + statistics). En muchos partidos CL/EL ese campo estaba en **NULL**, por lo que el backfill de estadísticas no los procesaba.

## Solución

1. **Rellenar `api_sports_fixture_id` en CL/EL** (partidos con `fixture_id` en rango 9xx):
   ```bash
   python run_backfill_cl_el_api_fixture_id.py
   ```
   Esto actualiza `api_sports_fixture_id = fixture_id - 900000000` donde sea NULL.

2. **Ejecutar backfill de estadísticas** (tiros a puerta, córners, faltas):
   ```bash
   python run_backfill_statistics_from_apisports.py
   ```
   Respeta cuota API (p. ej. `--delay 0.25` para plan Pro).

3. **Opcional: tarjetas y árbitro**
   ```bash
   python run_backfill_cards_from_apisports.py
   python run_backfill_attendance_referee_from_apisports.py --league CL
   ```

Tras esto, los partidos CL que tengan estadísticas en API-Sports tendrán datos para el análisis enriquecido (asedio, fricción, HT/FT) en la app y en V3.

## Resumen por temporada (CL)

Para ver el estado actual:
```bash
cd football-betting-analysis && source venv/bin/activate && python -c "
from db import get_connection, init_db
init_db()
with get_connection() as conn:
    c = conn.cursor()
    c.execute('''
        SELECT season, COUNT(*), SUM(CASE WHEN api_sports_fixture_id IS NOT NULL THEN 1 ELSE 0 END),
               SUM(CASE WHEN home_shots_target IS NOT NULL THEN 1 ELSE 0 END)
        FROM historical_matches WHERE league_id = \"CL\"
        GROUP BY season ORDER BY season DESC
    ''')
    for r in c.fetchall():
        print('Season %s: %s partidos, con api_fid: %s, con stats: %s' % (r[0], r[1], r[2], r[3]))
"
```
