# Fuentes de datos para columnas vacías en historical_matches

## 1. APIs ya conectadas en el proyecto

| API | Uso actual | Clave .env |
|-----|------------|------------|
| **API-Sports** (api-sports.io) | Fixtures, standings, referee, H2H, form | API_FOOTBALL_KEY |
| **football-data.org v4** | Standings, partidos del día, form | FOOTBALL_DATA_ORG_TOKEN |
| **football-data.co.uk** (CSV) | Histórico por liga/temporada | (descarga directa) |
| **TheSportsDB** | Fallback partidos | THE_SPORTS_DB_KEY |
| **The Odds API** | Cuotas 1X2 | ODDS_API_KEY |

---

## 2. Datos que necesitamos vs APIs que los tienen

| Columna en historical_matches | API que lo contiene | Notas |
|------------------------------|---------------------|-------|
| **home_team_id** | API-Sports | Al emparejar fixture; no lo guardamos aún en backfill |
| **away_team_id** | API-Sports | Igual que home_team_id |
| **kickoff_time** | API-Sports, football-data.co.uk | fixture.date (API-Sports); Time (CSV) |
| **hthg, htag, htr** | football-data.org v4, football-data.co.uk | score.halfTime (org); HTHG, HTAG, HTR (CSV) |
| **attendance** | football-data.org v4 | Campo `attendance` en Match. API-Sports no lo devuelve en nuestro plan |
| **referee** | API-Sports ✅, football-data.org, football-data.co.uk | Ya lo rellenamos con API-Sports |
| **home_shots, away_shots** | API-Sports /statistics, football-data.co.uk | Shots / Total Shots; HS, AS (CSV) |
| **home_shots_target, away_shots_target** | API-Sports /statistics, football-data.co.uk | Shots on Goal; HST, AST (CSV) |
| **home_corners, away_corners** | API-Sports /statistics, football-data.co.uk | Corner Kicks; HC, AC (CSV) |
| **home_fouls, away_fouls** | API-Sports /statistics, football-data.co.uk | Fouls; HF, AF (CSV) |
| **home_yellow, away_yellow, home_red, away_red** | API-Sports /statistics ✅, football-data.co.uk | Yellow/Red Cards; HY, AY, HR, AR (CSV) |
| **home_offsides, away_offsides** | API-Sports /statistics, football-data.co.uk | Offsides; HO, AO (CSV) |
| **home_xg, away_xg** | — | Ninguna de nuestras APIs lo incluye de forma directa |
| **api_sports_fixture_id** | API-Sports | Ya lo rellenamos en el backfill |

---

## 3. Fuentes alternativas (internet)

| Fuente | Datos | Plan / acceso |
|--------|-------|----------------|
| **football-data.org** | attendance, referee, halfTime, venue | Free: 10 req/min; competiciones top gratuitas |
| **StatsBomb Open Data** | xG, estadísticas detalladas | Gratuito en GitHub |
| **SofaScore / Flashscore** | Asistencia, árbitro, estadísticas | API de pago |
| **Understat** | xG por partido | Web scraping |
| **Transfermarkt** | Asistencia (parcial) | Web scraping |
| **FBref** | Estadísticas avanzadas, xG | Web scraping / API limitada |
| **football.db** | Ligas, equipos, partidos básicos | Open source, dominio público |
| **API-Sports (api-sports.io)** | Estadísticas, asistencia, referee (Pro en ForgeWin) | ForgeWin configurado para Pro (300 req/min) |

---

## 4. Ajustes realizados

1. **Backfill attendance/referee** (`run_backfill_attendance_referee_from_apisports.py`):  
   Rellena `referee`, `api_sports_fixture_id`, `home_team_id` y `away_team_id` para partidos emparejados con API-Sports.

2. **API-Sports /statistics** (`get_fixture_statistics`):  
   Ahora devuelve además de yellow/red: `shots`, `shots_on_target`, `corners`, `fouls`, `offsides`.

3. **Backfill de estadísticas** (`run_backfill_statistics_from_apisports.py`):  
   Para filas con `api_sports_fixture_id` + `home_team_id` + `away_team_id`, llama a /statistics y actualiza `home_shots`, `away_shots`, `home_shots_target`, `away_shots_target`, `home_corners`, `away_corners`, `home_fouls`, `away_fouls`, `home_offsides`, `away_offsides`.

4. **Orden de ejecución:**  
   Ejecutar primero `run_backfill_attendance_referee_from_apisports.py` (rellena team IDs) y luego `run_backfill_statistics_from_apisports.py` para estadísticas.

5. **API-Sports Pro (300 req/min):** ForgeWin está configurado para Pro. Los backfills usan por defecto `--delay 0.25` y `--batch-size 50`. Si usas otro plan, override con `--delay 6 --batch-size 5`.
