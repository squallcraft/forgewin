# Por qué attendance y referee siguen vacíos

## Lo que se hizo en la petición anterior

1. **Cargas nuevas desde API-Sports**  
   En `get_finished_fixtures_for_historical` (api_sports_fetcher) se extraen `attendance` y `referee` de la respuesta de la API y se envían a `upsert_historical_match`.  
   En `data_downloader`: al cargar desde API-Sports (`fetch_recent_finished_from_api_sports` y `fetch_and_load_historical_cl_el`) se pasan `attendance` y `referee` al upsert y se normalizan los nombres de equipos.

2. **Backfill para filas que ya tienen API-Sports**  
   Se añadió `run_backfill_attendance_referee_from_apisports.py`, que actualiza `attendance` y `referee` **solo** en filas que ya tienen `api_sports_fixture_id` (es decir, partidos cargados desde API-Sports).

3. **No se rellenaron los 111k partidos actuales** porque:
   - Esas filas tienen **`api_sports_fixture_id = NULL`**: la base actual se llenó desde **football-data.co.uk (CSV)**, no desde API-Sports.
   - El backfill solo actúa sobre filas con `api_sports_fixture_id` → con 0 filas en ese caso, no se actualiza nada.
   - Los CSV de football-data.co.uk para Bundesliga (D1) y Serie A (I1) **no traen columnas "Attendance" ni "Referee"** en la cabecera, así que aunque el parser las mapee, esos datos no existen en el CSV y nunca se guardaron.

## Resumen

| Qué se hizo | Efecto |
|-------------|--------|
| Extraer attendance/referee al cargar desde API-Sports | Solo afecta a **cargas nuevas** desde API-Sports (delta semanal, CL/EL). |
| Backfill por `api_sports_fixture_id` | Solo afecta a filas que **ya tengan** ese id (hoy hay 0). |
| Carga desde CSV (football-data) | Los CSV de BL1/SA no incluyen Attendance ni Referee; no hay dato que cargar. |

**Corrección (backfill para toda la data histórica):** El script `run_backfill_attendance_referee_from_apisports.py` fue corregido. Ahora rellena **toda** la data histórica usando API-Sports como fuente: para cada (liga, temporada) pide los partidos a la API, empareja con nuestros partidos por fecha y equipos (nombres normalizados) y actualiza attendance, referee y opcionalmente api_sports_fixture_id. No se limita a filas que ya tenían api_sports_fixture_id.

**Uso:** `python run_backfill_attendance_referee_from_apisports.py` (sin --dry-run). Opciones: `--league BL1`, `--season 2023`, `--delay 6`, `--dry-run`, `--all`.
