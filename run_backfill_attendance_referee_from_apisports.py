#!/usr/bin/env python3
"""
Backfill de attendance (asistencia) y referee (árbitro) en TODA la data histórica
usando API-Sports como fuente. Empareja por liga, temporada, fecha y equipos (nombres normalizados).

No se limita a partidos que ya tenían api_sports_fixture_id: rellena cualquier partido
de historical_matches que pueda emparejarse con un fixture de API-Sports.

Uso (1 petición por liga+temporada; respetar cuota API):
  python run_backfill_attendance_referee_from_apisports.py           # todas las ligas/temporadas con datos faltantes
  python run_backfill_attendance_referee_from_apisports.py --dry-run  # solo lista qué se procesaría
  python run_backfill_attendance_referee_from_apisports.py --league BL1 --season 2023
  python run_backfill_attendance_referee_from_apisports.py --delay 8
"""

import argparse
import logging
import sys
import time
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def _match_key(date: str, home: str, away: str, league_id: str, normalize_team_name) -> tuple:
    """Clave para emparejar: (date, nombre canónico local, nombre canónico visitante)."""
    h = (normalize_team_name(home, league_id) or (home or "").strip()).lower()
    a = (normalize_team_name(away, league_id) or (away or "").strip()).lower()
    return (date, h, a)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Backfill asistencia y árbitro en toda la data histórica desde API-Sports (match por liga+temporada+fecha+equipos)"
    )
    parser.add_argument(
        "--league",
        type=str,
        default=None,
        help="Solo procesar esta liga (ej. BL1, SA). Por defecto todas.",
    )
    parser.add_argument(
        "--season",
        type=int,
        default=None,
        help="Solo procesar esta temporada (año fin, ej. 2023). Por defecto todas.",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.25,
        help="Segundos entre peticiones (default 0.25 para Pro). Override solo si usas otro plan",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Solo lista ligas/temporadas y número de filas a emparejar, sin llamar API ni actualizar",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        dest="all_seasons",
        help="Procesar todas las ligas/temporadas (no solo donde falta attendance/referee)",
    )
    args = parser.parse_args()

    from api_sports_fetcher import (
        get_finished_fixtures_for_historical,
        _season_date_range,
        _season_from_date,
        LEAGUE_CODE_TO_ID,
        API_KEY,
    )
    from db import (
        get_distinct_league_season_for_backfill,
        get_historical_match_rows_for_backfill,
        update_historical_attendance_referee,
        normalize_team_name,
        init_db,
    )

    init_db()
    if not API_KEY:
        logger.error("API_FOOTBALL_KEY no configurada en .env")
        return 1

    # Pares (league_id, season) a procesar: BD + siempre temporada actual por liga
    pairs = get_distinct_league_season_for_backfill(only_missing=not args.all_seasons)
    seen = set(pairs)
    today_str = date.today().isoformat()
    for lid in LEAGUE_CODE_TO_ID:
        current_season = _season_from_date(today_str, lid)
        if (lid, current_season) not in seen:
            pairs.append((lid, current_season))
            seen.add((lid, current_season))
    if args.league is not None:
        pairs = [(lid, s) for lid, s in pairs if lid == args.league]
    if args.season is not None:
        pairs = [(lid, s) for lid, s in pairs if s == args.season]
    # Solo ligas que API-Sports conoce
    pairs = [(lid, s) for lid, s in pairs if lid in LEAGUE_CODE_TO_ID]

    if not pairs:
        logger.info("No hay pares (liga, temporada) a procesar.")
        return 0

    if args.dry_run:
        total_rows = 0
        for league_id, season in pairs:
            rows = get_historical_match_rows_for_backfill(league_id, season)
            total_rows += len(rows)
            logger.info("%s season %s: %d partidos en BD", league_id, season, len(rows))
        logger.info("Total: %d partidos en %d pares (liga, temporada). Ejecuta sin --dry-run para rellenar.", total_rows, len(pairs))
        return 0

    total_updated = 0
    for idx, (league_id, season) in enumerate(pairs):
        date_from, date_to = _season_date_range(season, league_id)
        logger.info("[%d/%d] %s season %s (%s a %s)", idx + 1, len(pairs), league_id, season, date_from, date_to)

        try:
            api_rows = get_finished_fixtures_for_historical(league_id, date_from, date_to)
        except Exception as e:
            logger.warning("Error API %s %s: %s", league_id, season, e)
            time.sleep(args.delay)
            continue

        # Mapa API: (date, norm_home, norm_away) -> {attendance, referee, api_sports_fixture_id, home_team_id, away_team_id}
        api_map = {}
        for r in api_rows:
            date_str = (r.get("date") or "")[:10]
            if len(date_str) < 10:
                continue
            home = (r.get("home_team_name") or "").strip()
            away = (r.get("away_team_name") or "").strip()
            key = _match_key(date_str, home, away, league_id, normalize_team_name)
            api_map[key] = {
                "attendance": r.get("attendance"),
                "referee": (r.get("referee") or "").strip() or None,
                "api_sports_fixture_id": r.get("api_sports_fixture_id"),
                "home_team_id": r.get("home_team_id"),
                "away_team_id": r.get("away_team_id"),
            }

        our_rows = get_historical_match_rows_for_backfill(league_id, season)
        updated_here = 0
        for row in our_rows:
            fixture_id = row["fixture_id"]
            date_str = (row.get("date") or "")[:10]
            home = (row.get("home_team_name") or "").strip()
            away = (row.get("away_team_name") or "").strip()
            key = _match_key(date_str, home, away, league_id, normalize_team_name)
            data = api_map.get(key)
            if not data:
                continue
            att = data.get("attendance")
            ref = data.get("referee")
            api_fid = data.get("api_sports_fixture_id")
            hid = data.get("home_team_id")
            aid = data.get("away_team_id")
            if att is not None or ref or api_fid is not None or hid is not None or aid is not None:
                if update_historical_attendance_referee(
                    fixture_id=fixture_id,
                    attendance=att,
                    referee=ref,
                    api_sports_fixture_id=api_fid,
                    home_team_id=hid,
                    away_team_id=aid,
                ):
                    updated_here += 1
                    total_updated += 1

        logger.info("  → %d partidos actualizados (de %d en BD, %d de API)", updated_here, len(our_rows), len(api_rows))
        time.sleep(args.delay)

    logger.info("Total actualizados en esta ejecución: %d", total_updated)
    return 0


if __name__ == "__main__":
    sys.exit(main())
