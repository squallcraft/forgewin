#!/usr/bin/env python3
"""
Actualiza datos de Champions League en historical_matches desde football-data.org (2025–2026).

Fuente: football-data.org v4 (FOOTBALL_DATA_ORG_TOKEN en .env).
Sin duplicar: solo se actualizan filas existentes; emparejo por homologación de nombres
(data_fetcher._team_names_match). Si no existe la fila en BD, no se inserta.

Uso:
  python run_update_cl_football_data.py
  python run_update_cl_football_data.py --from 2025-06-01 --to 2026-06-30
  python run_update_cl_football_data.py --dry-run
"""

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent / ".env")
except ImportError:
    pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def _season_from_date(date_str: str) -> int:
    """Año de temporada europea (fin): julio 2025 -> 2026, junio 2025 -> 2025."""
    if not date_str or len(date_str) < 10:
        return 2025
    try:
        y = int(date_str[:4])
        m = int(date_str[5:7])
        return y + 1 if m >= 7 else y
    except (ValueError, TypeError):
        return 2025


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Actualiza Champions League en historical_matches desde football-data.org (2025–2026)"
    )
    parser.add_argument("--from", dest="date_from", default="2025-01-01", help="Fecha inicio YYYY-MM-DD")
    parser.add_argument("--to", dest="date_to", default="2026-12-31", help="Fecha fin YYYY-MM-DD")
    parser.add_argument("--dry-run", action="store_true", help="Solo listar partidos que se actualizarían, no escribir BD")
    args = parser.parse_args()

    from data_fetcher import fetch_cl_matches_football_data_org, _team_names_match
    from db import init_db, get_connection, upsert_historical_match, backfill_ftr_from_goals

    init_db()

    date_from = args.date_from[:10]
    date_to = args.date_to[:10]
    logger.info("Champions League (football-data.org): %s a %s (solo actualizar existentes)", date_from, date_to)

    # 1) Cargar partidos CL existentes en ese rango para emparejar por homologación de nombres
    bd_rows: list = []
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            """SELECT fixture_id, date, home_team_name, away_team_name
               FROM historical_matches
               WHERE league_id = 'CL' AND date >= ? AND date <= ?""",
            (date_from, date_to + " 23:59:59"),
        )
        for row in cur.fetchall():
            bd_rows.append({
                "fixture_id": row[0],
                "date": row[1],
                "home_team_name": row[2] or "",
                "away_team_name": row[3] or "",
            })
    logger.info("Partidos CL ya en BD en el rango: %d", len(bd_rows))

    # 2) Obtener partidos desde football-data.org
    matches = fetch_cl_matches_football_data_org(date_from, date_to)
    if not matches:
        logger.warning("No se obtuvieron partidos de football-data.org (¿token o fechas?)")
        return 0 if args.dry_run else 1

    # 3) Solo actualizar filas existentes: emparejar por fecha + homologación de nombres
    updated = 0
    skipped = 0
    for m in matches:
        home = (m.get("homeTeam") or {})
        away = (m.get("awayTeam") or {})
        home_name = home.get("name") or ""
        away_name = away.get("name") or ""
        if not home_name.strip() and not away_name.strip():
            continue
        utc = (m.get("utcDate") or "")[:19].replace("Z", "").strip()
        date_iso = utc[:10] if utc else ""
        home_id = home.get("id")
        away_id = away.get("id")
        # Buscar fila en BD con misma fecha y equipos (homologación)
        fixture_id = None
        for row in bd_rows:
            if (row["date"] or "")[:10] != date_iso:
                continue
            if _team_names_match(
                row["home_team_name"], row["away_team_name"],
                home_name, away_name,
            ):
                fixture_id = row["fixture_id"]
                break
        if fixture_id is None:
            skipped += 1
            if args.dry_run:
                logger.info("  [dry-run] sin fila en BD: %s %s vs %s", date_iso, home_name, away_name)
            continue
        updated += 1

        score = m.get("score") or {}
        ft = score.get("fullTime") or score.get("score") or {}
        home_goals = ft.get("homeTeam") if ft.get("homeTeam") is not None else ft.get("home")
        away_goals = ft.get("awayTeam") if ft.get("awayTeam") is not None else ft.get("away")
        if home_goals is None and away_goals is None:
            home_goals, away_goals = 0, 0
        home_goals = int(home_goals) if home_goals is not None else 0
        away_goals = int(away_goals) if away_goals is not None else 0
        status = m.get("status") or "SCHEDULED"
        if status == "FINISHED" or (home_goals is not None and away_goals is not None and (home_goals > 0 or away_goals > 0)):
            status = "FINISHED"
        season = _season_from_date(date_iso)
        date_for_db = utc if utc else (date_iso + " 00:00:00")

        if args.dry_run:
            logger.info(
                "  [dry-run] %s %s vs %s -> fixture_id=%s goals=%s-%s",
                date_iso, home_name, away_name, fixture_id, home_goals, away_goals,
            )
            continue

        try:
            upsert_historical_match(
                fixture_id=fixture_id,
                date=date_for_db,
                league_id="CL",
                home_goals=home_goals,
                away_goals=away_goals,
                season=season,
                status=status,
                home_team_id=home_id,
                away_team_id=away_id,
                home_team_name=home_name,
                away_team_name=away_name,
            )
        except Exception as e:
            logger.warning("Error actualizar %s %s vs %s: %s", date_iso, home_name, away_name, e)

    if args.dry_run:
        logger.info("DRY-RUN: %d partidos FD, %d con fila en BD (no se escribió).", len(matches), updated)
        return 0

    logger.info("Champions League: %d actualizados, %d sin fila en BD (no insertados).", updated, skipped)
    n_ftr = backfill_ftr_from_goals()
    if n_ftr:
        logger.info("Backfill FTR desde goles: %d filas.", n_ftr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
