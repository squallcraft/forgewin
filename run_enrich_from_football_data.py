#!/usr/bin/env python3
"""
Enriquece partidos existentes en historical_matches con datos desde football-data.org.

Regla: nunca duplicar. Solo se actualizan filas ya existentes (mismo fixture_id).
Emparejamiento con FD por homologación de nombres (data_fetcher._team_names_match).

Candidatos: partidos con estadísticas vacías o 0-0 en ligas soportadas por FD
(CL, PL, PD, SA, BL1, FL1, DED, PPL, EL). Incluye filas con o sin api_sports_fixture_id.

Uso:
  python run_enrich_from_football_data.py              # 50 partidos, pausa 2 s
  python run_enrich_from_football_data.py --limit 100 --delay 3
  python run_enrich_from_football_data.py --league CL --dry-run
"""

import argparse
import logging
import sys
import time
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


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Enriquecer historical_matches desde football-data.org (solo UPDATE, sin duplicar)"
    )
    parser.add_argument("--limit", type=int, default=50, help="Máximo partidos a procesar")
    from config import REQUEST_DELAY_SECONDS as FD_DELAY_DEFAULT
    parser.add_argument("--delay", type=float, default=FD_DELAY_DEFAULT, help="Segundos entre partidos (FD es gratuita; default=config, mínimo 6)")
    parser.add_argument("--league", type=str, action="append", dest="leagues",
                        help="Solo estas ligas (ej. --league CL --league PL). Por defecto todas las FD.")
    parser.add_argument("--dry-run", action="store_true", help="Solo listar candidatos, no llamar FD ni actualizar")
    args = parser.parse_args()

    from db import (
        init_db,
        get_historical_matches_for_football_data_enrichment,
        update_historical_statistics,
        update_historical_match_cards,
    )
    from data_fetcher import get_match_statistics_football_data_org

    init_db()

    league_codes = args.leagues if args.leagues else None
    pending = get_historical_matches_for_football_data_enrichment(
        limit=args.limit,
        league_codes=league_codes,
    )
    if not pending:
        logger.info("No hay partidos pendientes de enriquecimiento desde football-data.org.")
        return 0

    logger.info("Candidatos a enriquecer desde FD: %d (ligas: %s)", len(pending), league_codes or "todas FD")
    if args.dry_run:
        for row in pending[:10]:
            logger.info("  [dry-run] fixture_id=%s %s %s vs %s", row["fixture_id"], row["date"][:10], row["home_team_name"], row["away_team_name"])
        if len(pending) > 10:
            logger.info("  ... y %d más.", len(pending) - 10)
        return 0

    updated = 0
    for row in pending:
        fid = row["fixture_id"]
        league_id = row["league_id"]
        date_str = (row["date"] or "")[:10]
        home_name = row.get("home_team_name") or ""
        away_name = row.get("away_team_name") or ""
        if not date_str or (not home_name and not away_name):
            continue
        time.sleep(args.delay)
        try:
            fd = get_match_statistics_football_data_org(league_id, date_str, home_name, away_name)
        except Exception as e:
            logger.debug("FD fixture_id=%s: %s", fid, e)
            continue
        if not fd or fd.get("_match_found_no_stats"):
            continue
        if not (fd.get("home_shots_target") or fd.get("away_shots_target")):
            continue
        try:
            ok = update_historical_statistics(
                fixture_id=fid,
                home_shots=fd.get("home_shots"),
                away_shots=fd.get("away_shots"),
                home_shots_target=fd.get("home_shots_target"),
                away_shots_target=fd.get("away_shots_target"),
                home_corners=fd.get("home_corners"),
                away_corners=fd.get("away_corners"),
                home_fouls=fd.get("home_fouls"),
                away_fouls=fd.get("away_fouls"),
                home_offsides=fd.get("home_offsides"),
                away_offsides=fd.get("away_offsides"),
            )
            if ok:
                updated += 1
                logger.info("Enriquecido fixture_id=%s %s vs %s (FD)", fid, home_name, away_name)
            if fd.get("home_yellow") is not None or fd.get("away_yellow") is not None or fd.get("home_red") is not None or fd.get("away_red") is not None:
                try:
                    update_historical_match_cards(
                        fid,
                        home_yellow=fd.get("home_yellow"),
                        away_yellow=fd.get("away_yellow"),
                        home_red=fd.get("home_red"),
                        away_red=fd.get("away_red"),
                    )
                except Exception:
                    pass
        except Exception as e:
            logger.warning("Error actualizando fixture_id=%s: %s", fid, e)

    logger.info("Enriquecidos desde football-data.org: %d de %d.", updated, len(pending))
    return 0


if __name__ == "__main__":
    sys.exit(main())
