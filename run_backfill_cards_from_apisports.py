#!/usr/bin/env python3
"""
Backfill de tarjetas amarillas/rojas en historical_matches desde API-Sports /statistics.
Solo procesa partidos con api_sports_fixture_id + home_team_id + away_team_id.

Defaults para API-Sports Pro (300 req/min). Otro plan: --delay 6 --batch-size 5.
  python run_backfill_cards_from_apisports.py                    # 50 partidos, 0.25 s entre llamadas (Pro)
  python run_backfill_cards_from_apisports.py --continuous
  python run_backfill_cards_from_apisports.py --dry-run
"""

import argparse
import logging
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Backfill tarjetas amarillas/rojas desde API-Sports en historical_matches"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=50,
        help="Partidos por lote (default 50; Pro 300 req/min)",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.25,
        help="Segundos entre peticiones (default 0.25 para Pro)",
    )
    parser.add_argument(
        "--continuous",
        action="store_true",
        help="Seguir hasta que no queden pendientes (pausas largas entre lotes)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Solo muestra cuántos partidos pendientes y sale",
    )
    args = parser.parse_args()

    from api_sports_fetcher import get_fixture_statistics, API_KEY
    from db import (
        get_historical_matches_pending_card_backfill,
        update_historical_match_cards,
        init_db,
    )

    init_db()
    if not API_KEY:
        logger.error("API_FOOTBALL_KEY no configurada en .env")
        return 1

    if args.dry_run:
        pending = get_historical_matches_pending_card_backfill(limit=99999)
        logger.info("Partidos pendientes de backfill de tarjetas: %d", len(pending))
        return 0

    total_updated = 0
    batch_delay = max(2.0, min(60, args.delay * 4))  # pausa entre lotes

    while True:
        pending = get_historical_matches_pending_card_backfill(limit=args.batch_size)
        if not pending:
            logger.info("No quedan partidos pendientes.")
            break

        logger.info("Procesando lote de %d partidos (delay %.1f s entre peticiones)", len(pending), args.delay)
        updated_this_batch = 0

        for m in pending:
            fid = m["fixture_id"]
            api_fid = m["api_sports_fixture_id"]
            home_tid = m["home_team_id"]
            away_tid = m["away_team_id"]
            if not api_fid or not home_tid or not away_tid:
                continue
            try:
                home_stats = get_fixture_statistics(api_fid, home_tid)
                time.sleep(args.delay)
                away_stats = get_fixture_statistics(api_fid, away_tid)
                time.sleep(args.delay)

                home_y = (home_stats or {}).get("yellow_cards")
                home_r = (home_stats or {}).get("red_cards")
                away_y = (away_stats or {}).get("yellow_cards")
                away_r = (away_stats or {}).get("red_cards")

                if home_y is not None or away_y is not None or home_r is not None or away_r is not None:
                    update_historical_match_cards(
                        fixture_id=fid,
                        home_yellow=home_y,
                        away_yellow=away_y,
                        home_red=home_r,
                        away_red=away_r,
                    )
                    updated_this_batch += 1
                    total_updated += 1
                    logger.debug("fixture %s: HY=%s AY=%s HR=%s AR=%s", fid, home_y, away_y, home_r, away_r)
            except Exception as e:
                logger.warning("Error fixture %s: %s", fid, e)

        logger.info("Lote completado: %d partidos actualizados (total esta ejecución: %d)", updated_this_batch, total_updated)

        if not args.continuous:
            break
        logger.info("Pausa %.1f s antes del siguiente lote...", batch_delay)
        time.sleep(batch_delay)

    return 0


if __name__ == "__main__":
    sys.exit(main())
