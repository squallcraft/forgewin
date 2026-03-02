#!/usr/bin/env python3
"""
Backfill de offsides (y xG si faltan) para partidos con api_sports_fixture_id
que tienen stats (shots, etc.) pero home_offsides/away_offsides vacíos.

El backfill de estadísticas solo procesa filas con home_shots IS NULL, así que
las ~66k con shots pero sin offsides nunca se rellenaban. Este script los cubre.

1 llamada /statistics por partido. ~66k pendientes × 0.25s ≈ 4.6 h con Pro.

Uso:
  python run_backfill_offsides_from_apisports.py              # 50 partidos
  python run_backfill_offsides_from_apisports.py --continuous
  python run_backfill_offsides_from_apisports.py --dry-run
"""

import argparse
import logging
import sys
import time
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Backfill offsides (y xG si faltan) para filas con stats pero sin offsides"
    )
    parser.add_argument("--batch-size", type=int, default=50)
    parser.add_argument("--delay", type=float, default=0.25)
    parser.add_argument("--continuous", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--max-empty-batches", type=int, default=5,
        help="Salir tras N lotes consecutivos sin actualizaciones (default 5). Evita loop infinito en ligas sin datos.",
    )
    args = parser.parse_args()

    from api_sports_fetcher import get_fixture_statistics_both, API_KEY
    from db import get_connection, init_db

    init_db()
    if not API_KEY:
        logger.error("API_FOOTBALL_KEY no configurada en .env")
        return 1

    def get_pending(limit: int):
        with get_connection() as conn:
            rows = conn.execute(
                """SELECT fixture_id, api_sports_fixture_id, league_id, date,
                          home_team_name, away_team_name, home_xg, away_xg
                   FROM historical_matches
                   WHERE api_sports_fixture_id IS NOT NULL
                     AND (home_offsides IS NULL OR away_offsides IS NULL)
                   ORDER BY season DESC, date DESC
                   LIMIT ?""",
                (limit,),
            ).fetchall()
        return [{"fixture_id": r[0], "api_sports_fixture_id": r[1], "league_id": r[2],
                 "date": r[3], "home_team_name": r[4], "away_team_name": r[5],
                 "home_xg": r[6], "away_xg": r[7]} for r in rows]

    def count_pending() -> int:
        with get_connection() as conn:
            return conn.execute(
                """SELECT COUNT(*) FROM historical_matches
                   WHERE api_sports_fixture_id IS NOT NULL
                     AND (home_offsides IS NULL OR away_offsides IS NULL)""",
            ).fetchone()[0]

    def update_offsides_only(fixture_id: int, home_offs: Optional[int], away_offs: Optional[int]) -> bool:
        if home_offs is None and away_offs is None:
            return False
        with get_connection() as conn:
            c = conn.cursor()
            c.execute(
                """UPDATE historical_matches SET
                       home_offsides = COALESCE(?, home_offsides),
                       away_offsides = COALESCE(?, away_offsides)
                   WHERE fixture_id = ?""",
                (home_offs, away_offs, fixture_id),
            )
            return c.rowcount > 0

    def update_xg(fixture_id: int, home_xg: Optional[float], away_xg: Optional[float]):
        if home_xg is None and away_xg is None:
            return
        with get_connection() as conn:
            conn.execute(
                """UPDATE historical_matches
                   SET home_xg = COALESCE(?, home_xg),
                       away_xg = COALESCE(?, away_xg)
                   WHERE fixture_id = ?""",
                (home_xg, away_xg, fixture_id),
            )

    pending_count = count_pending()
    if args.dry_run:
        logger.info("Partidos pendientes (offsides): %d", pending_count)
        logger.info("Calls estimadas: %d | Tiempo (%.2fs delay): %.1f h",
                    pending_count, args.delay, pending_count * args.delay / 3600)
        return 0

    logger.info("Backfill offsides — %d pendientes", pending_count)
    total_offsides = 0
    total_xg = 0
    batch_num = 0
    consecutive_empty = 0

    while True:
        pending = get_pending(args.batch_size)
        if not pending:
            logger.info("Cola vacía — backfill completado.")
            break

        batch_num += 1
        offs_this = 0
        xg_this = 0
        for m in pending:
            fid = m["fixture_id"]
            api_fid = m["api_sports_fixture_id"]
            try:
                both = get_fixture_statistics_both(api_fid)
                time.sleep(args.delay)
                if not both:
                    continue
                h = both.get("home") or {}
                a = both.get("away") or {}
                home_offs = h.get("offsides")
                away_offs = a.get("offsides")
                if home_offs is not None or away_offs is not None:
                    if update_offsides_only(fid, home_offs, away_offs):
                        offs_this += 1
                        total_offsides += 1
                # xG si faltan
                if m["home_xg"] is None or m["away_xg"] is None:
                    h_xg, a_xg = h.get("xg"), a.get("xg")
                    if h_xg is not None or a_xg is not None:
                        update_xg(fid, h_xg, a_xg)
                        xg_this += 1
                        total_xg += 1
            except Exception as e:
                logger.warning("Error %s (%s vs %s): %s",
                               api_fid, m.get("home_team_name"), m.get("away_team_name"), e)

        remaining = count_pending()
        logger.info("Lote %d: offsides=%d, xG=%d | Total offs=%d, xG=%d | Pendientes=%d",
                    batch_num, offs_this, xg_this, total_offsides, total_xg, remaining)

        if not args.continuous:
            break
        if remaining == 0:
            logger.info("Cola vacía — backfill completado.")
            break

        if offs_this == 0 and xg_this == 0:
            consecutive_empty += 1
            if consecutive_empty >= args.max_empty_batches:
                logger.info(
                    "Deteniendo: %d lotes consecutivos sin datos de offsides/xG. "
                    "Quedan %d pendientes (ligas/temporadas sin datos en API-Sports).",
                    consecutive_empty, remaining,
                )
                break
        else:
            consecutive_empty = 0

        time.sleep(max(2.0, args.delay * 4))

    logger.info("COMPLETADO — offsides: %d, xG: %d", total_offsides, total_xg)
    return 0


if __name__ == "__main__":
    sys.exit(main())
