#!/usr/bin/env python3
"""
Backfill de attendance (asistencia) y referee (árbitro) usando GET /fixtures?id=
por cada partido con api_sports_fixture_id y sin attendance/referee.

Aprovecha el plan Pro/Ultra: 1 llamada por partido, resultado directo sin matching.
~54k partidos pendientes × 0.25s ≈ 3.7 horas con plan Pro.

Uso:
  python run_backfill_attendance_via_fixture_id.py              # 50 partidos
  python run_backfill_attendance_via_fixture_id.py --continuous # hasta vaciar
  python run_backfill_attendance_via_fixture_id.py --dry-run
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
        description="Backfill attendance y referee vía GET /fixtures?id= (1 call/partido)"
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

    from api_sports_fetcher import get_fixture_by_id, API_KEY
    from db import get_connection, init_db, update_historical_attendance_referee

    init_db()
    if not API_KEY:
        logger.error("API_FOOTBALL_KEY no configurada en .env")
        return 1

    def get_pending(limit: int):
        with get_connection() as conn:
            return conn.execute(
                """SELECT fixture_id, api_sports_fixture_id
                   FROM historical_matches
                   WHERE api_sports_fixture_id IS NOT NULL
                     AND (attendance IS NULL OR referee IS NULL OR referee = '')
                   ORDER BY season DESC, date DESC
                   LIMIT ?""",
                (limit,),
            ).fetchall()

    def count_pending() -> int:
        with get_connection() as conn:
            return conn.execute(
                """SELECT COUNT(*) FROM historical_matches
                   WHERE api_sports_fixture_id IS NOT NULL
                     AND (attendance IS NULL OR referee IS NULL OR referee = '')""",
            ).fetchone()[0]

    pending_count = count_pending()
    if args.dry_run:
        logger.info("Partidos pendientes (attendance/referee): %d", pending_count)
        logger.info("Calls estimadas: %d | Tiempo (%.2fs delay): %.1f h",
                    pending_count, args.delay, pending_count * args.delay / 3600)
        return 0

    logger.info("Backfill attendance+referee vía fixture ID — %d pendientes", pending_count)
    total_updated = 0
    batch_num = 0
    consecutive_empty = 0

    while True:
        rows = get_pending(args.batch_size)
        if not rows:
            logger.info("Cola vacía — backfill completado.")
            break

        batch_num += 1
        updated_this = 0
        for fixture_id, api_fid in rows:
            try:
                data = get_fixture_by_id(int(api_fid))
                time.sleep(args.delay)
                if not data:
                    continue
                att = data.get("attendance")
                ref = data.get("referee")
                # Solo actualizar si hay dato nuevo: attendance tiene prioridad
                # (referee puede ya estar en BD desde otra fuente)
                if att is not None:
                    if update_historical_attendance_referee(
                        fixture_id=fixture_id,
                        attendance=att,
                        referee=ref,
                    ):
                        updated_this += 1
                        total_updated += 1
                elif ref:
                    # Solo referee sin attendance: escribir igualmente (puede ser nuevo)
                    update_historical_attendance_referee(
                        fixture_id=fixture_id,
                        attendance=None,
                        referee=ref,
                    )
            except Exception as e:
                logger.warning("Error fixture %s: %s", api_fid, e)

        remaining = count_pending()
        logger.info("Lote %d: attendance_nuevos=%d | Total=%d | Pendientes=%d",
                    batch_num, updated_this, total_updated, remaining)

        if not args.continuous:
            break
        if remaining == 0:
            logger.info("Cola vacía — backfill completado.")
            break

        if updated_this == 0:
            consecutive_empty += 1
            if consecutive_empty >= args.max_empty_batches:
                logger.info(
                    "Deteniendo: %d lotes consecutivos sin datos de attendance. "
                    "Quedan %d pendientes (ligas/temporadas sin datos en API-Sports).",
                    consecutive_empty, remaining,
                )
                break
        else:
            consecutive_empty = 0

        time.sleep(max(2.0, args.delay * 4))

    logger.info("COMPLETADO — attendance actualizados: %d", total_updated)
    return 0


if __name__ == "__main__":
    sys.exit(main())
