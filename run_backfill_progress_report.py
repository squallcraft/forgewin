#!/usr/bin/env python3
"""
Revisión del progreso del backfill de estadísticas cada N minutos.
Usa la BD del proyecto (forgewin.db o FOOTBALL_DB_PATH) y opcionalmente
el archivo de log del backfill para mostrar "total actualizados en esta ejecución".

Uso:
  python run_backfill_progress_report.py              # cada 10 min, indefinido
  python run_backfill_progress_report.py --interval 5 # cada 5 min
  python run_backfill_progress_report.py --once       # una sola revisión y sale
"""

import argparse
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parent))

INTERVAL_SECONDS = 600  # 10 min


def get_db_counts():
    """Cuenta partidos con estadísticas y elegibles para backfill."""
    try:
        from db import get_connection, init_db

        init_db()
        with get_connection() as conn:
            c = conn.cursor()
            c.execute(
                """SELECT COUNT(*) FROM historical_matches WHERE home_shots IS NOT NULL"""
            )
            con_stats = c.fetchone()[0]
            c.execute(
                """SELECT COUNT(*) FROM historical_matches
                   WHERE api_sports_fixture_id IS NOT NULL
                     AND home_team_id IS NOT NULL AND away_team_id IS NOT NULL
                     AND home_shots IS NULL"""
            )
            pendientes = c.fetchone()[0]
            c.execute(
                """SELECT COUNT(*) FROM historical_matches
                   WHERE api_sports_fixture_id IS NOT NULL
                     AND home_team_id IS NOT NULL AND away_team_id IS NOT NULL"""
            )
            elegibles = c.fetchone()[0]
        return {"con_estadisticas": con_stats, "pendientes": pendientes, "elegibles": elegibles}
    except Exception as e:
        return {"error": str(e)}


def get_last_total_from_log(log_path: str) -> Optional[int]:
    """Extrae el último 'total: N' del log del backfill."""
    path = Path(log_path)
    if not path.exists():
        return None
    text = path.read_text(encoding="utf-8", errors="replace")
    matches = re.findall(r"Lote:.*\(total:\s*(\d+)\)", text)
    return int(matches[-1]) if matches else None


def main():
    parser = argparse.ArgumentParser(description="Revisión de progreso del backfill cada N minutos")
    parser.add_argument(
        "--interval",
        type=int,
        default=10,
        help="Minutos entre cada revisión (default 10)",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Una sola revisión y salir",
    )
    parser.add_argument(
        "--log",
        type=str,
        default="",
        help="Ruta al archivo de log del backfill (opcional)",
    )
    args = parser.parse_args()

    interval_sec = args.interval * 60
    log_path = args.log.strip()

    while True:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        counts = get_db_counts()

        if "error" in counts:
            print(f"[{now}] Error BD: {counts['error']}")
        else:
            con = counts["con_estadisticas"]
            pend = counts["pendientes"]
            elig = counts["elegibles"]
            pct = (100 * con / elig) if elig else 0
            log_total = get_last_total_from_log(log_path) if log_path else None

            print(f"[{now}] Con estadísticas: {con} | Pendientes: {pend} | Elegibles: {elig} | {pct:.1f}%")
            if log_total is not None:
                print(f"       (log backfill total en esta ejecución: {log_total})")

        if args.once:
            break
        time.sleep(interval_sec)

    return 0


if __name__ == "__main__":
    sys.exit(main())
