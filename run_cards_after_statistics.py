#!/usr/bin/env python3
"""
Espera a que termine el backfill de estadísticas (0 pendientes) y luego
ejecuta run_backfill_cards_from_apisports.py --continuous.

Uso:
  python run_cards_after_statistics.py              # espera y luego ejecuta cards
  python run_cards_after_statistics.py --interval 5 # revisar cada 5 min (default 10)
  python run_cards_after_statistics.py --dry-run    # solo verifica pendientes y sale
"""

import argparse
import logging
import subprocess
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
        description="Esperar fin del backfill de estadísticas y luego ejecutar backfill de tarjetas"
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=10,
        help="Minutos entre cada revisión de pendientes (default 10)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Solo muestra pendientes de estadísticas y sale",
    )
    args = parser.parse_args()

    from db import get_historical_matches_pending_statistics_backfill, init_db

    init_db()

    if args.dry_run:
        pending = get_historical_matches_pending_statistics_backfill(limit=99999)
        logger.info("Pendientes de estadísticas: %d", len(pending))
        return 0

    interval_sec = args.interval * 60
    while True:
        pending = get_historical_matches_pending_statistics_backfill(limit=1)
        if not pending:
            logger.info("Backfill de estadísticas terminado. Ejecutando backfill de tarjetas...")
            break
        logger.info("Esperando a que terminen estadísticas (pendientes > 0). Próxima revisión en %d min.", args.interval)
        time.sleep(interval_sec)

    script = Path(__file__).resolve().parent / "run_backfill_cards_from_apisports.py"
    result = subprocess.run(
        [sys.executable, str(script), "--continuous"],
        cwd=str(script.parent),
    )

    # Al finalizar tarjetas: listado columna | registros
    logger.info("Backfill de tarjetas finalizado. Generando listado de columnas...")
    import io
    from datetime import datetime
    from report_historical_columns import run_report

    buf = io.StringIO()
    old_stdout = sys.stdout
    sys.stdout = buf
    try:
        run_report()
        report_text = buf.getvalue()
    finally:
        sys.stdout = old_stdout
    print(report_text)
    out_dir = Path(__file__).resolve().parent / "reports"
    out_dir.mkdir(exist_ok=True)
    out_path = out_dir / f"historical_columns_{datetime.now().strftime('%Y%m%d_%H%M')}.txt"
    out_path.write_text(report_text, encoding="utf-8")
    logger.info("Listado guardado en %s", out_path)

    return result.returncode


if __name__ == "__main__":
    sys.exit(main())
