#!/usr/bin/env python3
"""
Revisa completitud de datos en historical_matches.
Detecta gaps: partidos con api_sports_fixture_id pero sin offsides, attendance, referee, stats, xG, cards.

Útil para:
  - Monitoreo diario tras run_production_daily
  - Alertas si los gaps superan umbrales
  - Auditoría previa a backfills

Uso:
  python scripts/check_data_completeness.py
  python scripts/check_data_completeness.py --json
  python scripts/check_data_completeness.py --log-to-db
  python scripts/check_data_completeness.py --fail-threshold 10000   # exit 1 si >10k sin offsides
"""

import argparse
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def get_completeness_counts() -> dict:
    """Cuenta totales y gaps. Parte con api_sports_fixture_id (vinculados a API-Sports)."""
    from db import get_connection

    with get_connection() as conn:
        # Base: partidos con api_sports_fixture_id (se pueden enriquecer)
        base = conn.execute(
            "SELECT COUNT(*) FROM historical_matches WHERE api_sports_fixture_id IS NOT NULL"
        ).fetchone()[0]

        total = conn.execute("SELECT COUNT(*) FROM historical_matches").fetchone()[0]
        max_date = conn.execute("SELECT MAX(date) FROM historical_matches").fetchone()[0]

        # Gaps (solo entre los que tienen api_sports_fixture_id)
        sin_stats = conn.execute(
            """SELECT COUNT(*) FROM historical_matches
               WHERE api_sports_fixture_id IS NOT NULL
                 AND (home_shots IS NULL OR home_shots_target IS NULL)
            """
        ).fetchone()[0]

        sin_offsides = conn.execute(
            """SELECT COUNT(*) FROM historical_matches
               WHERE api_sports_fixture_id IS NOT NULL
                 AND (home_offsides IS NULL OR away_offsides IS NULL)
            """
        ).fetchone()[0]

        sin_attendance_referee = conn.execute(
            """SELECT COUNT(*) FROM historical_matches
               WHERE api_sports_fixture_id IS NOT NULL
                 AND (attendance IS NULL OR referee IS NULL OR referee = '')
            """
        ).fetchone()[0]

        sin_xg = conn.execute(
            """SELECT COUNT(*) FROM historical_matches
               WHERE api_sports_fixture_id IS NOT NULL
                 AND (home_xg IS NULL AND away_xg IS NULL)
                 AND status = 'FT'
            """
        ).fetchone()[0]

        sin_cards = conn.execute(
            """SELECT COUNT(*) FROM historical_matches
               WHERE api_sports_fixture_id IS NOT NULL
                 AND home_yellow IS NULL AND away_yellow IS NULL
                 AND home_red IS NULL AND away_red IS NULL
            """
        ).fetchone()[0]

    return {
        "checked_at": datetime.utcnow().isoformat() + "Z",
        "total_matches": total,
        "with_api_sports_fixture_id": base,
        "max_date": max_date,
        "gaps": {
            "sin_estadisticas": sin_stats,
            "sin_offsides": sin_offsides,
            "sin_attendance_referee": sin_attendance_referee,
            "sin_xg": sin_xg,
            "sin_cards": sin_cards,
        },
    }


def log_to_db(data: dict) -> None:
    """Persiste el reporte en data_completeness_log (auditoría)."""
    from db import get_connection, init_db

    init_db()
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO data_completeness_log (checked_at, total_matches, with_api_id, max_date,
                sin_stats, sin_offsides, sin_attendance_referee, sin_xg, sin_cards)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                data["checked_at"],
                data["total_matches"],
                data["with_api_sports_fixture_id"],
                data["max_date"],
                data["gaps"]["sin_estadisticas"],
                data["gaps"]["sin_offsides"],
                data["gaps"]["sin_attendance_referee"],
                data["gaps"]["sin_xg"],
                data["gaps"]["sin_cards"],
            ),
        )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Revisa completitud de datos en historical_matches"
    )
    parser.add_argument("--json", action="store_true", help="Salida en JSON")
    parser.add_argument("--log-to-db", action="store_true", help="Guardar reporte en data_completeness_log")
    parser.add_argument(
        "--fail-threshold",
        type=int,
        default=None,
        help="Exit 1 si algún gap supera este número",
    )
    args = parser.parse_args()

    from db import init_db

    init_db()

    data = get_completeness_counts()

    if args.log_to_db:
        try:
            log_to_db(data)
            logger.info("Reporte guardado en data_completeness_log")
        except Exception as e:
            logger.warning("No se pudo guardar en BD (¿tabla existe?): %s", e)

    if args.json:
        print(json.dumps(data, indent=2))
    else:
        g = data["gaps"]
        logger.info("=" * 50)
        logger.info("Completitud de datos (historical_matches)")
        logger.info("  Total partidos:            %d", data["total_matches"])
        logger.info("  Con api_sports_fixture_id: %d", data["with_api_sports_fixture_id"])
        logger.info("  Último partido:            %s", data["max_date"])
        logger.info("  Gaps:")
        logger.info("    Sin estadísticas:        %d", g["sin_estadisticas"])
        logger.info("    Sin offsides:            %d", g["sin_offsides"])
        logger.info("    Sin attendance/referee:   %d", g["sin_attendance_referee"])
        logger.info("    Sin xG:                  %d", g["sin_xg"])
        logger.info("    Sin tarjetas:            %d", g["sin_cards"])
        logger.info("=" * 50)

    if args.fail_threshold is not None:
        for name, count in g.items():
            if count > args.fail_threshold:
                logger.error("Gap %s supera umbral: %d > %d", name, count, args.fail_threshold)
                return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
