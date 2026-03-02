#!/usr/bin/env python3
"""
Migración única: partidos de ligas europeas con date >= 2025-08-01 y season=2025
pasan a season=2026 (temporada 2025/26 = año fin 2026).

Así el backfill de attendance/referee pide a API-Sports season=2026 y empareja correctamente.

Uso: python scripts/fix_season_european_2025_26.py [--dry-run]
"""

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


# Ligas europeas (ago–may). CLI no.
EUROPEAN_LEAGUES = frozenset(
    {"PL", "PD", "SA", "BL1", "FL1", "CL", "EL", "DED", "PPL", "ELC", "EL1"}
)


def main():
    from db import get_connection, init_db

    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Solo mostrar qué se actualizaría")
    args = parser.parse_args()

    init_db()
    placeholders = ",".join("?" for _ in EUROPEAN_LEAGUES)
    with get_connection() as conn:
        c = conn.cursor()
        c.execute(
            """SELECT league_id, season, date, COUNT(*) as n
               FROM historical_matches
               WHERE league_id IN ({}) AND season = 2025 AND date >= '2025-08-01'
               GROUP BY league_id, season, date
               ORDER BY league_id, date""".format(placeholders),
            tuple(EUROPEAN_LEAGUES),
        )
        rows = c.fetchall()
        if not rows:
            print("No hay filas con league europea, season=2025 y date >= 2025-08-01.")
            return 0

        total = sum(r[3] for r in rows)
        print(f"Filas a actualizar (season 2025 → 2026): {total}")
        for r in rows[:20]:
            print(f"  {r[0]} {r[2]} ({r[3]} partidos)")
        if len(rows) > 20:
            print(f"  ... y {len(rows) - 20} fechas más")

        if args.dry_run:
            print("Dry-run: no se escribió nada.")
            return 0

        c.execute(
            """UPDATE historical_matches
               SET season = 2026
               WHERE league_id IN ({}) AND season = 2025 AND date >= '2025-08-01'""".format(
                placeholders
            ),
            tuple(EUROPEAN_LEAGUES),
        )
        print(f"Actualizadas {c.rowcount} filas.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
