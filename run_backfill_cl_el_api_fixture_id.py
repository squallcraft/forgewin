#!/usr/bin/env python3
"""
Rellena api_sports_fixture_id en partidos de Champions League y Europa League
que fueron cargados desde API-Sports pero quedaron con api_sports_fixture_id NULL.
Sin este ID, el backfill de estadísticas (tiros, córners, árbitro, HT/FT) no puede ejecutarse.

Uso:
  python run_backfill_cl_el_api_fixture_id.py

Después, ejecuta el backfill de estadísticas y tarjetas para que CL/EL tengan datos recientes:
  python run_backfill_statistics_from_apisports.py
  python run_backfill_cards_from_apisports.py
  python run_backfill_attendance_referee_from_apisports.py --league CL
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from db import init_db, backfill_cl_el_api_sports_fixture_id, get_connection


def main() -> int:
    init_db()
    n = backfill_cl_el_api_sports_fixture_id()
    print(f"Actualizados api_sports_fixture_id en {n} partidos (CL/EL).")
    if n > 0:
        with get_connection() as conn:
            c = conn.cursor()
            c.execute(
                "SELECT league_id, COUNT(*) FROM historical_matches "
                "WHERE league_id IN ('CL','EL') AND api_sports_fixture_id IS NOT NULL GROUP BY league_id"
            )
            for row in c.fetchall():
                print(f"  {row[0]}: {row[1]} partidos con api_sports_fixture_id.")
        print("\nSiguiente paso: ejecutar backfill de estadísticas y tarjetas para rellenar tiros, córners, árbitro, HT/FT:")
        print("  python run_backfill_statistics_from_apisports.py")
        print("  python run_backfill_cards_from_apisports.py")
    return 0


if __name__ == "__main__":
    sys.exit(main())
