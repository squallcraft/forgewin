#!/usr/bin/env python3
"""
Comprueba si API-Sports devuelve fixtures y estadísticas para Serie A en 2026.
Uso: python scripts/check_apisports_serie_a_2026.py
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def main():
    from api_sports_fetcher import (
        API_KEY,
        get_finished_fixtures_for_historical,
        get_fixture_statistics,
        _season_from_date,
    )

    if not API_KEY:
        print("API_FOOTBALL_KEY no configurada. Salida.")
        return 1

    date_from = "2026-01-01"
    date_to = "2026-02-22"
    league = "SA"

    season_computed = _season_from_date(date_from, league)
    print(f"Serie A: rango {date_from} a {date_to}")
    print(f"  _season_from_date({date_from}, {league}) = {season_computed} (debe ser 2026 para API)")

    rows = get_finished_fixtures_for_historical(league, date_from, date_to)
    print(f"  Fixtures devueltos por API: {len(rows)}")

    if not rows:
        print("  -> No hay partidos; comprobar season en la petición (debe ser 2026).")
        return 0

    # Primer partido: referee, attendance, api_sports_fixture_id
    r0 = rows[0]
    fid = r0.get("api_sports_fixture_id")
    print(f"  Primer partido: {r0.get('home_team_name')} - {r0.get('away_team_name')} ({r0.get('date')})")
    print(f"    api_sports_fixture_id={fid}, referee={r0.get('referee')!r}, attendance={r0.get('attendance')}")

    if fid and r0.get("home_team_id") and r0.get("away_team_id"):
        home_stats = get_fixture_statistics(fid, r0["home_team_id"])
        away_stats = get_fixture_statistics(fid, r0["away_team_id"])
        print("  Estadísticas (local):", home_stats)
        print("  Estadísticas (visitante):", away_stats)
    else:
        print("  (Sin fixture_id o team_ids, no se llama /statistics)")

    return 0


if __name__ == "__main__":
    sys.exit(main())
