#!/usr/bin/env python3
"""
Detecta discrepancias entre master_table y API-Sports en TODOS los datos:
- Campeón por liga/temporada (standings_champion)
- Partido a partido: home_goals, away_goals, home_team_name, away_team_name, ftr, hthg, htag, htr

Inserta en data_discrepancies para que el admin las resuelva.

API-Sports usa season = año inicio (2023 = 2023/24).
master_table usa season = año fin (2024 = 2023/24).

Uso:
  python run_detect_discrepancies.py                    # standings + fixtures (por defecto)
  python run_detect_discrepancies.py --standings-only   # solo campeones
  python run_detect_discrepancies.py --fixtures-only    # solo partidos
  python run_detect_discrepancies.py --league PL --season 2024 --limit 100
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

LEAGUE_IDS = ["PL", "SA", "PD", "BL1", "FL1"]

# Columnas de master_table comparables con API-Sports (datos de partido)
FIXTURE_COMPARE_COLUMNS = [
    "home_goals",
    "away_goals",
    "home_team_name",
    "away_team_name",
    "ftr",
    "hthg",
    "htag",
    "htr",
]


def _api_season_for_master_season(master_season: int) -> int:
    """master_table season = año fin (2024 = 2023/24) → API-Sports season = año inicio (2023)"""
    return master_season - 1


def _ftr_from_goals(hg: int, ag: int) -> str:
    """H=local, D=empate, A=visitante."""
    if hg > ag:
        return "H"
    if hg < ag:
        return "A"
    return "D"


def _normalize_val(v: Any) -> str:
    """Valor comparable para discrepancias."""
    if v is None:
        return ""
    s = str(v).strip()
    if isinstance(v, (int, float)):
        return str(int(v))
    return s


def _detect_standings_discrepancies(
    init_db_fn,
    get_standings_from_master_table_fn,
    insert_discrepancy_fn,
    get_existing_discrepancy_fn,
    get_standings_api_sports_fn,
    leagues: List[str],
    seasons: List[int],
) -> int:
    """Detecta discrepancias de campeón por liga/temporada. Devuelve número insertadas."""
    init_db_fn()
    n = 0
    for league_id in leagues:
        if league_id not in ("PL", "SA", "PD", "BL1", "FL1"):
            continue
        for master_season in seasons:
            try:
                st_master = get_standings_from_master_table_fn(league_id, master_season)
            except Exception as e:
                logger.warning("Standings master %s %s: %s", league_id, master_season, e)
                continue
            if not st_master:
                continue
            champ_master = st_master[0]["team_name"]
            pts_master = st_master[0]["points"]

            try:
                api_season = _api_season_for_master_season(master_season)
                st_api = get_standings_api_sports_fn(league_id, api_season)
            except Exception as e:
                logger.warning("Standings API %s %s: %s", league_id, master_season, e)
                continue
            if not st_api:
                continue
            champ_api = next((r["team_name"] for r in st_api if r.get("rank") == 1), None)
            pts_api = next((r.get("points") for r in st_api if r.get("rank") == 1), None)

            if champ_api and champ_master and champ_master.lower() != champ_api.lower():
                entity_id = f"{league_id}:{master_season}"
                if get_existing_discrepancy_fn("standings_champion", entity_id, "champion"):
                    continue
                try:
                    insert_discrepancy_fn(
                        entity_type="standings_champion",
                        entity_id=entity_id,
                        field="champion",
                        value_source_a=f"{champ_master} ({pts_master} pts)",
                        value_source_b=f"{champ_api} ({pts_api} pts)" if pts_api else champ_api,
                        source_a="master_table",
                        source_b="api_sports",
                        league_id=league_id,
                        season=master_season,
                    )
                    n += 1
                    logger.info("Discrepancia campeón: %s %s - master=%s vs API=%s", league_id, master_season, champ_master, champ_api)
                except Exception as e:
                    logger.warning("Insert standings discrepancy: %s", e)
    return n


def _build_api_lookup(
    api_fixtures: List[Dict[str, Any]],
    league_id: str,
    normalize_fn,
) -> Dict[Tuple[str, str, str], Dict[str, Any]]:
    """Lookup (date_str, home_norm, away_norm) -> api_match."""
    lookup: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
    for m in api_fixtures:
        date_str = (m.get("date") or "")[:10]
        if len(date_str) < 10:
            continue
        home = (m.get("home_team_name") or "").strip()
        away = (m.get("away_team_name") or "").strip()
        home_n = normalize_fn(home, league_id) or home.lower()
        away_n = normalize_fn(away, league_id) or away.lower()
        key = (date_str, home_n, away_n)
        if key not in lookup:
            lookup[key] = m
    return lookup


def _detect_fixture_discrepancies(
    init_db_fn,
    get_master_table_fixtures_fn,
    get_fixtures_api_sports_fn,
    insert_discrepancy_fn,
    get_existing_discrepancy_fn,
    normalize_team_fn,
    leagues: List[str],
    seasons: List[int],
    limit: int,
) -> int:
    """Detecta discrepancias partido a partido. Devuelve número insertadas."""
    init_db_fn()
    n = 0
    for league_id in leagues:
        if league_id not in ("PL", "SA", "PD", "BL1", "FL1"):
            continue
        for master_season in seasons:
            try:
                master_rows = get_master_table_fixtures_fn(
                    league_id=league_id,
                    season=master_season,
                    limit=limit,
                )
            except Exception as e:
                logger.warning("Master fixtures %s %s: %s", league_id, master_season, e)
                continue
            if not master_rows:
                continue
            try:
                api_fixtures = get_fixtures_api_sports_fn(league_id, master_season)
            except Exception as e:
                logger.warning("API fixtures %s %s: %s", league_id, master_season, e)
                continue
            if not api_fixtures:
                continue

            def norm(name: str, lid: str) -> str:
                return (normalize_team_fn(name, lid) or name or "").strip().lower()

            api_lookup = _build_api_lookup(api_fixtures, league_id, norm)

            for row in master_rows:
                date_str = (row.get("date") or "")[:10]
                if len(date_str) < 10:
                    continue
                home_m = (row.get("home_team_name") or "").strip()
                away_m = (row.get("away_team_name") or "").strip()
                key = (date_str, norm(home_m, league_id), norm(away_m, league_id))
                api_match = api_lookup.get(key)
                if not api_match:
                    continue

                # Comparar columnas
                master_vals = {
                    "home_goals": row.get("home_goals"),
                    "away_goals": row.get("away_goals"),
                    "home_team_name": row.get("home_team_name"),
                    "away_team_name": row.get("away_team_name"),
                    "ftr": row.get("ftr") or _ftr_from_goals(int(row.get("home_goals") or 0), int(row.get("away_goals") or 0)),
                    "hthg": row.get("hthg"),
                    "htag": row.get("htag"),
                    "htr": row.get("htr"),
                }
                api_vals = {
                    "home_goals": api_match.get("home_goals"),
                    "away_goals": api_match.get("away_goals"),
                    "home_team_name": api_match.get("home_team_name"),
                    "away_team_name": api_match.get("away_team_name"),
                    "ftr": _ftr_from_goals(int(api_match.get("home_goals") or 0), int(api_match.get("away_goals") or 0)),
                    "hthg": None,
                    "htag": None,
                    "htr": None,
                }

                for field in FIXTURE_COMPARE_COLUMNS:
                    mv = _normalize_val(master_vals.get(field))
                    av = _normalize_val(api_vals.get(field))
                    if mv == av:
                        continue
                    fid = row.get("fixture_id")
                    entity_id = str(fid)
                    if get_existing_discrepancy_fn("fixture", entity_id, field):
                        continue
                    try:
                        insert_discrepancy_fn(
                            entity_type="fixture",
                            entity_id=entity_id,
                            field=field,
                            value_source_a=mv or "(vacío)",
                            value_source_b=av or "(vacío)",
                            source_a="master_table",
                            source_b="api_sports",
                            league_id=league_id,
                            season=master_season,
                        )
                        n += 1
                        logger.info("Discrepancia fixture %s %s %s: master=%s vs API=%s", fid, field, league_id, mv, av)
                    except Exception as e:
                        logger.warning("Insert fixture discrepancy: %s", e)
    return n


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Detecta discrepancias master_table vs API-Sports (campeones + partidos)"
    )
    parser.add_argument("--league", type=str, default=None, help="Liga (PL, SA, etc.)")
    parser.add_argument("--season", type=int, default=None, help="Temporada año fin (master_table)")
    parser.add_argument("--standings-only", action="store_true", help="Solo discrepancias de campeón")
    parser.add_argument("--fixtures-only", action="store_true", help="Solo discrepancias partido a partido")
    parser.add_argument("--limit", type=int, default=500, help="Límite de partidos por liga/temporada (fixtures)")
    args = parser.parse_args()

    from db import (
        init_db,
        get_standings_from_master_table,
        get_master_table_fixtures_for_comparison,
        insert_discrepancy,
        get_existing_discrepancy,
        normalize_team_name,
    )
    from api_sports_fetcher import get_standings_api_sports, get_fixtures_api_sports_for_season

    leagues = [args.league] if args.league else LEAGUE_IDS
    seasons = [args.season] if args.season else [2024, 2023, 2022, 2021, 2020]
    total = 0

    if not args.fixtures_only:
        n_standings = _detect_standings_discrepancies(
            init_db,
            get_standings_from_master_table,
            insert_discrepancy,
            get_existing_discrepancy,
            get_standings_api_sports,
            leagues,
            seasons,
        )
        total += n_standings
        logger.info("Discrepancias standings (campeón): %d", n_standings)

    if not args.standings_only:
        n_fixtures = _detect_fixture_discrepancies(
            init_db,
            get_master_table_fixtures_for_comparison,
            get_fixtures_api_sports_for_season,
            insert_discrepancy,
            get_existing_discrepancy,
            normalize_team_name,
            leagues,
            seasons,
            args.limit,
        )
        total += n_fixtures
        logger.info("Discrepancias fixtures (partido a partido): %d", n_fixtures)

    logger.info("Total discrepancias insertadas: %d", total)
    return 0


if __name__ == "__main__":
    sys.exit(main())
