"""
Orquestador de fallbacks: cuando football-data.org no devuelve partidos,
intenta API-Sports y luego TheSportsDB.
"""

import logging
from typing import Any, Dict, List

from api_sports_fetcher import fetch_fallback_matches as _api_sports_fetch
from thesportsdb_fetcher import fetch_fallback_matches as _thesportsdb_fetch

logger = logging.getLogger(__name__)

# Ligas que tienen fallback (football-data.org free tier no las incluye)
FALLBACK_LEAGUE_CODES = frozenset({"EL", "CLI"})


def fetch_fallback(
    league_codes: List[str],
    date_from: str,
    date_to: str,
) -> List[Dict[str, Any]]:
    """
    Intenta obtener partidos de fuentes alternativas.
    1. API-Sports (requiere API_FOOTBALL_KEY)
    2. TheSportsDB (gratis, key compartida)
    Retorna lista en formato ForgeWin.
    """
    to_try = [c for c in league_codes if c in FALLBACK_LEAGUE_CODES]
    if not to_try:
        return []

    # 1. API-Sports
    try:
        matches = _api_sports_fetch(to_try, date_from, date_to, only_fallback_leagues=True)
        if matches:
            logger.info("fallback_fixtures: API-Sports devolvió %d partidos", len(matches))
            return matches
    except Exception as e:
        logger.warning("fallback_fixtures: API-Sports falló: %s", e)

    # 2. TheSportsDB
    try:
        matches = _thesportsdb_fetch(to_try, date_from, date_to)
        if matches:
            logger.info("fallback_fixtures: TheSportsDB devolvió %d partidos", len(matches))
            return matches
    except Exception as e:
        logger.warning("fallback_fixtures: TheSportsDB falló: %s", e)

    return []
