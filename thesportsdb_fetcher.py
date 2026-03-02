"""
Fetch fixtures from TheSportsDB como fallback cuando football-data.org y API-Sports no devuelven partidos.
Gratis con key "3" (compartida). Sin API key adicional necesaria.
"""

import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger(__name__)

# Key gratuita compartida (sin registro)
API_KEY = "3"
BASE_URL = "https://www.thesportsdb.com/api/v1/json"

# Patrones de nombre de liga para filtrar (case-insensitive)
LEAGUE_PATTERNS: Dict[str, List[str]] = {
    "EL": ["uefa europa league", "europa league"],
    "CLI": ["copa libertadores", "libertadores"],
    "CL": ["uefa champions league", "champions league"],
}


def _api_get(endpoint: str, params: Optional[Dict[str, Any]] = None) -> Optional[Dict]:
    url = f"{BASE_URL}/{API_KEY}/{endpoint}"
    try:
        time.sleep(1.5)  # Evitar 429 rate limit (key compartida)
        r = requests.get(url, params=params or {}, timeout=12)
        r.raise_for_status()
        return r.json()
    except requests.RequestException as e:
        logger.warning("thesportsdb_fetcher: error GET %s: %s", endpoint, e)
        return None


def _matches_league(ev: Dict[str, Any], league_code: str) -> bool:
    """True si el evento pertenece a la liga indicada."""
    patterns = LEAGUE_PATTERNS.get(league_code, [])
    name = (ev.get("strLeague") or "").lower()
    return any(p in name for p in patterns)


def _to_forgewin_format(ev: Dict[str, Any], league_code: str) -> Dict[str, Any]:
    """Convierte evento TheSportsDB a formato ForgeWin."""
    date_str = ev.get("dateEvent") or ""
    time_str = ev.get("strTime") or "00:00:00"
    ts = ev.get("strTimestamp") or ""
    if ts:
        try:
            if "T" in ts and "+" not in ts and "Z" not in ts:
                ts = ts + "Z"
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            dt = datetime.now(timezone.utc)
    else:
        try:
            dt = datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M:%S")
            dt = dt.replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
            dt = datetime.now(timezone.utc)

    iso_date = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    fid = int(ev.get("idEvent") or 0)
    fid_forgewin = 800_000_000 + (fid % 99_999_999)

    return {
        "fixture_id": fid_forgewin,
        "home_team": ev.get("strHomeTeam"),
        "away_team": ev.get("strAwayTeam"),
        "home_team_id": ev.get("idHomeTeam"),
        "away_team_id": ev.get("idAwayTeam"),
        "date": iso_date,
        "match_date_utc": iso_date,
        "datetime": dt,
        "league_id": league_code,
        "league_code": league_code,
        "league_name": ev.get("strLeague") or league_code,
        "status": "SCHEDULED",
        "_source": "thesportsdb",
    }


def get_fixtures_for_date(
    date_iso: str,
    league_codes: List[str],
) -> List[Dict[str, Any]]:
    """
    Obtiene eventos Soccer de TheSportsDB para una fecha.
    date_iso: YYYY-MM-DD
    Filtra por league_codes (EL, CLI, CL).
    """
    if not league_codes:
        return []

    resp = _api_get("eventsday.php", {"d": date_iso, "l": "Soccer"})
    if not resp or not resp.get("events"):
        return []

    events = resp.get("events") or []
    out: List[Dict[str, Any]] = []
    for ev in events:
        for code in league_codes:
            if _matches_league(ev, code):
                out.append(_to_forgewin_format(ev, code))
                break
    return out


def fetch_fallback_matches(
    league_codes: List[str],
    date_from: str,
    date_to: str,
) -> List[Dict[str, Any]]:
    """
    Obtiene partidos de TheSportsDB para el rango de fechas.
    Solo ligas con patrón definido: EL, CLI, CL.
    """
    supported = [c for c in league_codes if c in LEAGUE_PATTERNS]
    if not supported:
        return []

    all_matches: List[Dict[str, Any]] = []
    from_dt = datetime.strptime(date_from, "%Y-%m-%d")
    to_dt = datetime.strptime(date_to, "%Y-%m-%d")
    # Limitar a 7 días para evitar 429 (rate limit TheSportsDB free)
    max_days = 7
    if (to_dt - from_dt).days > max_days:
        to_dt = from_dt + timedelta(days=max_days)
    current = from_dt
    while current <= to_dt:
        date_str = current.strftime("%Y-%m-%d")
        matches = get_fixtures_for_date(date_str, supported)
        if matches:
            logger.info("thesportsdb_fetcher: %s -> %d partidos", date_str, len(matches))
            all_matches.extend(matches)
        current = current + timedelta(days=1)

    all_matches.sort(key=lambda x: x.get("datetime") or datetime.min)
    return all_matches
