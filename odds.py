"""
Cuotas de mercado para mezclar con probabilidades Poisson.
The Odds API (v4) para odds 1X2. football-data.org no incluye odds.
Variables en .env: ODDS_API_KEY o THE_ODDS_API_KEY.
"""

import os
import re
import time
from typing import Any, Dict, List, Optional, Tuple

import requests

ODDS_API_KEY = os.getenv("ODDS_API_KEY") or os.getenv("THE_ODDS_API_KEY") or ""
ODDS_API_BASE = "https://api.the-odds-api.com/v4"

# Mapeo league_code (football-data.org) -> sport_key (The Odds API)
LEAGUE_TO_ODDS_SPORT: Dict[str, str] = {
    "PL": "soccer_epl",
    "PD": "soccer_spain_la_liga",
    "SA": "soccer_italy_serie_a",
    "BL1": "soccer_germany_bundesliga",
    "FL1": "soccer_france_ligue_one",
    "DED": "soccer_netherlands_eredivisie",
    "PPL": "soccer_portugal_primeira_liga",
    "CL": "soccer_uefa_champions_league",
    "ELC": "soccer_england_championship",
    "EL1": "soccer_england_league_one",
}

# Cache simple: sport_key -> (timestamp, list of events with odds). TTL 5 min
_odds_cache: Dict[str, Tuple[float, List[Dict]]] = {}
_ODDS_CACHE_TTL_SEC = 300.0


def _normalize_team_name(name: str) -> str:
    """Normaliza nombre de equipo para comparación (minúsculas, sin extras)."""
    if not name:
        return ""
    s = name.strip().lower()
    s = re.sub(r"[^\w\s]", "", s)
    return " ".join(s.split())


def _fetch_sport_odds(sport_key: str) -> List[Dict]:
    """Obtiene eventos con odds para un sport (1 request por sport). Respeta cache."""
    now = time.time()
    if sport_key in _odds_cache:
        ts, events = _odds_cache[sport_key]
        if now - ts < _ODDS_CACHE_TTL_SEC:
            return events
    if not ODDS_API_KEY:
        return []
    url = f"{ODDS_API_BASE}/sports/{sport_key}/odds"
    params = {
        "apiKey": ODDS_API_KEY,
        "regions": "eu",
        "markets": "h2h",
        "oddsFormat": "decimal",
    }
    try:
        r = requests.get(url, params=params, timeout=12)
        r.raise_for_status()
        data = r.json()
        _odds_cache[sport_key] = (now, data)
        return data
    except requests.RequestException:
        return []


def _match_event(home_team: str, away_team: str, match_date: Any, events: List[Dict]) -> Optional[Dict]:
    """Encuentra un evento por equipos (y opcionalmente fecha)."""
    norm_home = _normalize_team_name(home_team or "")
    norm_away = _normalize_team_name(away_team or "")
    if not norm_home or not norm_away:
        return None
    for ev in events:
        eh = _normalize_team_name(ev.get("home_team") or "")
        ea = _normalize_team_name(ev.get("away_team") or "")
        if not eh or not ea:
            continue
        # Match: ambos nombres contienen las palabras clave o coinciden
        if (norm_home in eh or eh in norm_home) and (norm_away in ea or ea in norm_away):
            return ev
        if (norm_home in ea or ea in norm_home) and (norm_away in eh or eh in norm_away):
            return ev
    return None


def _extract_h2h_decimal(event: Dict) -> Optional[Dict[str, float]]:
    """Extrae cuotas 1X2 decimales del primer bookmaker con mercado h2h (soccer)."""
    for bm in event.get("bookmakers") or []:
        for mkt in bm.get("markets") or []:
            if mkt.get("key") != "h2h":
                continue
            outcomes = mkt.get("outcomes") or []
            if len(outcomes) != 3:
                continue
            # The Odds API soccer h2h: 3 outcomes, típicamente [Home, Draw, Away]
            try:
                return {
                    "home_win": float(outcomes[0].get("price") or 0),
                    "draw": float(outcomes[1].get("price") or 0),
                    "away_win": float(outcomes[2].get("price") or 0),
                }
            except (TypeError, IndexError, ValueError):
                pass
    return None


def odds_to_implied_probs(home_odds: float, draw_odds: float, away_odds: float) -> Tuple[float, float, float]:
    """Convierte cuotas decimales a probabilidades implícitas (normalizadas)."""
    if home_odds <= 0 or draw_odds <= 0 or away_odds <= 0:
        return 0.0, 0.0, 0.0
    inv = 1.0 / home_odds + 1.0 / draw_odds + 1.0 / away_odds
    p1 = (1.0 / home_odds) / inv
    px = (1.0 / draw_odds) / inv
    p2 = (1.0 / away_odds) / inv
    return round(p1, 4), round(px, 4), round(p2, 4)


def get_match_odds(
    fixture_id: int,
    use_mock: bool = False,
    home_team: Optional[str] = None,
    away_team: Optional[str] = None,
    match_date: Any = None,
    league_code: Optional[str] = None,
) -> Optional[Dict[str, float]]:
    """
    Devuelve cuotas decimales 1X2 si hay API configurada.
    Formato: {"home_win", "draw", "away_win"}.
    Para The Odds API se necesitan home_team, away_team y league_code para localizar el partido.
    """
    if use_mock:
        return {"home_win": 2.10, "draw": 3.40, "away_win": 3.20}
    if not ODDS_API_KEY or not league_code:
        return None
    sport_key = LEAGUE_TO_ODDS_SPORT.get(league_code)
    if not sport_key:
        return None
    events = _fetch_sport_odds(sport_key)
    if not events:
        return None
    ev = _match_event(home_team or "", away_team or "", match_date, events)
    if not ev:
        return None
    return _extract_h2h_decimal(ev)
