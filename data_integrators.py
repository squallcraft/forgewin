"""
Integración de datos externos (API-Football) para xG, estadísticas y lesiones.
Resuelve partidos por (liga, fecha, equipos) ya que fixture_id es distinto a football-data.org.
Variables en .env: API_FOOTBALL_KEY (api-sports.io).
"""

import os
import re
import time
from typing import Any, Dict, List, Optional

import requests

try:
    from dotenv import load_dotenv
    from pathlib import Path
    load_dotenv(Path(__file__).resolve().parent / ".env")
except ImportError:
    pass

API_FOOTBALL_KEY = os.getenv("API_FOOTBALL_KEY") or os.getenv("API_FOOTBALL_API_KEY") or ""
API_FOOTBALL_BASE = "https://v3.football.api-sports.io"

# football-data.org league_code -> API-Football league id
LEAGUE_CODE_TO_APIFOOTBALL: Dict[str, int] = {
    "PL": 39,
    "PD": 140,
    "SA": 135,
    "BL1": 78,
    "FL1": 61,
    "CL": 2,
    "EL": 3,    # UEFA Europa League (API-Sports)
    "CLI": 13,  # CONMEBOL Libertadores (API-Sports)
    "DED": 88,
    "PPL": 94,
    "ELC": 40,
    "EL1": 41,
}

_CACHE: Dict[str, tuple] = {}
_CACHE_TTL = 300.0


def _normalize(s: str) -> str:
    if not s:
        return ""
    return re.sub(r"[^\w\s]", "", s.strip().lower())


def _api_get(endpoint: str, params: Optional[Dict[str, Any]] = None) -> Optional[Dict]:
    if not API_FOOTBALL_KEY:
        return None
    url = f"{API_FOOTBALL_BASE.rstrip('/')}/{endpoint.lstrip('/')}"
    headers = {"x-apisports-key": API_FOOTBALL_KEY}
    try:
        time.sleep(0.5)
        r = requests.get(url, headers=headers, params=params, timeout=12)
        r.raise_for_status()
        return r.json()
    except requests.RequestException:
        return None


def _resolve_fixture_id(match: Dict[str, Any]) -> Optional[int]:
    """
    Dado un partido con home_team, away_team, date (o datetime) y league_code,
    devuelve el fixture_id de API-Football si lo encuentra.
    """
    league_code = match.get("league_code") or match.get("league_id") or ""
    league_id = LEAGUE_CODE_TO_APIFOOTBALL.get(league_code)
    if league_id is None:
        return None
    date_val = match.get("date") or match.get("datetime")
    if hasattr(date_val, "strftime"):
        date_str = date_val.strftime("%Y-%m-%d")
    elif date_val:
        date_str = str(date_val)[:10]
    else:
        return None
    cache_key = f"fixtures_{league_id}_{date_str}"
    if cache_key in _CACHE:
        ts, data = _CACHE[cache_key]
        if time.time() - ts < _CACHE_TTL:
            events = data
        else:
            del _CACHE[cache_key]
            events = None
    else:
        events = None
    if events is None:
        resp = _api_get("fixtures", {"league": league_id, "date": date_str})
        if not resp or "response" not in resp:
            return None
        events = resp.get("response") or []
        _CACHE[cache_key] = (time.time(), events)
    home = _normalize(str(match.get("home_team") or match.get("home") or ""))
    away = _normalize(str(match.get("away_team") or match.get("away") or ""))
    if not home or not away:
        return None
    for ev in events:
        teams = ev.get("teams") or {}
        h = _normalize(teams.get("home", {}).get("name") or "")
        a = _normalize(teams.get("away", {}).get("name") or "")
        if (home in h or h in home) and (away in a or a in away):
            return ev.get("fixture", {}).get("id")
        if (home in a or a in home) and (away in h or h in away):
            return ev.get("fixture", {}).get("id")
    return None


def get_fixture_stats(match: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Estadísticas del partido (xG, posesión, tiros, etc.) desde API-Football.
    match debe tener home_team, away_team, date, league_code.
    Devuelve dict con xG_home, xG_away y otros si están disponibles.
    """
    fid = _resolve_fixture_id(match)
    if not fid:
        return None
    cache_key = f"stats_{fid}"
    if cache_key in _CACHE:
        ts, data = _CACHE[cache_key]
        if time.time() - ts < _CACHE_TTL:
            return data
        del _CACHE[cache_key]
    resp = _api_get("fixtures/statistics", {"fixture": fid})
    if not resp or "response" not in resp:
        return None
    response = resp.get("response") or []
    out: Dict[str, Any] = {}
    for i, team_block in enumerate(response[:2]):
        stats_list = team_block.get("statistics") or []
        xg_val = None
        for s in stats_list:
            if (s.get("type") or "").lower() == "expected goals":
                v = s.get("value") or "0"
                try:
                    xg_val = float(str(v).replace(",", "."))
                except ValueError:
                    xg_val = 0.0
                break
        if xg_val is not None:
            if i == 0:
                out["xG_home"] = xg_val
            else:
                out["xG_away"] = xg_val
    if len(response) >= 2 and "xG_home" not in out and "xG_away" not in out:
        # Algunas ligas no tienen xG; intentar extraer de otro campo
        pass
    _CACHE[cache_key] = (time.time(), out)
    return out if out else None


def get_injuries(match: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Lesiones/ausentes para el partido desde API-Football.
    match debe tener home_team, away_team, date, league_code.
    Devuelve lista de dicts con player, team, reason, etc. para uso en injury_impact.
    """
    fid = _resolve_fixture_id(match)
    if not fid:
        return []
    cache_key = f"injuries_{fid}"
    if cache_key in _CACHE:
        ts, data = _CACHE[cache_key]
        if time.time() - ts < _CACHE_TTL:
            return data
        del _CACHE[cache_key]
    resp = _api_get("injuries", {"fixture": fid})
    if not resp or "response" not in resp:
        return []
    raw = resp.get("response") or []
    result = []
    for r in raw:
        player = r.get("player") or {}
        team = r.get("team") or {}
        result.append({
            "player": player.get("name"),
            "team_id": team.get("id"),
            "team_name": team.get("name"),
            "reason": (r.get("player") or {}).get("reason") or r.get("reason"),
        })
    _CACHE[cache_key] = (time.time(), result)
    return result


def get_external_data(match: Dict[str, Any]) -> Dict[str, Any]:
    """
    Fusiona stats (xG) y lesiones para un partido.
    Retorna dict con xG_home, xG_away (si hay), injuries (lista), y opcionalmente
    injury_impact_home, injury_impact_away estimados para pasar al analyzer.
    """
    out: Dict[str, Any] = {}
    stats = get_fixture_stats(match)
    if stats:
        out["xG_home"] = stats.get("xG_home")
        out["xG_away"] = stats.get("xG_away")
    injuries = get_injuries(match)
    if injuries:
        out["injuries"] = injuries
        # Estimar impacto por equipo (simplificado: 0.1 por cada lesión, máx 0.25)
        home_id = match.get("home_team_id")
        away_id = match.get("away_team_id")
        team_ids = {}
        for inc in injuries:
            tid = inc.get("team_id")
            if tid:
                team_ids[tid] = team_ids.get(tid, 0) + 1
        out["injury_impact_home"] = min(0.25, 0.10 * team_ids.get(home_id, 0))
        out["injury_impact_away"] = min(0.25, 0.10 * team_ids.get(away_id, 0))
    return out
