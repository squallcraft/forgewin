"""
Módulo de Barrido Diario - Próximos partidos por competición.
Usa football-data.org v4: GET /competitions/{code}/matches con dateFrom/dateTo y status=SCHEDULED.
"""

import logging
import time
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

import requests

from config import BASE_URL, API_KEY, REQUEST_DELAY_SECONDS, get_league_name

logger = logging.getLogger(__name__)


def _api_request_football_data(path: str, params: Optional[Dict[str, Any]] = None, use_mock: bool = False) -> dict:
    """
    GET football-data.org (fuente secundaria). Header: X-Auth-Token.
    Si no hay API_KEY o use_mock=True, devuelve mock.
    """
    if use_mock:
        logger.info("scraper: use_mock=True, devolviendo mock para path=%s", path)
        return _mock_matches_response(path, params or {})
    if not API_KEY:
        logger.warning("scraper: API_KEY no configurada (X-Auth-Token). path=%s", path)
        return _mock_matches_response(path, params or {})

    url = f"{BASE_URL.rstrip('/')}/{path.lstrip('/')}"
    headers = {"X-Auth-Token": API_KEY}
    try:
        time.sleep(REQUEST_DELAY_SECONDS)
        r = requests.get(url, headers=headers, params=params, timeout=15)
        r.raise_for_status()
        data = r.json()
        n = len(data.get("matches") or [])
        logger.info("scraper: GET %s -> %d partidos", path, n)
        return data
    except requests.RequestException as e:
        logger.exception("scraper: error en GET %s: %s", path, e)
        return {"matches": [], "message": str(e)}


def _mock_matches_response(path: str, params: dict) -> dict:
    """Mock de partidos para pruebas sin API key."""
    # path puede ser "competitions/PL/matches" o similar
    code = "PL"
    if "/" in path:
        parts = path.rstrip("/").split("/")
        for i, p in enumerate(parts):
            if p == "competitions" and i + 1 < len(parts):
                code = parts[i + 1]
                break
    base = datetime.now()
    matches = []
    for j in range(5):
        d = base + timedelta(days=j)
        matches.append({
            "id": 900000 + j,
            "utcDate": d.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "status": "SCHEDULED",
            "homeTeam": {"id": 57, "name": f"Team Home {j}"},
            "awayTeam": {"id": 58, "name": f"Team Away {j}"},
            "competition": {"id": 2021, "name": get_league_name(code), "code": code},
            "score": {"fullTime": {"home": None, "away": None}},
        })
    return {"matches": matches}


def get_upcoming_matches(
    league_codes: list[str],
    days_ahead: int = 3,
    use_mock: bool = False,
) -> list[dict]:
    """
    Barre próximos partidos en las competiciones dadas.
    Retorna lista de dicts: fixture_id, home_team, away_team, date, league_id (code), league_name.
    Ordenada por fecha.
    """
    today = datetime.utcnow().date()
    date_to = today + timedelta(days=days_ahead)
    date_from_str = today.isoformat()
    date_to_str = date_to.isoformat()

    # Copa Libertadores, Europa League y Champions: ventana mayor
    CUP_MIN_DAYS = 60
    all_matches: list[dict] = []
    for code in league_codes:
        effective_days = max(days_ahead, CUP_MIN_DAYS) if code in ("EL", "CLI", "CL") else days_ahead
        date_to_code = today + timedelta(days=effective_days)
        date_to_str_code = date_to_code.isoformat()
        logger.info("scraper: get_upcoming_matches league_codes=%s days_ahead=%s code=%s effective_days=%s dateFrom=%s dateTo=%s",
                    league_codes, days_ahead, code, effective_days, date_from_str, date_to_str_code)
        raw: list = []

        # 1) Fuente principal: API-Sports
        if not use_mock:
            try:
                from api_sports_fetcher import fetch_primary_matches
                primary = fetch_primary_matches([code], date_from_str, date_to_str_code)
                for m in primary:
                    raw.append({
                        "id": m.get("fixture_id"),
                        "utcDate": m.get("date") or m.get("match_date_utc"),
                        "homeTeam": {"id": m.get("home_team_id"), "name": m.get("home_team")},
                        "awayTeam": {"id": m.get("away_team_id"), "name": m.get("away_team")},
                        "competition": {"code": code, "name": m.get("league_name") or code},
                    })
            except Exception as e:
                logger.debug("scraper: API-Sports para %s falló: %s", code, e)

        # 2) Fuente secundaria: football-data.org (o mock)
        if not raw:
            data = _api_request_football_data(
                f"competitions/{code}/matches",
                {"dateFrom": date_from_str, "dateTo": date_to_str_code, "status": "SCHEDULED"},
                use_mock=use_mock,
            )
            if data.get("message"):
                logger.warning("scraper: API devolvió message para %s: %s", code, data["message"])
            raw = data.get("matches") or []
            if not raw and code in ("EL", "CLI") and not use_mock:
                try:
                    from fallback_fixtures import fetch_fallback
                    fallback_matches = fetch_fallback([code], date_from_str, date_to_str_code)
                    for m in fallback_matches:
                        raw.append({
                            "id": m.get("fixture_id"),
                            "utcDate": m.get("date") or m.get("match_date_utc"),
                            "homeTeam": {"id": m.get("home_team_id"), "name": m.get("home_team")},
                            "awayTeam": {"id": m.get("away_team_id"), "name": m.get("away_team")},
                            "competition": {"code": code, "name": m.get("league_name")},
                        })
                except Exception as e:
                    logger.debug("scraper: fallback para %s falló: %s", code, e)

        logger.info("scraper: liga %s -> %d partidos en respuesta", code, len(raw))
        for m in raw:
            home = m.get("homeTeam") or {}
            away = m.get("awayTeam") or {}
            comp = m.get("competition") or {}
            date_str = m.get("utcDate") or ""
            try:
                if date_str.endswith("Z"):
                    date_str = date_str.replace("Z", "+00:00")
                dt = datetime.fromisoformat(date_str.replace("+00:00", ""))
            except (ValueError, TypeError):
                dt = datetime.now()
            all_matches.append({
                "fixture_id": m.get("id"),
                "home_team": home.get("name"),
                "away_team": away.get("name"),
                "home_team_id": home.get("id"),
                "away_team_id": away.get("id"),
                "date": m.get("utcDate", date_str),
                "datetime": dt,
                "league_id": comp.get("code") or code,
                "league_name": comp.get("name") or get_league_name(code),
            })
    all_matches.sort(key=lambda x: x.get("datetime") or datetime.min)
    logger.info("scraper: get_upcoming_matches total=%d partidos", len(all_matches))
    return all_matches


def get_todays_matches(
    league_codes: list,
    use_mock: bool = False,
):
    """Partidos del día (hoy) en las competiciones dadas. Filtra por fecha actual."""
    all_matches = get_upcoming_matches(league_codes, days_ahead=1, use_mock=use_mock)
    today = datetime.utcnow().date()
    return [m for m in all_matches if m.get("datetime") and m["datetime"].date() == today]
