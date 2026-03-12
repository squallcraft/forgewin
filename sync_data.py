"""
Sincronización: API-Sports como fuente principal, football-data.org como secundaria.
Guardar partidos en BD y actualizar resultados al día siguiente.
"""

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

import requests

from config import BASE_URL, API_KEY, REQUEST_DELAY_SECONDS
from db import upsert_match, update_match_result, get_connection
import time


def _normalize_date_utc(s: str) -> str:
    """Deja la fecha en formato YYYY-MM-DDTHH:MM:SS para que la comparación en BD sea correcta."""
    if not s:
        return s
    s = str(s).replace("Z", "").replace("+00:00", "").strip()
    return s[:19] if len(s) > 19 else s


def _normalize_match_for_upsert(m: Dict[str, Any], code: str) -> Dict[str, Any]:
    """Formato para upsert_match. Fechas sin 'Z' para que la comparación en BD funcione."""
    raw_date = _normalize_date_utc(m.get("match_date_utc") or m.get("date") or "")
    return {
        "fixture_id": m.get("fixture_id"),
        "home_team": m.get("home_team"),
        "away_team": m.get("away_team"),
        "home_team_id": m.get("home_team_id"),
        "away_team_id": m.get("away_team_id"),
        "date": raw_date or m.get("date") or m.get("match_date_utc"),
        "match_date_utc": raw_date or m.get("match_date_utc") or m.get("date"),
        "league_id": m.get("league_code") or code,
        "league_code": m.get("league_code") or code,
        "league_name": m.get("league_name") or code,
        "status": m.get("status", "SCHEDULED"),
        "home_goals": m.get("home_goals"),
        "away_goals": m.get("away_goals"),
    }


def _api_get_football_data(path: str, params: Dict[str, Any] = None) -> dict:
    """GET football-data.org (fuente secundaria)."""
    if not API_KEY:
        return {}
    url = f"{BASE_URL.rstrip('/')}/{path.lstrip('/')}"
    headers = {"X-Auth-Token": API_KEY}
    try:
        time.sleep(REQUEST_DELAY_SECONDS)
        r = requests.get(url, headers=headers, params=params or {}, timeout=15)
        r.raise_for_status()
        return r.json()
    except requests.RequestException:
        return {}


def fetch_and_store_matches(league_codes: List[str], days_ahead: int = 3) -> List[Dict]:
    """
    Obtiene próximos partidos: primero API-Sports, si no hay datos usa football-data.org.
    Para EL, CL, CLI usa ventana extendida (60 días).
    """
    today = datetime.now(timezone.utc).date()
    date_from_str = today.isoformat()
    all_matches: List[Dict] = []
    CUP_MIN_DAYS = 60

    for code in league_codes:
        effective_days = max(days_ahead, CUP_MIN_DAYS) if code in ("EL", "CLI", "CL") else days_ahead
        date_to = today + timedelta(days=effective_days)
        date_to_str = date_to.isoformat()

        # 1) Fuente principal: API-Sports
        try:
            from api_sports_fetcher import fetch_primary_matches
            primary = fetch_primary_matches([code], date_from_str, date_to_str)
            if primary:
                for m in primary:
                    mm = _normalize_match_for_upsert(m, code)
                    upsert_match(mm)
                    all_matches.append(mm)
                continue
        except Exception:
            pass

        # 2) Fuente secundaria: football-data.org
        data = _api_get_football_data(
            f"competitions/{code}/matches",
            {"dateFrom": date_from_str, "dateTo": date_to_str, "status": "SCHEDULED"},
        )
        matches_raw = data.get("matches") or []
        if not matches_raw and code in ("EL", "CLI"):
            try:
                from fallback_fixtures import fetch_fallback
                fallback_matches = fetch_fallback([code], date_from_str, date_to_str)
                for m in fallback_matches:
                    mm = _normalize_match_for_upsert(m, code)
                    upsert_match(mm)
                    all_matches.append(mm)
            except Exception:
                pass
        for m in matches_raw:
            home = m.get("homeTeam") or {}
            away = m.get("awayTeam") or {}
            comp = m.get("competition") or {}
            date_str = _normalize_date_utc(m.get("utcDate") or "")
            match_dict = {
                "fixture_id": m.get("id"),
                "home_team": home.get("name"),
                "away_team": away.get("name"),
                "home_team_id": home.get("id"),
                "away_team_id": away.get("id"),
                "date": date_str,
                "match_date_utc": date_str,
                "league_id": comp.get("code") or code,
                "league_code": comp.get("code") or code,
                "league_name": comp.get("name") or code,
                "status": m.get("status", "SCHEDULED"),
                "home_goals": None,
                "away_goals": None,
            }
            upsert_match(match_dict)
            all_matches.append(match_dict)
    return all_matches


def fetch_finished_results(league_codes: List[str], days_back: int = 1) -> int:
    """
    Actualiza resultados: primero API-Sports, luego football-data.org.
    Devuelve número de partidos actualizados.
    """
    today = datetime.now(timezone.utc).date()
    date_from = today - timedelta(days=days_back)
    date_to = today
    date_from_str = date_from.isoformat()
    date_to_str = date_to.isoformat()
    updated = 0

    for code in league_codes:
        # 1) Fuente principal: API-Sports (fixture_id remapeado 9xx)
        try:
            from api_sports_fetcher import get_finished_fixtures_api_sports
            finished = get_finished_fixtures_api_sports(code, date_from_str, date_to_str)
            for row in finished:
                fid = row.get("fixture_id")
                h, a = row.get("home_goals"), row.get("away_goals")
                if fid is not None and h is not None and a is not None:
                    update_match_result(fid, int(h), int(a))
                    updated += 1
        except Exception:
            pass

        # 2) Fuente secundaria: football-data.org
        data = _api_get_football_data(
            f"competitions/{code}/matches",
            {"dateFrom": date_from_str, "dateTo": date_to_str, "status": "FINISHED"},
        )
        for m in data.get("matches") or []:
            score = m.get("score") or {}
            ft = score.get("fullTime") or {}
            h = ft.get("home")
            a = ft.get("away")
            if h is not None and a is not None:
                fid = m.get("id")
                if fid:
                    update_match_result(fid, int(h), int(a))
                    updated += 1
    return updated


def maintain_historical_rolling_window(dry_run: bool = False) -> int:
    """
    Mantiene la ventana rolling (4 temporadas completas + actual; keep_seasons=5).
    dry_run=True: no borra, solo reporta. Devuelve número eliminados (0 si dry_run).
    """
    from rolling_window import maintain_rolling_window
    deleted, _ = maintain_rolling_window(keep_seasons=5, dry_run=dry_run)
    return deleted


if __name__ == "__main__":
    """Sincroniza partidos y actualiza resultados de partidos finalizados."""
    from config import TOP_10_LEAGUE_CODES
    print("Sincronizando partidos (top 10 ligas, próximos 3 días)...")
    matches = fetch_and_store_matches(TOP_10_LEAGUE_CODES, days_ahead=3)
    print(f"✓ {len(matches)} partidos guardados en BD.")

    print("Actualizando resultados (últimos 3 días)...")
    updated = fetch_finished_results(TOP_10_LEAGUE_CODES, days_back=3)
    print(f"✓ {updated} resultados actualizados.")
