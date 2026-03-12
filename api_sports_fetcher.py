"""
Fetch fixtures from API-Sports (api-sports.io) como fallback cuando football-data.org no devuelve partidos.
Requerido: API_FOOTBALL_KEY en .env
"""

import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests

try:
    from dotenv import load_dotenv
    from pathlib import Path
    load_dotenv(Path(__file__).resolve().parent / ".env")
except ImportError:
    pass

logger = logging.getLogger(__name__)

API_KEY = os.getenv("API_FOOTBALL_KEY") or os.getenv("API_FOOTBALL_API_KEY") or ""
BASE_URL = "https://v3.football.api-sports.io"

# football-data.org code -> API-Sports league_id
LEAGUE_CODE_TO_ID: Dict[str, int] = {
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

# Ligas que usamos como fallback (football-data.org no las incluye en free tier)
FALLBACK_LEAGUES = frozenset({"EL", "CLI"})

# fixture_id en este rango indica partido de API-Sports (usar get_form_last_n_api_sports / get_h2h_api_sports)
FALLBACK_FIXTURE_ID_MIN = 900_000_000

# ForgeWin usa API-Sports Pro (300 req/min). Delay 0.25s deja margen. Override con API_FOOTBALL_REQUEST_DELAY en .env si necesitas otro ritmo.
def _parse_request_delay() -> float:
    raw = (os.getenv("API_FOOTBALL_REQUEST_DELAY") or "").strip()
    if not raw:
        return 0.25  # Pro: 300 req/min
    try:
        return max(0.1, float(raw))
    except ValueError:
        return 0.25


REQUEST_DELAY_SECONDS = _parse_request_delay()


def _api_get(endpoint: str, params: Optional[Dict[str, Any]] = None) -> Optional[Dict]:
    if not API_KEY:
        return None
    url = f"{BASE_URL.rstrip('/')}/{endpoint.lstrip('/')}"
    headers = {"x-apisports-key": API_KEY}
    try:
        time.sleep(REQUEST_DELAY_SECONDS)
        r = requests.get(url, headers=headers, params=params or {}, timeout=15)
        r.raise_for_status()
        return r.json()
    except requests.RequestException as e:
        logger.warning("api_sports_fetcher: error GET %s: %s", endpoint, e)
        return None


def _season_from_date(date_str: str, league_code: str = "") -> int:
    """
    Año de temporada para API-Sports (usa el año de INICIO de temporada).
    Europeas (PL, SA, CL, EL, etc.): ago-may → API-Sports usa año de inicio.
    Sudamericanas (CLI): año civil.
    """
    year = int(date_str[:4])
    if league_code == "CLI":
        return year
    month = int(date_str[5:7]) if len(date_str) >= 7 else 7
    if month >= 8:
        return year      # ago-dic: temporada inicia este año (2025-08 → season 2025)
    return year - 1  # ene-jul: temporada inició el año pasado (2026-03 → season 2025)


def get_fixtures(
    league_code: str,
    date_from: str,
    date_to: str,
    season: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """
    Obtiene fixtures de API-Sports para una liga y rango de fechas.
    date_from, date_to: YYYY-MM-DD
    API-Sports REQUIERE el parámetro season. Lo derivamos de la fecha si no se pasa.
    """
    league_id = LEAGUE_CODE_TO_ID.get(league_code)
    if league_id is None:
        logger.info("api_sports_fetcher: sin mapeo para league_code=%s", league_code)
        return []

    season = season if season is not None else _season_from_date(date_from, league_code)
    params: Dict[str, Any] = {
        "league": league_id,
        "season": season,
        "from": date_from,
        "to": date_to,
    }
    resp = _api_get("fixtures", params)
    if resp and resp.get("errors"):
        err = resp["errors"]
        if "plan" in str(err).lower():
            logger.info("api_sports_fetcher: API devolvió error de plan/cuota: %s", err)
        return []
    if not resp or "response" not in resp:
        return []

    raw = resp.get("response") or []
    out: List[Dict[str, Any]] = []
    for ev in raw:
        fixture = ev.get("fixture") or {}
        league = ev.get("league") or {}
        teams = ev.get("teams") or {}
        home = teams.get("home") or {}
        away = teams.get("away") or {}

        date_str = fixture.get("date") or ""
        if not date_str:
            continue
        try:
            if date_str.endswith("Z"):
                date_str = date_str.replace("Z", "+00:00")
            dt = datetime.fromisoformat(date_str.replace("+00:00", ""))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
            dt = datetime.now(timezone.utc)

        # fixture_id: rango 9xx_xxx_xxx para evitar colisiones con football-data.org
        fid = fixture.get("id") or 0
        fid_forgewin = 900_000_000 + (int(fid) % 99_999_999)

        iso_date = dt.strftime("%Y-%m-%dT%H:%M:%SZ") if dt.tzinfo else (dt.isoformat() + "Z")

        out.append({
            "fixture_id": fid_forgewin,
            "home_team": home.get("name"),
            "away_team": away.get("name"),
            "home_team_id": home.get("id"),
            "away_team_id": away.get("id"),
            "date": iso_date,
            "datetime": dt,
            "league_id": league_code,
            "league_code": league_code,
            "league_name": league.get("name") or league_code,
            "status": "SCHEDULED",
            "_source": "api_sports",
        })
    return out


def fetch_fallback_matches(
    league_codes: List[str],
    date_from: str,
    date_to: str,
    only_fallback_leagues: bool = True,
) -> List[Dict[str, Any]]:
    """
    Obtiene partidos de API-Sports para las ligas indicadas.
    Si only_fallback_leagues=True, solo consulta EL y CLI (las que football-data no da en free).
    """
    if not API_KEY:
        logger.info("api_sports_fetcher: API_FOOTBALL_KEY no configurada, saliendo")
        return []

    codes = [c for c in league_codes if c in LEAGUE_CODE_TO_ID]
    if only_fallback_leagues:
        codes = [c for c in codes if c in FALLBACK_LEAGUES]
    if not codes:
        return []

    all_matches: List[Dict[str, Any]] = []
    for code in codes:
        matches = get_fixtures(code, date_from, date_to)
        logger.info("api_sports_fetcher: %s -> %d partidos", code, len(matches))
        all_matches.extend(matches)

    all_matches.sort(key=lambda x: x.get("datetime") or datetime.min)
    return all_matches


def fetch_primary_matches(
    league_codes: List[str],
    date_from: str,
    date_to: str,
) -> List[Dict[str, Any]]:
    """
    Obtiene partidos de API-Sports para todas las ligas indicadas (fuente principal).
    Devuelve lista en formato ForgeWin (fixture_id 9xx, home_team, away_team, etc.).
    """
    if not API_KEY:
        return []
    codes = [c for c in league_codes if c in LEAGUE_CODE_TO_ID]
    if not codes:
        return []
    all_matches: List[Dict[str, Any]] = []
    for code in codes:
        matches = get_fixtures(code, date_from, date_to)
        logger.info("api_sports_fetcher: primary %s -> %d partidos", code, len(matches))
        all_matches.extend(matches)
    all_matches.sort(key=lambda x: x.get("datetime") or datetime.min)
    return all_matches


# ---------- Búsqueda de equipo por nombre (para usar API-Sports como fuente principal) ----------

def get_team_id_by_name(team_name: str) -> Optional[int]:
    """
    Resuelve el nombre del equipo al ID de API-Sports. Usado cuando el partido viene de
    football-data.org y solo tenemos nombres; así podemos pedir forma/H2H a API-Sports primero.
    Devuelve None si no hay API key, nombre vacío o no se encuentra.
    """
    if not API_KEY or not (team_name and str(team_name).strip()):
        return None
    name = str(team_name).strip()
    resp = _api_get("teams", params={"name": name[:80]})
    if not resp or "response" not in resp:
        return None
    teams = resp.get("response") or []
    if not teams:
        return None
    # Preferir coincidencia exacta (case-insensitive)
    for t in teams:
        if (t.get("name") or "").strip().lower() == name.lower():
            tid = t.get("id")
            if tid is not None:
                return int(tid)
    # Si no hay exacta, devolver el primero (a menudo la búsqueda ya filtra bien)
    first = teams[0]
    tid = first.get("id")
    return int(tid) if tid is not None else None


# ---------- Forma reciente y H2H (API-Sports como fuente principal) ----------

def get_form_last_n_api_sports(team_id: int, n: int = 5) -> List[Dict[str, Any]]:
    """
    Últimos N partidos del equipo (API-Sports). Mismo formato que data_fetcher.get_form_last_n.
    Devuelve lista de dicts con goals_for, goals_against, result (W/D/L).
    """
    if not API_KEY or not team_id:
        return []
    resp = _api_get("fixtures", params={"team": team_id, "last": n})
    if not resp or "response" not in resp:
        return []
    out: List[Dict[str, Any]] = []
    for ev in resp.get("response") or []:
        teams = ev.get("teams") or {}
        home = teams.get("home") or {}
        away = teams.get("away") or {}
        goalse = ev.get("goals") or {}
        gh = goalse.get("home") if goalse.get("home") is not None else 0
        ga = goalse.get("away") if goalse.get("away") is not None else 0
        try:
            gh, ga = int(gh), int(ga)
        except (TypeError, ValueError):
            gh, ga = 0, 0
        home_id = home.get("id")
        is_home = home_id == team_id
        goals_for = gh if is_home else ga
        goals_against = ga if is_home else gh
        if goals_for > goals_against:
            result = "W"
        elif goals_for < goals_against:
            result = "L"
        else:
            result = "D"
        out.append({"goals_for": goals_for, "goals_against": goals_against, "result": result})
    return out[:n]


def get_fixture_by_id(api_sports_fixture_id: int) -> Optional[Dict[str, Any]]:
    """
    Obtiene un fixture por ID (API-Sports GET /fixtures?id=).
    Devuelve {"attendance": int|None, "referee": str|None} o None si no hay datos.
    """
    if not API_KEY or not api_sports_fixture_id:
        return None
    resp = _api_get("fixtures", params={"id": api_sports_fixture_id})
    if not resp or "response" not in resp:
        return None
    items = resp.get("response") or []
    if not items:
        return None
    ev = items[0]
    fixture = ev.get("fixture") or {}
    ref = (fixture.get("referee") or "").strip() or None
    raw_att = fixture.get("attendance")
    try:
        attendance = int(raw_att) if raw_att is not None else None
    except (TypeError, ValueError):
        attendance = None
    return {"attendance": attendance, "referee": ref}


def _parse_stat_int(v: Any) -> int:
    """Convierte value de API a int."""
    if v is None:
        return 0
    try:
        return int(v) if isinstance(v, (int, float)) else int(str(v).replace("'", "").strip() or 0)
    except (ValueError, TypeError):
        return 0


def _parse_team_statistics(statistics: list) -> Dict[str, Any]:
    """Parsea la lista de estadísticas de un equipo de la respuesta de API-Sports."""
    out: Dict[str, Any] = {
        "yellow_cards": None, "red_cards": None,
        "shots": None, "shots_on_target": None,
        "corners": None, "fouls": None, "offsides": None,
        "xg": None,
    }
    for s in (statistics or []):
        if not isinstance(s, dict):
            continue
        t = (s.get("type") or "").strip().lower()
        v = s.get("value")
        if "yellow" in t and "red" not in t:
            out["yellow_cards"] = _parse_stat_int(v)
        elif "red" in t:
            out["red_cards"] = _parse_stat_int(v)
        elif "shots on goal" in t or "shots on target" in t:
            out["shots_on_target"] = _parse_stat_int(v)
        elif "total shots" in t or t == "shots":
            if out["shots"] is None:
                out["shots"] = _parse_stat_int(v)
        elif "corner" in t:
            out["corners"] = _parse_stat_int(v)
        elif "foul" in t and "conceded" not in t:
            out["fouls"] = _parse_stat_int(v)
        elif "offside" in t:
            out["offsides"] = _parse_stat_int(v)
        elif t in ("expected_goals", "xg", "expected goals"):
            try:
                xg_val = float(v) if v is not None else None
                if xg_val is not None:
                    out["xg"] = xg_val
            except (TypeError, ValueError):
                pass
    # Fallback: si shots_on_target no vino separado, usar total shots
    if out["shots_on_target"] is None and out["shots"] is not None and out["shots"] > 0:
        out["shots_on_target"] = out["shots"]
    return out


def get_fixture_statistics_both(fixture_id: int) -> Optional[Dict[str, Any]]:
    """
    Estadísticas completas de un partido en UNA sola llamada (ambos equipos).
    Devuelve dict con claves 'home' y 'away', cada una con:
      shots, shots_on_target, corners, fouls, offsides, yellow_cards, red_cards, xg.
    Retorna None si la API no responde o no hay datos.

    Ventaja vs get_fixture_statistics: 1 llamada por partido en vez de 2 → ahorra 50% de cuota.
    """
    if not API_KEY or not fixture_id:
        return None
    resp = _api_get("fixtures/statistics", params={"fixture": fixture_id})
    if not resp or not resp.get("response"):
        return None

    result: Dict[str, Any] = {"home": None, "away": None}
    for i, team_data in enumerate(resp["response"][:2]):
        if not isinstance(team_data, dict):
            continue
        stats = team_data.get("statistics") or []
        parsed = _parse_team_statistics(stats)
        key = "home" if i == 0 else "away"
        result[key] = parsed

    if result["home"] is None and result["away"] is None:
        return None
    return result


def get_fixture_statistics(fixture_id: int, team_id: int) -> Optional[Dict[str, Any]]:
    """
    Estadísticas de un partido para un equipo específico.
    NOTA: Preferir get_fixture_statistics_both() cuando se necesitan ambos equipos
    (1 llamada vs 2, ahorra 50% de cuota API).
    """
    if not API_KEY or not fixture_id or not team_id:
        return None
    resp = _api_get("fixtures/statistics", params={"fixture": fixture_id, "team": team_id})
    if not resp or "response" not in resp:
        return None
    stats_list = (resp.get("response") or [{}])[0]
    statistics = stats_list.get("statistics") if isinstance(stats_list, dict) else []
    if not statistics:
        return {
            "yellow_cards": 0, "red_cards": 0, "shots": 0, "shots_on_target": 0,
            "corners": 0, "fouls": 0, "offsides": 0, "xg": None,
        }
    out = _parse_team_statistics(statistics)
    # Compatibilidad hacia atrás: devolver 0 en vez de None para campos enteros
    for k in ("yellow_cards", "red_cards", "shots", "shots_on_target", "corners", "fouls", "offsides"):
        if out[k] is None:
            out[k] = 0
    return out


def get_standings_api_sports(league_code: str, season: Optional[int] = None) -> List[Dict[str, Any]]:
    """
    Clasificación de la liga (API-Sports). Formato compatible con data_fetcher.get_standings:
    list of {"rank": int, "team_id": int, "team_name": str} (team_id = API-Sports).
    """
    if not API_KEY:
        return []
    league_id = LEAGUE_CODE_TO_ID.get(league_code)
    if league_id is None:
        return []
    from datetime import datetime
    year = datetime.now().year
    month = datetime.now().month
    if season is None:
        season = year if (league_code == "CLI" or month >= 7) else year - 1
    resp = _api_get("standings", params={"league": league_id, "season": season})
    if not resp or "response" not in resp:
        return []
    out: List[Dict[str, Any]] = []
    for elem in resp.get("response") or []:
        league = elem.get("league") or {}
        standings = league.get("standings") or []
        for group in standings:
            rows = group if isinstance(group, list) else [group]
            for row in rows:
                r = row if isinstance(row, dict) else {}
                team = r.get("team") or {}
                rank = r.get("rank") or r.get("position")
                tid = team.get("id")
                if rank is None or tid is None:
                    continue
                points = r.get("points")
                all_stats = r.get("all") or {}
                if isinstance(all_stats, dict):
                    wins = all_stats.get("win")
                    draws = all_stats.get("draw")
                    losses = all_stats.get("lose")
                else:
                    wins = draws = losses = None
                entry = {
                    "rank": int(rank),
                    "team_id": int(tid),
                    "team_name": (team.get("name") or "").strip() or "",
                }
                if points is not None:
                    entry["points"] = int(points)
                if wins is not None:
                    entry["wins"] = int(wins)
                if draws is not None:
                    entry["draws"] = int(draws)
                if losses is not None:
                    entry["losses"] = int(losses)
                out.append(entry)
    out.sort(key=lambda x: x["rank"])
    return out


def get_finished_fixtures_api_sports(
    league_code: str,
    date_from: str,
    date_to: str,
) -> List[Dict[str, Any]]:
    """
    Partidos ya finalizados en la liga (API-Sports). date_from/date_to: YYYY-MM-DD.
    Devuelve lista de {fixture_id (remapped 9xx), home_goals, away_goals} para actualizar BD.
    """
    if not API_KEY:
        return []
    league_id = LEAGUE_CODE_TO_ID.get(league_code)
    if league_id is None:
        return []
    season = _season_from_date(date_from, league_code)
    params: Dict[str, Any] = {
        "league": league_id,
        "season": season,
        "from": date_from,
        "to": date_to,
        "status": "FT-AET-PEN",
    }
    resp = _api_get("fixtures", params=params)
    if not resp or "response" not in resp:
        return []
    out: List[Dict[str, Any]] = []
    for ev in resp.get("response") or []:
        fixture = ev.get("fixture") or {}
        status = (fixture.get("status") or {}).get("short")
        if status not in ("FT", "AET", "PEN"):
            continue
        fid = fixture.get("id") or 0
        fid_forgewin = 900_000_000 + (int(fid) % 99_999_999)
        goalse = ev.get("goals") or {}
        gh = goalse.get("home")
        ga = goalse.get("away")
        if gh is None or ga is None:
            continue
        try:
            out.append({
                "fixture_id": fid_forgewin,
                "home_goals": int(gh),
                "away_goals": int(ga),
            })
        except (TypeError, ValueError):
            continue
    return out


def _season_date_range(master_season: int, league_code: str = "") -> tuple:
    """
    Devuelve (date_from, date_to) YYYY-MM-DD para la temporada master (año fin).
    Ej: master_season 2024 = 2023/24 → 2023-08-01 a 2024-05-31 (ligas europeas).
    CLI usa año civil.
    """
    if league_code == "CLI":
        return (f"{master_season}-01-01", f"{master_season}-12-31")
    # Europeas: ago-may
    prev_year = master_season - 1
    return (f"{prev_year}-08-01", f"{master_season}-05-31")


def get_fixtures_api_sports_for_season(
    league_code: str,
    master_season: int,
) -> List[Dict[str, Any]]:
    """
    Partidos finalizados de API-Sports para una temporada completa.
    master_season = año fin (2024 = 2023/24).
    Devuelve lista con fixture_id, date, league_id, home_team_name, away_team_name,
    home_goals, away_goals, api_sports_fixture_id, etc.
    """
    date_from, date_to = _season_date_range(master_season, league_code)
    return get_finished_fixtures_for_historical(league_code, date_from, date_to)


def get_finished_fixtures_for_historical(
    league_code: str,
    date_from: str,
    date_to: str,
) -> List[Dict[str, Any]]:
    """
    Partidos finalizados para historial rolling (tabla historical_matches).
    date_from/date_to: YYYY-MM-DD. Devuelve filas con fixture_id, date, league_id,
    home_team_id, away_team_id, home_goals, away_goals, status, season (y opcional home_xg, away_xg).
    """
    if not API_KEY:
        return []
    league_id = LEAGUE_CODE_TO_ID.get(league_code)
    if league_id is None:
        return []
    season = _season_from_date(date_from, league_code)
    params: Dict[str, Any] = {
        "league": league_id,
        "season": season,
        "from": date_from,
        "to": date_to,
        "status": "FT-AET-PEN",
    }
    resp = _api_get("fixtures", params=params)
    if not resp or "response" not in resp:
        return []
    out: List[Dict[str, Any]] = []
    for ev in resp.get("response") or []:
        fixture = ev.get("fixture") or {}
        status_short = (fixture.get("status") or {}).get("short")
        if status_short not in ("FT", "AET", "PEN"):
            continue
        fid = fixture.get("id") or 0
        fid_forgewin = 900_000_000 + (int(fid) % 99_999_999)
        date_str = (fixture.get("date") or "")[:10]
        if not date_str and len((fixture.get("date") or "")) >= 10:
            date_str = (fixture.get("date") or "")[:10]
        if len(date_str) < 10:
            continue
        teams = ev.get("teams") or {}
        home = teams.get("home") or {}
        away = teams.get("away") or {}
        goalse = ev.get("goals") or {}
        gh, ga = goalse.get("home"), goalse.get("away")
        if gh is None or ga is None:
            continue
        try:
            gh, ga = int(gh), int(ga)
        except (TypeError, ValueError):
            continue
        home_xg = away_xg = None  # Opcional: rellenar desde ev.get("statistics") si el plan lo incluye
        # API-Sports fixture: referee (string), attendance (int, opcional)
        ref = (fixture.get("referee") or "").strip() or None
        raw_att = fixture.get("attendance")
        try:
            attendance = int(raw_att) if raw_att is not None else None
        except (TypeError, ValueError):
            attendance = None
        out.append({
            "fixture_id": fid_forgewin,
            "api_sports_fixture_id": int(fid),
            "date": date_str,
            "league_id": league_code,
            "home_team_id": home.get("id"),
            "away_team_id": away.get("id"),
            "home_team_name": (home.get("name") or "").strip() or None,
            "away_team_name": (away.get("name") or "").strip() or None,
            "home_goals": gh,
            "away_goals": ga,
            "status": status_short,
            "season": season,
            "home_xg": home_xg,
            "away_xg": away_xg,
            "attendance": attendance,
            "referee": ref,
        })
    return out


def get_h2h_api_sports(team_id_home: int, team_id_away: int, limit: int = 5) -> List[Dict[str, Any]]:
    """
    Enfrentamientos directos entre dos equipos (API-Sports). Mismo formato que data_fetcher.get_h2h.
    Devuelve lista de dicts con home_goals, away_goals, winner ("home"/"away"/"draw").
    """
    if not API_KEY or not team_id_home or not team_id_away:
        return []
    h2h_param = f"{team_id_home}-{team_id_away}"
    resp = _api_get("fixtures/headtohead", params={"h2h": h2h_param})
    if not resp or "response" not in resp:
        return []
    out: List[Dict[str, Any]] = []
    for ev in (resp.get("response") or [])[:limit]:
        teams = ev.get("teams") or {}
        home = teams.get("home") or {}
        away = teams.get("away") or {}
        goalse = ev.get("goals") or {}
        gh = goalse.get("home") if goalse.get("home") is not None else 0
        ga = goalse.get("away") if goalse.get("away") is not None else 0
        try:
            gh, ga = int(gh), int(ga)
        except (TypeError, ValueError):
            gh, ga = 0, 0
        winner = "home" if gh > ga else ("away" if ga > gh else "draw")
        home_name = (home.get("name") or "").strip() or None
        away_name = (away.get("name") or "").strip() or None
        out.append({
            "home_goals": gh,
            "away_goals": ga,
            "winner": winner,
            "home_team_name": home_name,
            "away_team_name": away_name,
        })
    return out
