"""
Módulo de Datos por Partido - Stats de equipos, H2H, forma.
Fuente principal: API-Sports. Secundaria: football-data.org v4.
"""

import logging
import time
from typing import Any, Dict, List, Optional

import requests

from config import (
    BASE_URL,
    API_KEY,
    REQUEST_DELAY_SECONDS,
    get_league_name,
)

logger = logging.getLogger(__name__)


def _api_get(path: str, params: Optional[Dict[str, Any]] = None, use_mock: bool = False) -> dict:
    """GET a football-data.org v4. X-Auth-Token."""
    if use_mock:
        logger.debug("data_fetcher: use_mock=True path=%s", path)
        return _mock_response(path, params or {})
    if not API_KEY:
        logger.warning("data_fetcher: API_KEY no configurada, usando mock. path=%s", path)
        return _mock_response(path, params or {})

    url = f"{BASE_URL.rstrip('/')}/{path.lstrip('/')}"
    headers = {"X-Auth-Token": API_KEY}
    try:
        time.sleep(REQUEST_DELAY_SECONDS)
        r = requests.get(url, headers=headers, params=params, timeout=15)
        r.raise_for_status()
        return r.json()
    except requests.RequestException as e:
        logger.exception("data_fetcher: error GET %s: %s", path, e)
        return {"message": str(e), "standings": [], "matches": []}


def _mock_response(path: str, params: dict) -> dict:
    """Respuestas mock para testing sin API key."""
    if "standings" in path:
        code = path.split("/")[1] if "competitions" in path else "PL"
        return {
            "standings": [
                {
                    "type": "TOTAL",
                    "table": [
                        {"position": i, "team": {"id": 100 + i, "name": f"Team{i}"}}
                        for i in range(1, 21)
                    ],
                }
            ]
        }
    if "teams" in path and "matches" in path:
        parts = path.strip("/").split("/")
        team_id = int(parts[1]) if len(parts) > 1 else 57
        n = min(int(params.get("limit", 10)), 10)
        return {
            "matches": [
                {
                    "id": 800000 + i,
                    "homeTeam": {"id": team_id, "name": "Home"},
                    "awayTeam": {"id": 50 + i, "name": "Away"},
                    "score": {"fullTime": {"home": 1 + (i % 2), "away": i % 2}},
                }
                for i in range(n)
            ]
        }
    if "head2head" in path:
        return {
            "head2head": [
                {"score": {"fullTime": {"home": 1, "away": 0}}} for _ in range(5)
            ]
        }
    return {"matches": [], "standings": []}


def _current_season() -> str:
    """Año de inicio de temporada (ej: 2024 para 2024-25)."""
    from datetime import datetime
    y = datetime.now().year
    m = datetime.now().month
    if m >= 7:
        return str(y)
    return str(y - 1)


def get_standings(league_code: str, season: Optional[str] = None, use_mock: bool = False) -> list:
    """Clasificación: primero API-Sports, si no hay datos usa football-data.org."""
    if not use_mock:
        try:
            from api_sports_fetcher import get_standings_api_sports, LEAGUE_CODE_TO_ID
            if league_code in LEAGUE_CODE_TO_ID:
                out_primary = get_standings_api_sports(
                    league_code,
                    season=int(season) if season else None,
                )
                if out_primary:
                    logger.info("data_fetcher: get_standings %s -> %d equipos (API-Sports)", league_code, len(out_primary))
                    return out_primary
        except Exception as e:
            logger.debug("data_fetcher: get_standings API-Sports %s: %s", league_code, e)
    path = f"competitions/{league_code}/standings"
    data = _api_get(path, use_mock=use_mock)
    if data.get("message"):
        logger.warning("data_fetcher: get_standings %s API message: %s", league_code, data["message"])
    out = []
    for group in data.get("standings") or []:
        if group.get("type") != "TOTAL":
            continue
        for row in group.get("table") or []:
            team = row.get("team") or {}
            out.append({
                "rank": row.get("position") or row.get("rank"),
                "team_id": team.get("id"),
                "team_name": team.get("name"),
            })
    logger.info("data_fetcher: get_standings %s -> %d equipos (football-data)", league_code, len(out))
    return out


def get_top_n_teams(league_code: str, n: int = 7, use_mock: bool = False) -> list:
    """Top N equipos de la liga (por posición en la tabla)."""
    standings = get_standings(league_code, use_mock=use_mock)
    return standings[:n]


def get_team_stats(
    team_id: int,
    league_code: str,
    season: Optional[str] = None,
    use_mock: bool = False,
) -> dict[str, Any]:
    """
    Rendimiento del equipo: primero historical_matches (tabla maestra), luego API-Sports, luego football-data.org.
    """
    season = season or _current_season()
    if not use_mock:
        try:
            from historical_analyzer import get_recent_form
            form = get_recent_form(team_id=team_id, league_id=league_code, last_n=20, use_master_checked=True)
            if not form:
                form = get_recent_form(team_id=team_id, league_id=league_code, last_n=20, use_master_checked=False)
            if form:
                goals_for = sum(f.get("goals_for", 0) for f in form)
                goals_against = sum(f.get("goals_against", 0) for f in form)
                wins = sum(1 for f in form if (f.get("goals_for", 0) > f.get("goals_against", 0)))
                n = len(form) or 1
                return {
                    "team_id": team_id,
                    "league_id": league_code,
                    "season": season,
                    "matches_played": n,
                    "goals_avg_for": goals_for / n,
                    "goals_avg_against": goals_against / n,
                    "win_rate": wins / n,
                    "vs_top5": {"goals_for": 0, "goals_against": 0, "n": 0},
                    "vs_bottom5": {"goals_for": 0, "goals_against": 0, "n": 0},
                    "form_fixtures": [{"goals_for": x.get("goals_for", 0), "goals_against": x.get("goals_against", 0), "result": "W" if x.get("goals_for", 0) > x.get("goals_against", 0) else ("L" if x.get("goals_for", 0) < x.get("goals_against", 0) else "D")} for x in form[:10]],
                }
        except Exception as e:
            logger.debug("data_fetcher: get_team_stats historical_matches: %s", e)
        try:
            from api_sports_fetcher import get_standings_api_sports, get_form_last_n_api_sports, LEAGUE_CODE_TO_ID
            if league_code in LEAGUE_CODE_TO_ID:
                form = get_form_last_n_api_sports(team_id, n=20)
                if form:
                    goals_for = sum(f.get("goals_for", 0) for f in form)
                    goals_against = sum(f.get("goals_against", 0) for f in form)
                    wins = sum(1 for f in form if f.get("result") == "W")
                    n = len(form) or 1
                    return {
                        "team_id": team_id,
                        "league_id": league_code,
                        "season": season,
                        "matches_played": n,
                        "goals_avg_for": goals_for / n,
                        "goals_avg_against": goals_against / n,
                        "win_rate": wins / n,
                        "vs_top5": {"goals_for": 0, "goals_against": 0, "n": 0},
                        "vs_bottom5": {"goals_for": 0, "goals_against": 0, "n": 0},
                        "form_fixtures": form[:10],
                    }
        except Exception as e:
            logger.debug("data_fetcher: get_team_stats API-Sports team_id=%s: %s", team_id, e)

    standings = get_standings(league_code, season, use_mock)
    top5_ids = {s["team_id"] for s in standings[:5]}
    bottom5_ids = {s["team_id"] for s in standings[-5:]}

    data = _api_get(
        f"teams/{team_id}/matches",
        {"status": "FINISHED", "limit": 20},
        use_mock=use_mock,
    )
    fixtures = data.get("matches") or []
    goals_for = 0
    goals_against = 0
    wins = 0
    vs_top5_gf, vs_top5_ga = 0, 0
    vs_bottom5_gf, vs_bottom5_ga = 0, 0
    n_top5, n_bottom5 = 0, 0

    for f in fixtures:
        home_team = f.get("homeTeam") or {}
        away_team = f.get("awayTeam") or {}
        home_id = home_team.get("id")
        away_id = away_team.get("id")
        score = f.get("score") or {}
        ft = score.get("fullTime") or {}
        gh = ft.get("home")
        ga = ft.get("away")
        if gh is None:
            gh = 0
        if ga is None:
            ga = 0
        is_home = home_id == team_id
        gf = gh if is_home else ga
        ga_opp = ga if is_home else gh
        goals_for += gf
        goals_against += ga_opp
        if gf > ga_opp:
            wins += 1
        opp_id = away_id if is_home else home_id
        if opp_id in top5_ids:
            vs_top5_gf += gf
            vs_top5_ga += ga_opp
            n_top5 += 1
        if opp_id in bottom5_ids:
            vs_bottom5_gf += gf
            vs_bottom5_ga += ga_opp
            n_bottom5 += 1

    n = len(fixtures) or 1
    return {
        "team_id": team_id,
        "league_id": league_code,
        "season": season,
        "matches_played": n,
        "goals_avg_for": goals_for / n,
        "goals_avg_against": goals_against / n,
        "win_rate": wins / n,
        "vs_top5": {"goals_for": vs_top5_gf, "goals_against": vs_top5_ga, "n": n_top5},
        "vs_bottom5": {"goals_for": vs_bottom5_gf, "goals_against": vs_bottom5_ga, "n": n_bottom5},
        "form_fixtures": fixtures[:10],
    }


def get_h2h(
    match_id: int,
    limit: int = 5,
    use_mock: bool = False,
) -> list[dict]:
    """
    Enfrentamientos directos previos entre los dos equipos del partido.
    GET /matches/{id}/head2head?limit=N
    """
    data = _api_get(f"matches/{match_id}/head2head", {"limit": limit}, use_mock=use_mock)
    out = []
    # La API puede devolver head2head como objeto con lista de matches
    head2head = data.get("head2head")
    if isinstance(head2head, list):
        matches_h2h = head2head
    elif isinstance(head2head, dict):
        matches_h2h = head2head.get("matches") or head2head.get("head2head") or []
    else:
        matches_h2h = data.get("matches") or []
    for m in matches_h2h[:limit]:
        score = m.get("score") or {}
        ft = score.get("fullTime") or {}
        gh = ft.get("home")
        ga = ft.get("away")
        if gh is None:
            gh = 0
        if ga is None:
            ga = 0
        winner = "home" if gh > ga else ("away" if ga > gh else "draw")
        home_team = m.get("homeTeam") or {}
        away_team = m.get("awayTeam") or {}
        home_name = (home_team.get("name") or "").strip() or None
        away_name = (away_team.get("name") or "").strip() or None
        out.append({
            "home_goals": gh,
            "away_goals": ga,
            "winner": winner,
            "home_team_name": home_name,
            "away_team_name": away_name,
        })
    return out


def get_injuries(fixture_id: int, use_mock: bool = False) -> list[dict]:
    """
    football-data.org no expone endpoint de lesiones en el plan estándar.
    Devolvemos lista vacía (el analizador ajusta por lesiones si hay datos externos).
    """
    if use_mock:
        return [{"player_name": "Mock Out", "team_id": 0, "contribution_estimate": 0.15}]
    return []


def get_form_last_n(
    team_id: int,
    n: int = 5,
    competition: str = "all",
    league_code: Optional[str] = None,
    use_mock: bool = False,
) -> list[dict]:
    """Últimos N partidos del equipo con resultado y goles."""
    data = _api_get(f"teams/{team_id}/matches", {"status": "FINISHED", "limit": n}, use_mock=use_mock)
    matches = data.get("matches") or []
    out = []
    for f in matches:
        home_team = f.get("homeTeam") or {}
        away_team = f.get("awayTeam") or {}
        score = f.get("score") or {}
        ft = score.get("fullTime") or {}
        gh = ft.get("home")
        ga = ft.get("away")
        if gh is None:
            gh = 0
        if ga is None:
            ga = 0
        is_home = (home_team.get("id")) == team_id
        out.append({
            "goals_for": gh if is_home else ga,
            "goals_against": ga if is_home else gh,
            "result": "W" if (gh > ga and is_home) or (ga > gh and not is_home) else ("L" if (gh < ga and is_home) or (ga < gh and not is_home) else "D"),
        })
    return out


def _normalize_team_name_for_match(name: Optional[str]) -> str:
    """Normaliza nombre para comparación: minúsculas, unicode→ascii (ø→o, ğ→g), guiones→espacios."""
    if not name:
        return ""
    s = (name or "").strip().lower()
    # Unicode a ASCII para emparejar "Bodø" con "Bodo", "Qarabağ" con "Qarabag"
    for old, new in [
        ("ø", "o"), ("œ", "oe"), ("ğ", "g"), ("ı", "i"), ("ł", "l"), ("ń", "n"),
        ("ş", "s"), ("ţ", "t"), ("ž", "z"), ("ć", "c"), ("ę", "e"), ("ą", "a"),
    ]:
        s = s.replace(old, new)
    # Guiones como espacios: "Saint-Germain" = "Saint Germain"
    s = s.replace("-", " ")
    s = " ".join(s.split())
    return s


def _team_names_match(our_home: str, our_away: str, fd_home: str, fd_away: str) -> bool:
    """True si los nombres del partido coinciden (contiene o es igual tras normalizar)."""

    def _name_match(a: str, b: str) -> bool:
        if not a or not b or len(a) <= 1 or len(b) <= 1:
            return False
        if a in b or b in a:
            return True
        # Palabras "de relleno" que suelen ser prefijos/sufijos (FC, FK, PAE, etc.)
        skip = {"fc", "fk", "cf", "pa", "pae", "as", "bc", "sk", "de", "e", "kv", "sfp", "bc"}
        words_a = [w for w in a.split() if w not in skip and len(w) > 1]
        words_b = [w for w in b.split() if w not in skip and len(w) > 1]
        if not words_a or not words_b:
            return False
        # Que la palabra más larga de uno aparezca en el otro (ej. "olympiakos" en "pae olympiakos sfp")
        longest_a = max(words_a, key=len)
        longest_b = max(words_b, key=len)
        return longest_a in b or longest_b in a

    nh = _normalize_team_name_for_match(our_home)
    na = _normalize_team_name_for_match(our_away)
    fh = _normalize_team_name_for_match(fd_home)
    fa = _normalize_team_name_for_match(fd_away)
    if not nh or not na or not fh or not fa:
        return False
    home_ok = _name_match(nh, fh)
    away_ok = _name_match(na, fa)
    return home_ok and away_ok


def get_match_statistics_football_data_org(
    league_code: str,
    date: str,
    home_team_name: str,
    away_team_name: str,
) -> Optional[Dict[str, Any]]:
    """
    Obtiene estadísticas de un partido desde football-data.org v4 (fallback cuando API-Sports devuelve 0-0).
    Busca partidos de la competición en esa fecha, empareja por nombres de equipos y devuelve
    home_shots_target, away_shots_target, home_corners, away_corners, home_fouls, away_fouls,
    home_offsides, away_offsides, home_yellow, away_yellow, home_red, away_red.
    date: YYYY-MM-DD. league_code: PL, PD, SA, BL1, FL1, CL, EL, etc. (códigos config).
    """
    if not API_KEY or not league_code or not date or len(date) < 10:
        return None
    # dateTo es exclusivo en football-data.org: para incluir date usamos dateTo = date + 1 día
    try:
        from datetime import datetime, timedelta
        d = datetime.strptime(date[:10], "%Y-%m-%d")
        date_to = (d + timedelta(days=1)).strftime("%Y-%m-%d")
    except Exception:
        date_to = date
    path = f"competitions/{league_code}/matches"
    params = {"dateFrom": date[:10], "dateTo": date_to}
    try:
        data = _api_get(path, params=params)
    except Exception as e:
        logger.debug("football-data.org matches %s %s: %s", league_code, date, e)
        return None
    if data.get("message"):
        logger.debug("football-data.org matches %s %s: %s", league_code, date, data.get("message"))
        return None
    matches: List[Dict[str, Any]] = data.get("matches") or []
    match_id = None
    for m in matches:
        ht = (m.get("homeTeam") or {}).get("name") or ""
        at = (m.get("awayTeam") or {}).get("name") or ""
        if _team_names_match(home_team_name, away_team_name, ht, at):
            match_id = m.get("id")
            break
    if match_id is None:
        logger.debug("football-data.org: no match found %s %s %s vs %s", league_code, date, home_team_name, away_team_name)
        return None
    try:
        detail = _api_get(f"matches/{match_id}")
    except Exception as e:
        logger.debug("football-data.org match %s: %s", match_id, e)
        return None
    if detail.get("message") or not detail.get("homeTeam") or not detail.get("awayTeam"):
        return None
    home_stats = (detail.get("homeTeam") or {}).get("statistics") or {}
    away_stats = (detail.get("awayTeam") or {}).get("statistics") or {}
    if not home_stats and not away_stats:
        # Partido encontrado pero sin estadísticas (p. ej. aún no jugado)
        logger.debug("football-data.org: match %s found but no statistics", match_id)
        return {"_match_found_no_stats": True, "match_id": match_id}
    def _int(v: Any) -> Optional[int]:
        if v is None:
            return None
        try:
            return int(v)
        except (TypeError, ValueError):
            return None
    return {
        "home_shots_target": _int(home_stats.get("shots_on_goal")),
        "away_shots_target": _int(away_stats.get("shots_on_goal")),
        "home_shots": _int(home_stats.get("shots")),
        "away_shots": _int(away_stats.get("shots")),
        "home_corners": _int(home_stats.get("corner_kicks")),
        "away_corners": _int(away_stats.get("corner_kicks")),
        "home_fouls": _int(home_stats.get("fouls")),
        "away_fouls": _int(away_stats.get("fouls")),
        "home_offsides": _int(home_stats.get("offsides")),
        "away_offsides": _int(away_stats.get("offsides")),
        "home_yellow": _int(home_stats.get("yellow_cards")),
        "away_yellow": _int(away_stats.get("yellow_cards")),
        "home_red": _int(home_stats.get("red_cards")),
        "away_red": _int(away_stats.get("red_cards")),
    }


def fetch_cl_matches_football_data_org(
    date_from: str,
    date_to: str,
    chunk_months: int = 6,
) -> List[Dict[str, Any]]:
    """
    Obtiene todos los partidos de Champions League desde football-data.org v4
    en el rango [date_from, date_to]. date_from/date_to en YYYY-MM-DD.
    Hace peticiones por bloques de chunk_months meses para respetar cuota.
    Devuelve lista de partidos con id, utcDate, homeTeam, awayTeam, score, status.
    """
    if not API_KEY or not date_from or not date_to:
        return []
    from datetime import datetime, timedelta
    out: List[Dict[str, Any]] = []
    try:
        start = datetime.strptime(date_from[:10], "%Y-%m-%d")
        end = datetime.strptime(date_to[:10], "%Y-%m-%d")
    except Exception:
        return []
    current = start
    while current <= end:
        chunk_end = current + timedelta(days=chunk_months * 31)
        if chunk_end > end:
            chunk_end = end
        date_from_str = current.strftime("%Y-%m-%d")
        date_to_str = (chunk_end + timedelta(days=1)).strftime("%Y-%m-%d")
        path = "competitions/CL/matches"
        params = {"dateFrom": date_from_str, "dateTo": date_to_str}
        try:
            data = _api_get(path, params=params)
        except Exception as e:
            logger.warning("fetch_cl_matches_football_data_org %s–%s: %s", date_from_str, date_to_str, e)
            current = chunk_end + timedelta(days=1)
            continue
        if data.get("message"):
            logger.warning("fetch_cl_matches_football_data_org %s: %s", date_from_str, data.get("message"))
            current = chunk_end + timedelta(days=1)
            continue
        matches = data.get("matches") or []
        for m in matches:
            m["_fd_date_from"] = date_from_str
            m["_fd_date_to"] = date_to_str
        out.extend(matches)
        logger.info("football-data.org CL %s–%s: %d partidos", date_from_str, date_to_str, len(matches))
        current = chunk_end + timedelta(days=1)
    return out
