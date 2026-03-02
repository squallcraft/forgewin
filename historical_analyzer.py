"""
Análisis sobre historical_matches para el modelo Poisson: forma reciente, H2H, promedios y bias de liga.
Usa tabla historical_matches (rolling window). Soporta búsqueda por team_id (API-Sports) o por nombre (CSV).
"""

import logging
from typing import Any, Dict, List, Optional

from db import (
    get_historical_matches_for_team,
    get_historical_matches_for_team_from_master_checked,
    get_historical_h2h,
    get_historical_h2h_from_master_checked,
    get_historical_league_goals,
    get_historical_match_seasons,
)
from rolling_window import get_current_window_seasons

logger = logging.getLogger(__name__)


def get_recent_form(
    team_id: Optional[int] = None,
    team_name: Optional[str] = None,
    league_id: Optional[str] = None,
    last_n: int = 10,
    use_master_checked: bool = False,
) -> List[Dict[str, Any]]:
    """
    Últimos N partidos del equipo.
    Formato: [{"goals_for", "goals_against", "date", "league_id", "season"}, ...] más reciente primero.
    Usar team_id cuando tengas ID (API-Sports); team_name + league_id para datos de CSV.
    Si use_master_checked=True, usa master_table_checked (datos verificados) primero; si vacío, fallback a historical_matches.
    """
    if team_id is None and not (team_name and league_id):
        return []
    if use_master_checked:
        matches = get_historical_matches_for_team_from_master_checked(
            team_id=team_id, team_name=team_name, league_id=league_id, last_n=last_n
        )
        if matches:
            return matches
    return get_historical_matches_for_team(
        team_id=team_id, team_name=team_name, league_id=league_id, last_n=last_n
    )


def get_head_to_head(
    home_id: Optional[int] = None,
    away_id: Optional[int] = None,
    home_name: Optional[str] = None,
    away_name: Optional[str] = None,
    league_id: Optional[str] = None,
    last_n: int = 8,
    use_master_checked: bool = False,
) -> List[Dict[str, Any]]:
    """
    Enfrentamientos directos desde historial. Formato: [{"date", "home_goals", "away_goals"}, ...].
    Por IDs (home_id, away_id) o por nombres (home_name, away_name, league_id).
    Si use_master_checked=True, usa master_table_checked primero; si vacío, fallback a historical_matches.
    """
    if use_master_checked:
        if home_id is not None and away_id is not None:
            h2h = get_historical_h2h_from_master_checked(home_id=home_id, away_id=away_id, last_n=last_n)
        elif home_name and away_name and league_id:
            h2h = get_historical_h2h_from_master_checked(home_name=home_name, away_name=away_name, league_id=league_id, last_n=last_n)
        else:
            h2h = []
        if h2h:
            return h2h
    if home_id is not None and away_id is not None:
        return get_historical_h2h(home_id=home_id, away_id=away_id, last_n=last_n)
    if home_name and away_name and league_id:
        return get_historical_h2h(home_name=home_name, away_name=away_name, league_id=league_id, last_n=last_n)
    return []


def get_average_goals_last_seasons(
    team_id: Optional[int] = None,
    team_name: Optional[str] = None,
    league_id: Optional[str] = None,
    seasons: int = 3,
) -> Dict[str, float]:
    """
    Promedio de goles a favor y en contra del equipo en las últimas N temporadas (desde historial).
    Devuelve {"goals_for_avg", "goals_against_avg", "matches_count"}.
    """
    allowed = get_current_window_seasons()
    season_list = allowed[-seasons:] if seasons else allowed
    matches = get_historical_matches_for_team(
        team_id=team_id,
        team_name=team_name,
        league_id=league_id,
        last_n=500,
    )
    # Filtrar por temporadas deseadas
    matches = [m for m in matches if m.get("season") in season_list]
    if not matches:
        return {"goals_for_avg": 1.2, "goals_against_avg": 1.2, "matches_count": 0}
    n = len(matches)
    gf_avg = sum(m["goals_for"] for m in matches) / n
    ga_avg = sum(m["goals_against"] for m in matches) / n
    return {"goals_for_avg": round(gf_avg, 3), "goals_against_avg": round(ga_avg, 3), "matches_count": n}


def recalculate_lambda_bias(league_id: str, seasons: Optional[int] = None) -> float:
    """
    Multiplicador de corrección para la liga según desviación real vs esperado.
    Si en la liga se marcan más goles que la media estándar (2.7 total por partido), factor > 1.
    Devuelve un factor recomendado para multiplicar (lambda_home + lambda_away) o cada lambda.
    """
    allowed = get_current_window_seasons()
    season_list = allowed[-(seasons or 3) :] if seasons else allowed
    rows = get_historical_league_goals(league_id, seasons=season_list)
    if not rows:
        return 1.0
    total_goals = sum(r["home_goals"] + r["away_goals"] for r in rows)
    n = len(rows)
    actual_avg = total_goals / n
    # Referencia: 2.7 goles por partido típico
    reference_avg = 2.7
    factor = actual_avg / reference_avg if reference_avg else 1.0
    # Limitar para no distorsionar
    factor = max(0.85, min(1.15, factor))
    return round(factor, 3)


def enrich_match_stats_from_history(
    home_id: Optional[int] = None,
    away_id: Optional[int] = None,
    home_name: Optional[str] = None,
    away_name: Optional[str] = None,
    league_id: Optional[str] = None,
    form_n: int = 10,
    h2h_n: int = 8,
    seasons_avg: int = 3,
) -> Dict[str, Any]:
    """
    Devuelve un dict con forma reciente, H2H y promedios desde historial para enriquecer Poisson.
    Claves: recent_form_home, recent_form_away, h2h, avg_goals_home, avg_goals_away, lambda_bias.
    """
    out = {
        "recent_form_home": [],
        "recent_form_away": [],
        "h2h": [],
        "avg_goals_home": {"goals_for_avg": 1.2, "goals_against_avg": 1.2, "matches_count": 0},
        "avg_goals_away": {"goals_for_avg": 1.1, "goals_against_avg": 1.2, "matches_count": 0},
        "lambda_bias": 1.0,
    }
    if not league_id:
        return out
    out["recent_form_home"] = get_recent_form(team_id=home_id, team_name=home_name, league_id=league_id, last_n=form_n)
    out["recent_form_away"] = get_recent_form(team_id=away_id, team_name=away_name, league_id=league_id, last_n=form_n)
    out["h2h"] = get_head_to_head(
        home_id=home_id, away_id=away_id,
        home_name=home_name, away_name=away_name,
        league_id=league_id, last_n=h2h_n,
    )
    out["avg_goals_home"] = get_average_goals_last_seasons(team_id=home_id, team_name=home_name, league_id=league_id, seasons=seasons_avg)
    out["avg_goals_away"] = get_average_goals_last_seasons(team_id=away_id, team_name=away_name, league_id=league_id, seasons=seasons_avg)
    out["lambda_bias"] = recalculate_lambda_bias(league_id, seasons=seasons_avg)
    return out
