"""
Módulo de Cálculos y Predicciones - Poisson, probabilidades, queries.
Sin usar odds de bookies; todo basado en stats históricas, forma, H2H, lesiones.
"""

import logging
from typing import Any, List, Optional, Union

logger = logging.getLogger(__name__)

import pandas as pd
from scipy.stats import poisson

from config import get_league_id, get_league_name, TOP_10_LEAGUE_CODES
from scraper import get_upcoming_matches
from data_fetcher import (
    get_team_stats,
    get_h2h,
    get_injuries,
    get_form_last_n,
    get_standings,
    _current_season,
)
from odds import get_match_odds

try:
    from data_integrators import get_external_data
    _HAS_INTEGRATOR = True
except ImportError:
    _HAS_INTEGRATOR = False

try:
    from historical_analyzer import enrich_match_stats_from_history
    _HAS_HISTORICAL = True
except ImportError:
    _HAS_HISTORICAL = False


def _lambda_attack_defense(goals_avg_for: float, goals_avg_against_opponent: float) -> float:
    """Lambda para Poisson: ataque del equipo * defensa del rival (goles encajados por el rival)."""
    return max(0.1, goals_avg_for * (goals_avg_against_opponent / 1.5 if goals_avg_against_opponent else 1.0))


def calculate_probabilities(
    home_stats: dict[str, Any],
    away_stats: dict[str, Any],
    h2h: list[dict],
    injuries: list[dict],
    injury_impact_home: float = 0.0,
    injury_impact_away: float = 0.0,
    bookmaker_odds: Optional[dict[str, float]] = None,
    blend_weight_poisson: float = 0.6,
    external_data: Optional[dict[str, Any]] = None,
    lambda_bias: float = 1.0,
) -> dict[str, Any]:
    """
    Calcula probabilidades usando Poisson y opcionalmente mezcla con cuotas de mercado.
    bookmaker_odds: {"home_win", "draw", "away_win"} en cuotas decimales.
    blend_weight_poisson: peso del modelo Poisson (1-blend = peso de las casas).
    external_data: opcional {"xG_home", "xG_away"} para ajustar lambdas.
    lambda_bias: multiplicador por liga (historical_analyzer.recalculate_lambda_bias).
    """
    # Lambdas base: ataque vs defensa del otro
    lambda_home = _lambda_attack_defense(
        home_stats.get("goals_avg_for", 1.2),
        away_stats.get("goals_avg_against", 1.2),
    )
    lambda_away = _lambda_attack_defense(
        away_stats.get("goals_avg_for", 1.1),
        home_stats.get("goals_avg_against", 1.2),
    )

    # Ajuste H2H: si en últimos H2H un equipo anotó más, subir un poco su lambda
    if h2h:
        h_home = sum(m.get("home_goals", 0) for m in h2h) / len(h2h)
        h_away = sum(m.get("away_goals", 0) for m in h2h) / len(h2h)
        lambda_home = 0.7 * lambda_home + 0.3 * max(0.2, h_home)
        lambda_away = 0.7 * lambda_away + 0.3 * max(0.2, h_away)

    # Ajuste xG externo (data_integrators): factor sobre lambda si hay xG
    if external_data:
        xg_h = external_data.get("xG_home")
        xg_a = external_data.get("xG_away")
        avg_h = home_stats.get("goals_avg_for") or 1.2
        avg_a = away_stats.get("goals_avg_for") or 1.1
        if xg_h is not None and avg_h and float(avg_h) > 0:
            lambda_home *= float(xg_h) / float(avg_h)
        if xg_a is not None and avg_a and float(avg_a) > 0:
            lambda_away *= float(xg_a) / float(avg_a)
        lambda_home = max(0.15, lambda_home)
        lambda_away = max(0.15, lambda_away)

    # Lesiones: reducir lambda del equipo afectado
    lambda_home *= (1.0 - injury_impact_home)
    lambda_away *= (1.0 - injury_impact_away)
    lambda_home = max(0.15, lambda_home)
    lambda_away = max(0.15, lambda_away)

    # Corrección por liga (historial rolling)
    lambda_home *= lambda_bias
    lambda_away *= lambda_bias
    lambda_home = max(0.15, lambda_home)
    lambda_away = max(0.15, lambda_away)

    # P(victoria local) = sum over (i,j) con i > j de P(home=i)*P(away=j)
    prob_home = 0.0
    prob_draw = 0.0
    prob_away = 0.0
    max_goals = 10
    for i in range(max_goals):
        for j in range(max_goals):
            p = poisson.pmf(i, lambda_home) * poisson.pmf(j, lambda_away)
            if i > j:
                prob_home += p
            elif i < j:
                prob_away += p
            else:
                prob_draw += p

    # BTTS: 1 - P(home=0) - P(away=0) + P(0,0)
    p_h0 = poisson.pmf(0, lambda_home)
    p_a0 = poisson.pmf(0, lambda_away)
    prob_btts = 1.0 - p_h0 - p_a0 + p_h0 * p_a0

    # Over 2.5: P(total goals > 2.5)
    prob_over25 = 0.0
    for i in range(max_goals):
        for j in range(max_goals):
            if i + j > 2.5:
                prob_over25 += poisson.pmf(i, lambda_home) * poisson.pmf(j, lambda_away)
    prob_under25 = 1.0 - prob_over25

    # Clean sheet: P(opponent scores 0)
    clean_sheet_home = poisson.pmf(0, lambda_away)  # local no encaja
    clean_sheet_away = poisson.pmf(0, lambda_home)

    expected_goals = lambda_home + lambda_away

    value_home = value_draw = value_away = False
    implied_home = implied_draw = implied_away = None
    if bookmaker_odds:
        from odds import odds_to_implied_probs
        h = bookmaker_odds.get("home_win")
        d = bookmaker_odds.get("draw")
        a = bookmaker_odds.get("away_win")
        if h and d and a:
            imp1, impx, imp2 = odds_to_implied_probs(float(h), float(d), float(a))
            implied_home, implied_draw, implied_away = imp1, impx, imp2
            w = blend_weight_poisson
            prob_home = w * prob_home + (1 - w) * imp1
            prob_draw = w * prob_draw + (1 - w) * impx
            prob_away = w * prob_away + (1 - w) * imp2
            # Value: probabilidad final mayor que implícita + margen 2%
            value_home = prob_home > imp1 + 0.02
            value_draw = prob_draw > impx + 0.02
            value_away = prob_away > imp2 + 0.02

    out: dict[str, Any] = {
        "prob_home_win": round(prob_home, 4),
        "prob_draw": round(prob_draw, 4),
        "prob_away_win": round(prob_away, 4),
        "prob_btts": round(prob_btts, 4),
        "prob_over25": round(prob_over25, 4),
        "prob_under25": round(prob_under25, 4),
        "expected_goals": round(expected_goals, 2),
        "clean_sheet_home": round(clean_sheet_home, 4),
        "clean_sheet_away": round(clean_sheet_away, 4),
        "lambda_home": round(lambda_home, 3),
        "lambda_away": round(lambda_away, 3),
    }
    if implied_home is not None:
        out["implied_home"] = implied_home
        out["implied_draw"] = implied_draw
        out["implied_away"] = implied_away
        out["value_home"] = value_home
        out["value_draw"] = value_draw
        out["value_away"] = value_away
    return out


def _injury_impact(injuries: list[dict], team_id: int) -> float:
    """Estima reducción de lambda (0-0.25) por lesiones de ese equipo."""
    team_injuries = [i for i in injuries if i.get("team_id") == team_id]
    if not team_injuries:
        return 0.0
    total = sum(i.get("contribution_estimate", 0.15) for i in team_injuries)
    return min(0.25, total)


def _analyze_one_match(
    m: dict,
    default_league_code: str,
    use_mock: bool,
) -> Optional[dict]:
    """
    Analiza un partido: stats, H2H, lesiones, datos externos, odds, probabilidades.
    Retorna dict con fixture_id, home, away, date, league, league_code, **probs y opcional external_data,
    o None si faltan home_team_id/away_team_id.
    """
    home_id = m.get("home_team_id")
    away_id = m.get("away_team_id")
    lcode = m.get("league_id") or default_league_code
    if not home_id or not away_id:
        return None
    home_stats = get_team_stats(home_id, lcode, use_mock=use_mock)
    away_stats = get_team_stats(away_id, lcode, use_mock=use_mock)
    fixture_id = m.get("fixture_id") or 0
    h2h = get_h2h(fixture_id, limit=5, use_mock=use_mock) if fixture_id else []
    injuries = get_injuries(fixture_id, use_mock=use_mock)
    ih = _injury_impact(injuries, home_id)
    ia = _injury_impact(injuries, away_id)

    # Historial rolling: forma, H2H y promedios para enriquecer Poisson
    lambda_bias = 1.0
    if _HAS_HISTORICAL and not use_mock and lcode:
        try:
            hist = enrich_match_stats_from_history(
                home_id=home_id, away_id=away_id,
                home_name=m.get("home_team"), away_name=m.get("away_team"),
                league_id=lcode, form_n=10, h2h_n=8, seasons_avg=3,
            )
            lambda_bias = hist.get("lambda_bias") or 1.0
            # Mezclar promedios con historial si hay suficientes partidos
            ah = hist.get("avg_goals_home") or {}
            aa = hist.get("avg_goals_away") or {}
            if ah.get("matches_count", 0) >= 3:
                home_stats["goals_avg_for"] = 0.6 * (home_stats.get("goals_avg_for") or 1.2) + 0.4 * ah.get("goals_for_avg", 1.2)
                home_stats["goals_avg_against"] = 0.6 * (home_stats.get("goals_avg_against") or 1.2) + 0.4 * ah.get("goals_against_avg", 1.2)
            if aa.get("matches_count", 0) >= 3:
                away_stats["goals_avg_for"] = 0.6 * (away_stats.get("goals_avg_for") or 1.1) + 0.4 * aa.get("goals_for_avg", 1.1)
                away_stats["goals_avg_against"] = 0.6 * (away_stats.get("goals_avg_against") or 1.2) + 0.4 * aa.get("goals_against_avg", 1.2)
            if len(hist.get("h2h") or []) >= 2:
                h2h = hist["h2h"]
        except Exception as e:
            logger.debug("analyzer: historial rolling no usado para este partido: %s", e)

    external_data = None
    if _HAS_INTEGRATOR and not use_mock:
        match_ctx = {
            "home_team": m.get("home_team"), "away_team": m.get("away_team"),
            "date": m.get("datetime") or m.get("date"), "league_code": lcode,
            "home_team_id": home_id, "away_team_id": away_id,
        }
        ext = get_external_data(match_ctx)
        if ext:
            external_data = {k: v for k, v in ext.items() if k in ("xG_home", "xG_away", "injuries")}
            if ext.get("injury_impact_home") is not None:
                ih = max(ih, float(ext["injury_impact_home"]))
            if ext.get("injury_impact_away") is not None:
                ia = max(ia, float(ext["injury_impact_away"]))
    bookmaker_odds = get_match_odds(
        fixture_id, use_mock=use_mock,
        home_team=m.get("home_team"), away_team=m.get("away_team"),
        match_date=m.get("datetime") or m.get("date"), league_code=lcode,
    )
    probs = calculate_probabilities(
        home_stats, away_stats, h2h, injuries, ih, ia,
        bookmaker_odds=bookmaker_odds, external_data=external_data, lambda_bias=lambda_bias,
    )
    row = {
        "fixture_id": fixture_id,
        "home": m.get("home_team"),
        "away": m.get("away_team"),
        "date": m.get("date") or m.get("datetime") or "",
        "league": m.get("league_name"),
        "league_code": lcode,
        "home_team_id": home_id,
        "away_team_id": away_id,
        **probs,
    }
    if external_data:
        row["external_data"] = external_data
    return row


def analyze_specific_league(
    league_code: str,
    date_filter: str = "today",
    use_mock: bool = False,
    days_ahead: int = 7,
) -> tuple[list[dict], pd.DataFrame]:
    """
    Barre partidos del día (o todos próximos si date_filter != 'today'),
    calcula probs para cada uno, y retorna lista de dicts + DataFrame.
    league_code: código de competición (PL, PD, EL1, etc.).
    days_ahead: cuántos días hacia adelante buscar partidos (por defecto 7).
    """
    from datetime import datetime

    logger.info("analyzer: analyze_specific_league league_code=%s date_filter=%s use_mock=%s days_ahead=%s", league_code, date_filter, use_mock, days_ahead)
    try:
        matches = get_upcoming_matches([league_code], days_ahead=days_ahead, use_mock=use_mock)
    except Exception as e:
        logger.exception("analyzer: get_upcoming_matches falló para %s: %s", league_code, e)
        raise
    logger.info("analyzer: get_upcoming_matches devolvió %d partidos", len(matches))

    today = datetime.now().date()
    if date_filter == "today":
        matches = [m for m in matches if m.get("datetime") and m["datetime"].date() == today]
        logger.info("analyzer: tras filtro 'today' quedan %d partidos", len(matches))
    if not matches:
        logger.warning("analyzer: sin partidos para league_code=%s (date_filter=%s)", league_code, date_filter)
        return [], pd.DataFrame()

    rows = []
    for i, m in enumerate(matches):
        try:
            row = _analyze_one_match(m, league_code, use_mock)
            if row is not None:
                rows.append(row)
        except Exception as e:
            logger.exception("analyzer: _analyze_one_match falló partido %d/%d %s vs %s: %s",
                             i + 1, len(matches), m.get("home_team"), m.get("away_team"), e)
            raise
    df = pd.DataFrame(rows)
    logger.info("analyzer: analyze_specific_league listo, %d filas", len(rows))
    return rows, df


def analyze_matches(
    matches: list,
    use_mock: bool = False,
) -> list:
    """
    Dado una lista de partidos (dicts con home_team_id, away_team_id, league_id, etc.),
    calcula probabilidades Poisson para cada uno usando football-data.org (stats, H2H, lesiones).
    Retorna lista de dicts con home, away, date, league, fixture_id y todas las probs.
    """
    rows = []
    for m in matches:
        row = _analyze_one_match(m, "", use_mock)
        if row is not None:
            rows.append(row)
    return rows


def top_teams_avg_goals(
    league_code: str,
    last_journeys: int = 10,
    min_avg: float = 1.0,
    use_mock: bool = False,
) -> pd.DataFrame:
    """
    Top equipos por promedio de goles anotados en últimos N partidos.
    Retorna top 3 (o más) que promedian > min_avg goles.
    """
    logger.info("analyzer: top_teams_avg_goals league_code=%s last_journeys=%s min_avg=%s use_mock=%s",
                league_code, last_journeys, min_avg, use_mock)
    standings = get_standings(league_code, use_mock=use_mock)
    if not standings:
        logger.warning("analyzer: top_teams_avg_goals sin clasificación para %s (API key o liga incorrecta)", league_code)
        return pd.DataFrame()

    team_goals: list[dict] = []
    for s in standings[:20]:  # limitar equipos para no gastar requests
        team_id = s.get("team_id")
        team_name = s.get("team_name")
        if not team_id:
            continue
        form = None
        if _HAS_HISTORICAL and not use_mock:
            try:
                from historical_analyzer import get_recent_form
                form = get_recent_form(team_id=team_id, team_name=team_name, league_id=league_code, last_n=last_journeys, use_master_checked=True)
                if not form:
                    form = get_recent_form(team_id=team_id, team_name=team_name, league_id=league_code, last_n=last_journeys, use_master_checked=False)
            except Exception as e:
                logger.debug("analyzer: top_teams_avg_goals historical form: %s", e)
        if not form:
            form = get_form_last_n(team_id, n=last_journeys, league_code=league_code, use_mock=use_mock)
        if not form:
            logger.debug("analyzer: equipo %s (%s) sin partidos en forma", team_id, team_name)
            continue
        avg_goals = sum(f.get("goals_for", 0) for f in form) / len(form)
        if avg_goals >= min_avg:
            team_goals.append({"team": team_name, "team_id": team_id, "avg_goals": round(avg_goals, 2), "matches": len(form)})

    if not team_goals:
        logger.warning("analyzer: top_teams_avg_goals ningún equipo con avg>=%s (form vacío para todos?)", min_avg)
    team_goals.sort(key=lambda x: x["avg_goals"], reverse=True)
    top3 = team_goals[:10]  # top 10 para tabla
    logger.info("analyzer: top_teams_avg_goals -> %d equipos", len(top3))
    return pd.DataFrame(top3)


def run_query(
    query: str,
    use_mock: bool = False,
) -> Union[pd.DataFrame, List[dict], str]:
    """
    Parser simple de queries en lenguaje natural (español).
    Ejemplos:
    - "análisis League One hoy"
    - "top 3 equipos league one >1 gol últimas 10"
    - "partidos con >70% prob de ganar"
    - "clean sheet alta probabilidad"
    """
    q = query.strip().lower()
    # Resolver liga por código (football-data.org)
    league_code = get_league_id("league one") or "EL1"  # default
    for name, code in [
        ("league one", "EL1"),
        ("premier", "PL"),
        ("laliga", "PD"),
        ("la liga", "PD"),
        ("serie a", "SA"),
        ("bundesliga", "BL1"),
        ("ligue 1", "FL1"),
    ]:
        if name in q:
            league_code = code
            break

    # "análisis ... hoy" -> partidos del día con probs
    if "análisis" in q or "analisis" in q or "hoy" in q:
        _, df = analyze_specific_league(league_code, date_filter="today", use_mock=use_mock)
        if df.empty:
            return "No hay partidos hoy para esa liga."
        return df

    # "top 3 equipos" / ">1 gol" / "últimas 10"
    if "top" in q and ("equipo" in q or "gol" in q):
        n_journeys = 10
        if "últimas" in q or "ultimas" in q:
            import re
            r = re.search(r"últimas\s*(\d+)|ultimas\s*(\d+)", q)
            if r:
                n_journeys = int(r.group(1) or r.group(2) or 10)
        min_gol = 1.0
        if ">1" in q or "> 1" in q or "más de 1" in q:
            min_gol = 1.0
        return top_teams_avg_goals(league_code, last_journeys=n_journeys, min_avg=min_gol, use_mock=use_mock)

    # "partidos con >70% prob" / "70% win"
    if "70" in q or ("prob" in q and "ganar" in q):
        _, df = analyze_specific_league(league_code, date_filter="", use_mock=use_mock)
        if df.empty:
            return "No hay partidos próximos."
        df_high = pd.concat([
            df[df["prob_home_win"] >= 0.70],
            df[df["prob_away_win"] >= 0.70],
        ], ignore_index=True).drop_duplicates()
        return df_high if not df_high.empty else df

    # "clean sheet" alta probabilidad
    if "clean" in q or "clean sheet" in q:
        _, df = analyze_specific_league(league_code, date_filter="", use_mock=use_mock)
        if df.empty:
            return "No hay partidos."
        df = df.assign(max_cs=df[["clean_sheet_home", "clean_sheet_away"]].max(axis=1))
        df = df[df["max_cs"] >= 0.40].sort_values("max_cs", ascending=False)
        return df

    # Por defecto: análisis de la liga (próximos partidos)
    _, df = analyze_specific_league(league_code, date_filter="", use_mock=use_mock)
    return df if not df.empty else "Sin resultados para la query."
