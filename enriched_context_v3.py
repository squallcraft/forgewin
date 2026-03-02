"""
Contexto enriquecido para V3: Índice de Asedio, Alerta de Fricción, Dinámica HT/FT.
Usa historical_matches (shots_target, corners, fouls, referee, hthg, htag, htr).
"""

import logging
from typing import Any, Dict, List, Optional

from db import (
    get_historical_matches_for_team_with_stats,
    get_referee_avg_cards,
)

log = logging.getLogger(__name__)


def _safe_float(val: Any) -> Optional[float]:
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _avg(values: List[Optional[float]]) -> Optional[float]:
    usable = [v for v in values if v is not None]
    if not usable:
        return None
    return round(sum(usable) / len(usable), 2)


def build_asedio_text(
    home_id: Optional[int],
    away_id: Optional[int],
    home_name: str,
    away_name: str,
    league_id: Optional[str],
    last_n: int = 5,
) -> str:
    """
    Índice de Asedio: tiros a puerta y córners por equipo (últimos N partidos).
    Si no hay datos, devuelve cadena vacía.
    """
    parts = []
    for label, team_id, team_name in [("Local", home_id, home_name), ("Visitante", away_id, away_name)]:
        rows = get_historical_matches_for_team_with_stats(
            team_id=team_id,
            team_name=team_name if not team_id else None,
            league_id=league_id or "",
            last_n=last_n,
        )
        if not rows:
            continue
        shots = [_safe_float(r.get("shots_target_team")) for r in rows]
        corners = [_safe_float(r.get("corners_team")) for r in rows]
        avg_s = _avg(shots)
        avg_c = _avg(corners)
        if avg_s is not None or avg_c is not None:
            s_str = f"{avg_s:.1f} tiros a puerta" if avg_s is not None else ""
            c_str = f"{avg_c:.1f} córners" if avg_c is not None else ""
            parts.append(f"  {label} ({team_name}): {', '.join(x for x in [s_str, c_str] if x)} (media últimos {len(rows)} partidos).")
    if not parts:
        return ""
    return "**Índice de Asedio (presión real):**\n" + "\n".join(parts)


def build_friccion_text(
    referee_name: Optional[str],
    league_id: Optional[str],
    home_id: Optional[int],
    away_id: Optional[int],
    home_name: str,
    away_name: str,
    last_n_team: int = 5,
) -> str:
    """
    Alerta de Fricción: árbitro (promedio tarjetas) + equipos (promedio faltas).
    Si no hay árbitro conocido, solo se pueden mostrar promedios de faltas por equipo.
    """
    parts = []
    ref_stats = get_referee_avg_cards(referee_name, league_id, last_n=30) if referee_name and referee_name.strip() else None
    if ref_stats:
        parts.append(
            f"  Árbitro: {referee_name} — {ref_stats['avg_yellow']:.1f} amarillas/partido, {ref_stats['avg_red']:.1f} rojas/partido "
            f"(últimos {ref_stats['matches']} partidos en la liga)."
        )
    for label, team_id, team_name in [("Local", home_id, home_name), ("Visitante", away_id, away_name)]:
        rows = get_historical_matches_for_team_with_stats(
            team_id=team_id,
            team_name=team_name if not team_id else None,
            league_id=league_id or "",
            last_n=last_n_team,
        )
        if not rows:
            continue
        fouls = [_safe_float(r.get("fouls_team")) for r in rows]
        avg_f = _avg(fouls)
        if avg_f is not None:
            parts.append(f"  Faltas cometidas ({label}, {team_name}): {avg_f:.1f} por partido (últimos {len(rows)}).")
    if not parts:
        return ""
    return "**Alerta de Fricción (árbitro y disciplina):**\n" + "\n".join(parts)


def build_ht_ft_text(
    home_id: Optional[int],
    away_id: Optional[int],
    home_name: str,
    away_name: str,
    league_id: Optional[str],
    last_n: int = 5,
) -> str:
    """
    Dinámica de tiempos: resultado al descanso (H/D/A) y goles al descanso por equipo.
    """
    parts = []
    for label, team_id, team_name in [("Local", home_id, home_name), ("Visitante", away_id, away_name)]:
        rows = get_historical_matches_for_team_with_stats(
            team_id=team_id,
            team_name=team_name if not team_id else None,
            league_id=league_id or "",
            last_n=last_n,
        )
        if not rows:
            continue
        htr_list = [r.get("htr") for r in rows if r.get("htr")]
        goals_ht = [r.get("goals_at_ht_team") for r in rows if r.get("goals_at_ht_team") is not None]
        goals_ht = [_safe_float(g) for g in goals_ht]
        if htr_list:
            n_h = sum(1 for h in htr_list if str(h).upper() == "H")
            n_d = sum(1 for h in htr_list if str(h).upper() == "D")
            n_a = sum(1 for h in htr_list if str(h).upper() == "A")
            total = len(htr_list)
            pct_h = round(100 * n_h / total, 0) if total else 0
            pct_d = round(100 * n_d / total, 0) if total else 0
            pct_a = round(100 * n_a / total, 0) if total else 0
            parts.append(
                f"  {label} ({team_name}): al descanso gana {pct_h:.0f}% / empate {pct_d:.0f}% / pierde {pct_a:.0f}% "
                f"(últimos {total} con dato)."
            )
        if goals_ht:
            avg_gh = _avg(goals_ht)
            if avg_gh is not None:
                parts.append(f"  Goles al descanso ({label}): {avg_gh:.2f} por partido (últimos {len(goals_ht)}).")
    if not parts:
        return ""
    return "**Dinámica HT/FT (primer tiempo y segundo tiempo):**\n" + "\n".join(parts)


def build_enriched_context_for_matches(match_data_list: List[Dict[str, Any]]) -> str:
    """
    Para cada partido en match_data_list, construye un bloque de texto con:
    - Índice de Asedio (tiros a puerta, córners)
    - Alerta de Fricción (árbitro + faltas; si no hay árbitro para el partido, se omite o se indica)
    - Dinámica HT/FT (resultado al descanso, goles al descanso)

    match_data_list: lista de dicts con home_team_id, away_team_id, home_team, away_team, league_code, referee (opcional).
    referee puede no estar disponible hasta que se confirme el partido.
    """
    if not match_data_list:
        return ""
    blocks = []
    for m in match_data_list:
        home = m.get("home_team") or m.get("home") or "Local"
        away = m.get("away_team") or m.get("away") or "Visitante"
        home_id = m.get("home_team_id")
        away_id = m.get("away_team_id")
        lcode = m.get("league_code") or m.get("league_id") or ""
        referee = m.get("referee")
        parts_match = [f"**{home} vs {away}**"]
        asedio = build_asedio_text(home_id, away_id, home, away, lcode, last_n=5)
        if asedio:
            parts_match.append(asedio)
        friccion = build_friccion_text(referee, lcode, home_id, away_id, home, away, last_n_team=5)
        if friccion:
            parts_match.append(friccion)
        elif referee:
            parts_match.append(f"**Árbitro:** {referee} (sin estadísticas de tarjetas en BD para este árbitro/liga).")
        ht_ft = build_ht_ft_text(home_id, away_id, home, away, lcode, last_n=5)
        if ht_ft:
            parts_match.append(ht_ft)
        if len(parts_match) > 1:
            blocks.append("\n".join(parts_match))
    if not blocks:
        return ""
    return "\n\n---\n\n".join(blocks)
