"""
Ventana deslizante: mantiene últimas 4 temporadas completas + temporada actual (keep_seasons=5).
Elimina registros con season anterior a (actual - 5). Modo --dry-run para ver qué se borraría.
"""

import logging
from datetime import date
from typing import List, Tuple

from db import (
    count_historical_matches_before_season,
    delete_historical_matches_with_season_before,
    get_historical_match_seasons,
    get_historical_match_seasons_before,
)

logger = logging.getLogger(__name__)


def _current_season_year() -> int:
    """Año de la temporada actual (europea: jul-jun)."""
    today = date.today()
    return today.year if today.month >= 7 else today.year - 1


def maintain_rolling_window(
    keep_seasons: int = 5,
    dry_run: bool = False,
) -> Tuple[int, dict]:
    """
    Elimina de historical_matches los partidos con season < (actual - keep_seasons).
    keep_seasons=5 → mantiene actual, actual-1, actual-2, actual-3, actual-4 (4 completas + actual).
    dry_run=True: no borra; devuelve (0, report) con lo que se eliminaría.
    Devuelve (número eliminados, report_dict con would_delete, by_season, cutoff).
    """
    current = _current_season_year()
    cutoff = current - keep_seasons
    report = {
        "current_season": current,
        "cutoff_season": cutoff,
        "would_delete": 0,
        "by_season": [],
    }
    if dry_run:
        report["would_delete"] = count_historical_matches_before_season(cutoff)
        report["by_season"] = [
            {"season": s, "count": n} for s, n in get_historical_match_seasons_before(cutoff)
        ]
        logger.info(
            "rolling_window [DRY-RUN]: se eliminarían %d partidos (season < %d). Por temporada: %s",
            report["would_delete"], cutoff, report["by_season"],
        )
        return 0, report
    deleted = delete_historical_matches_with_season_before(cutoff)
    if deleted:
        logger.info("rolling_window: eliminados %d partidos con season < %d", deleted, cutoff)
    report["would_delete"] = deleted
    return deleted, report


def get_current_window_seasons() -> List[int]:
    """Lista de temporadas que se mantienen en la ventana (keep_seasons=5)."""
    current = _current_season_year()
    return [current - 4, current - 3, current - 2, current - 1, current]


def report_window_status() -> dict:
    """Estado de la ventana: temporadas en BD y cutoff aplicado."""
    in_db = get_historical_match_seasons()
    allowed = get_current_window_seasons()
    current = _current_season_year()
    cutoff = current - 5
    return {
        "current_season": current,
        "cutoff_season": cutoff,
        "allowed_seasons": allowed,
        "seasons_in_db": in_db,
    }
