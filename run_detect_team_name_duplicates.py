#!/usr/bin/env python3
"""
Detecta partidos duplicados por variante de nombre de equipo e inserta candidatos
en pending_team_aliases para que el admin resuelva (control humano).

Se ejecuta una vez para homologar el parque actual; luego ocasionalmente si hay casos nuevos.
Uso: python run_detect_team_name_duplicates.py
"""

import logging
import sys
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def _date_str(m: dict) -> str:
    """Extrae YYYY-MM-DD del partido."""
    utc = m.get("match_date_utc") or m.get("date") or ""
    s = str(utc).strip()
    if not s:
        return ""
    return s[:10] if len(s) >= 10 else s


def _same_match(m1: dict, m2: dict) -> bool:
    """True si son el mismo partido: mismo fixture_id o misma fecha+liga y equipos equivalentes."""
    if m1.get("fixture_id") and m2.get("fixture_id") and m1["fixture_id"] == m2["fixture_id"]:
        return True
    if _date_str(m1) != _date_str(m2):
        return False
    if (m1.get("league_code") or m1.get("league_id")) != (m2.get("league_code") or m2.get("league_id")):
        return False
    h1 = (m1.get("home_team") or "").strip()
    h2 = (m2.get("home_team") or "").strip()
    a1 = (m1.get("away_team") or "").strip()
    a2 = (m2.get("away_team") or "").strip()
    if not h1 or not h2 or not a1 or not a2:
        return False
    # Uno contiene al otro (ej. "Mainz" en "1. FSV Mainz 05") o iguales ignorando case
    def _equiv(x: str, y: str) -> bool:
        x, y = x.lower(), y.lower()
        return x == y or (x in y) or (y in x)
    return _equiv(h1, h2) and _equiv(a1, a2)


def _collect_candidates(matches: list) -> list:
    """Agrupa por (date, league) y devuelve [(league_id, name1, name2), ...] sin duplicar."""
    key_to_matches = defaultdict(list)
    for m in matches:
        dt = _date_str(m)
        lc = m.get("league_code") or m.get("league_id") or ""
        if not dt or not lc:
            continue
        key_to_matches[(dt, lc)].append(m)
    candidates = []
    seen_pairs = set()
    for (dt, lc), group in key_to_matches.items():
        for i in range(len(group)):
            for j in range(i + 1, len(group)):
                m1, m2 = group[i], group[j]
                if not _same_match(m1, m2):
                    continue
                h1 = (m1.get("home_team") or "").strip()
                h2 = (m2.get("home_team") or "").strip()
                a1 = (m1.get("away_team") or "").strip()
                a2 = (m2.get("away_team") or "").strip()
                if h1 != h2:
                    pair = (lc, min(h1, h2), max(h1, h2)) if h1 and h2 else None
                    if pair and pair not in seen_pairs:
                        seen_pairs.add(pair)
                        candidates.append(pair)
                if a1 != a2:
                    pair = (lc, min(a1, a2), max(a1, a2)) if a1 and a2 else None
                    if pair and pair not in seen_pairs:
                        seen_pairs.add(pair)
                        candidates.append(pair)
    return candidates


def main() -> int:
    from config import TOP_10_LEAGUE_CODES
    from db import (
        get_connection,
        init_db,
        insert_pending_team_alias_if_new,
    )
    init_db()
    today = date.today()
    start = (today - timedelta(days=1)).isoformat() + "T00:00:00"
    end = (today + timedelta(days=14)).isoformat() + "T23:59:59"
    placeholders = ",".join("?" * len(TOP_10_LEAGUE_CODES))
    with get_connection() as conn:
        c = conn.cursor()
        c.execute(
            f"""SELECT * FROM matches
                WHERE (match_date_utc >= ? AND match_date_utc <= ?) AND league_code IN ({placeholders})
                ORDER BY match_date_utc""",
            [start, end] + list(TOP_10_LEAGUE_CODES),
        )
        rows = c.fetchall()
    matches = [dict(r) for r in rows]
    logger.info("Partidos leídos (último día + 14): %d", len(matches))
    candidates = _collect_candidates(matches)
    logger.info("Candidatos a alias (mismo partido, dos nombres): %d", len(candidates))
    inserted = 0
    for league_id, n1, n2 in candidates:
        if insert_pending_team_alias_if_new(league_id, n1, n2):
            inserted += 1
            logger.info("  + %s: «%s» / «%s»", league_id, n1, n2)
    logger.info("Insertados en pending_team_aliases: %d (el resto ya existían).", inserted)
    logger.info("Resuelve en la app: Administración → Verificación de datos → Nombres de equipos a normalizar.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
