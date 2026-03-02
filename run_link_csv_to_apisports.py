#!/usr/bin/env python3
"""
Vinculador CSV → API-Sports para historical_matches.

Problema: registros cargados desde CSV (fixture_id 800M+hash) no tienen
api_sports_fixture_id ni home/away_team_id, por lo que no se pueden backfill
con estadísticas, árbitro, tarjetas ni xG.

Solución: para cada liga/temporada, descarga TODOS los fixtures finalizados
de API-Sports (1 sola llamada por temporada), luego cruza cada fixture contra
los registros CSV de la BD usando fecha + nombres normalizados. Si hay match,
actualiza api_sports_fixture_id + team_ids en el registro CSV existente.

Estrategia de matching:
  1. Exacto: fecha ISO + nombre normalizado home + nombre normalizado away.
  2. Fuzzy: misma fecha, palabras largas clave del nombre en común
     (ej. "Manchester City" vs "Man. City").

Uso:
  python run_link_csv_to_apisports.py                      # todas las ligas, 2005→hoy
  python run_link_csv_to_apisports.py --leagues PL SA      # solo esas ligas
  python run_link_csv_to_apisports.py --season-from 2015   # desde 2015
  python run_link_csv_to_apisports.py --dry-run            # solo reporta, no modifica
  python run_link_csv_to_apisports.py --summary            # estado actual de la BD
"""

import argparse
import logging
import sys
import time
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("linker")

# ── Constantes ──────────────────────────────────────────────────────────────
DOMESTIC_LEAGUES = ["PL", "SA", "PD", "FL1", "BL1", "DED", "PPL", "ELC", "EL1"]

# API-Sports tiene datos desde aprox. 2005 para la mayoría de ligas domésticas
DEFAULT_SEASON_FROM = 2005


# ── Normalización de nombres ─────────────────────────────────────────────────
def _normalize(name: Optional[str]) -> str:
    """Normaliza nombre para comparación: minúsculas, unicode→ascii, guiones→espacios."""
    if not name:
        return ""
    s = name.strip().lower()
    for old, new in [
        ("á","a"),("é","e"),("í","i"),("ó","o"),("ú","u"),("ü","u"),("ñ","n"),
        ("ø","o"),("œ","oe"),("ğ","g"),("ı","i"),("ł","l"),("ń","n"),
        ("ş","s"),("ţ","t"),("ž","z"),("ć","c"),("ę","e"),("ą","a"),
        ("â","a"),("ê","e"),("î","i"),("ô","o"),("û","u"),("à","a"),
        ("è","e"),("ì","i"),("ò","o"),("ù","u"),("ä","a"),("ö","o"),
    ]:
        s = s.replace(old, new)
    s = s.replace("-", " ").replace(".", " ").replace("'", " ").replace("'", " ")
    return " ".join(s.split())


_SKIP_WORDS = {
    "fc","fk","cf","ac","as","sc","bc","sk","sv","vfb","vfl","tsg","rb",
    "de","del","los","las","el","la","le","les","1","0","2","3",
    "club","calcio","football","soccer",
}

# Expansiones de abreviaturas comunes en CSV de football-data.co.uk
_ABBREVIATIONS = {
    "man city": "manchester city",
    "man united": "manchester united",
    "man utd": "manchester united",
    "nott'm forest": "nottingham forest",
    "nottm forest": "nottingham forest",
    "wolves": "wolverhampton",
    "spurs": "tottenham",
    "qpr": "queens park rangers",
    "wba": "west bromwich albion",
    "west brom": "west bromwich",
    "sheff utd": "sheffield united",
    "sheff wed": "sheffield wednesday",
    "blackburn": "blackburn rovers",
    "brighton": "brighton hove",
    "palace": "crystal palace",
    "leicester": "leicester city",
    "norwich": "norwich city",
    "stoke": "stoke city",
    "hull": "hull city",
    "cardiff": "cardiff city",
    "swansea": "swansea city",
    "ipswich": "ipswich town",
    "luton": "luton town",
    "coventry": "coventry city",
    "birmingham": "birmingham city",
    "porto": "fc porto",
    "sporting cp": "sporting",
    "atletico": "atletico madrid",
    "inter": "inter milan",
    "paris sg": "paris saint germain",
    "paris saint-germain": "paris saint germain",
    # Bundesliga específico
    "bayern munich": "bayern munchen",
    "m gladbach": "monchengladbach",
    "mgladbach": "monchengladbach",
    "gladbach": "monchengladbach",
    "ein frankfurt": "eintracht frankfurt",
    "leverkusen": "bayer leverkusen",
    "dortmund": "borussia dortmund",
    "hannover": "hannover 96",
    "nurnberg": "1 fc nurnberg",
    "kaiserslautern": "1 fc kaiserslautern",
    # Eredivisie
    "ajax": "ajax amsterdam",
    "psv": "psv eindhoven",
    "feyenoord": "feyenoord rotterdam",
    # Ligue 1
    "paris sg": "paris saint germain",
    "marseille": "olympique marseille",
    "lyon": "olympique lyonnais",
    # La Liga
    "espanol": "espanyol",
    "vallecano": "rayo vallecano",
    # Championship
    "qpr": "queens park rangers",
    "sheff weds": "sheffield wednesday",
    "sheff wed": "sheffield wednesday",
}


def _expand(norm: str) -> str:
    """Expande abreviaturas comunes."""
    return _ABBREVIATIONS.get(norm, norm)


def _key_words(norm: str) -> List[str]:
    """Palabras significativas (no stop words, len>=3)."""
    return [w for w in norm.split() if w not in _SKIP_WORDS and len(w) >= 3]


def _names_match(a: str, b: str) -> bool:
    """True si dos nombres de equipo son el mismo tras normalizar y expandir."""
    na, nb = _expand(_normalize(a)), _expand(_normalize(b))
    if not na or not nb:
        return False
    if na == nb:
        return True
    # Contiene completo
    if na in nb or nb in na:
        return True
    # Todas las palabras significativas del nombre más corto aparecen en el más largo
    kwa = _key_words(na)
    kwb = _key_words(nb)
    if not kwa or not kwb:
        return False
    # Si el nombre más corto tiene pocas palabras, todas deben aparecer en el otro
    shorter, longer = (kwa, nb) if len(na) <= len(nb) else (kwb, na)
    if all(w in longer for w in shorter):
        return True
    # Palabra más larga de uno es prefijo o está en el otro
    longest_a = max(kwa, key=len)
    longest_b = max(kwb, key=len)
    if len(longest_a) >= 4 and longest_a in nb:
        return True
    if len(longest_b) >= 4 and longest_b in na:
        return True
    return False


def _match_pair(our_home: str, our_away: str, api_home: str, api_away: str) -> bool:
    return _names_match(our_home, api_home) and _names_match(our_away, api_away)


# ── Descarga desde API-Sports ────────────────────────────────────────────────
def _fetch_api_sports_season(
    league_code: str,
    season: int,
    delay: float,
) -> List[Dict[str, Any]]:
    """
    Descarga todos los fixtures FT de una liga/temporada desde API-Sports.
    Devuelve lista de dicts con: api_id, date, home_name, away_name, home_id, away_id, referee, attendance.
    UNA sola llamada a la API por temporada.
    """
    from api_sports_fetcher import _api_get, LEAGUE_CODE_TO_ID

    league_id = LEAGUE_CODE_TO_ID.get(league_code)
    if not league_id:
        return []

    # ForgeWin usa año-fin (season=2024 = temporada 2023/24).
    # API-Sports usa año-inicio (season=2023 = temporada 2023/24).
    api_season = season - 1
    time.sleep(delay)
    resp = _api_get("fixtures", params={
        "league": league_id,
        "season": api_season,
        "status": "FT-AET-PEN",
    })
    if not resp or not resp.get("response"):
        return []

    out = []
    for ev in resp["response"]:
        fix = ev.get("fixture") or {}
        status_short = (fix.get("status") or {}).get("short", "")
        if status_short not in ("FT", "AET", "PEN"):
            continue
        api_id = fix.get("id")
        if not api_id:
            continue
        date_str = (fix.get("date") or "")[:10]
        if len(date_str) < 10:
            continue
        teams = ev.get("teams") or {}
        home = teams.get("home") or {}
        away = teams.get("away") or {}
        ref = (fix.get("referee") or "").strip() or None
        raw_att = fix.get("attendance")
        try:
            att = int(raw_att) if raw_att is not None else None
        except (TypeError, ValueError):
            att = None
        out.append({
            "api_id": int(api_id),
            "date": date_str,
            "home_name": (home.get("name") or "").strip(),
            "away_name": (away.get("name") or "").strip(),
            "home_id": home.get("id"),
            "away_id": away.get("id"),
            "referee": ref,
            "attendance": att,
        })
    return out


# ── Carga de registros CSV sin api_id ────────────────────────────────────────
def _load_unlinked(league_code: str, season: int) -> List[Dict[str, Any]]:
    """Registros de historical_matches sin api_sports_fixture_id para liga/temporada."""
    from db import get_connection
    with get_connection() as conn:
        rows = conn.execute("""
            SELECT fixture_id, date, home_team_name, away_team_name
            FROM historical_matches
            WHERE league_id = ?
              AND season = ?
              AND api_sports_fixture_id IS NULL
        """, (league_code, season)).fetchall()
    return [
        {"fixture_id": r[0], "date": r[1], "home": r[2] or "", "away": r[3] or ""}
        for r in rows
    ]


# ── Actualización en BD ──────────────────────────────────────────────────────
def _update_link(
    fixture_id: int,
    api_sports_fixture_id: int,
    home_team_id: Optional[int],
    away_team_id: Optional[int],
    referee: Optional[str],
    attendance: Optional[int],
) -> None:
    from db import get_connection
    with get_connection() as conn:
        conn.execute("""
            UPDATE historical_matches
            SET api_sports_fixture_id = ?,
                home_team_id = COALESCE(home_team_id, ?),
                away_team_id = COALESCE(away_team_id, ?),
                referee = CASE WHEN referee IS NULL OR referee = '' THEN ? ELSE referee END,
                attendance = COALESCE(attendance, ?)
            WHERE fixture_id = ?
        """, (
            api_sports_fixture_id,
            home_team_id, away_team_id,
            referee, attendance,
            fixture_id,
        ))


# ── Lógica principal de vinculación ─────────────────────────────────────────
def link_league_season(
    league_code: str,
    season: int,
    delay: float,
    dry_run: bool,
) -> Tuple[int, int, int]:
    """
    Vincula registros CSV de una liga/temporada con sus fixture_ids de API-Sports.
    Devuelve (linked, already_done, not_found).
    """
    # Registros CSV sin vincular
    unlinked = _load_unlinked(league_code, season)
    if not unlinked:
        return 0, 0, 0

    # Descarga de API-Sports (1 sola llamada)
    api_fixtures = _fetch_api_sports_season(league_code, season, delay)
    if not api_fixtures:
        logger.debug("  %s/%s: API-Sports sin datos", league_code, season)
        return 0, 0, len(unlinked)

    # Índice por fecha para búsqueda eficiente
    api_by_date: Dict[str, List[Dict]] = {}
    for f in api_fixtures:
        api_by_date.setdefault(f["date"], []).append(f)

    linked = 0
    not_found = 0

    for row in unlinked:
        date_str = (row["date"] or "")[:10]
        candidates = api_by_date.get(date_str, [])

        matched = None
        for cand in candidates:
            if _match_pair(row["home"], row["away"], cand["home_name"], cand["away_name"]):
                matched = cand
                break

        if matched:
            if not dry_run:
                _update_link(
                    fixture_id=row["fixture_id"],
                    api_sports_fixture_id=matched["api_id"],
                    home_team_id=matched["home_id"],
                    away_team_id=matched["away_id"],
                    referee=matched["referee"],
                    attendance=matched["attendance"],
                )
            linked += 1
        else:
            not_found += 1
            logger.debug(
                "  Sin match: %s %s vs %s (fecha=%s)",
                league_code, row["home"], row["away"], date_str,
            )

    return linked, 0, not_found


# ── Resumen del estado de la BD ──────────────────────────────────────────────
def print_summary() -> None:
    from db import get_connection
    with get_connection() as conn:
        rows = conn.execute("""
            SELECT league_id,
                   COUNT(*) as total,
                   SUM(CASE WHEN api_sports_fixture_id IS NOT NULL THEN 1 ELSE 0 END) as vinculados,
                   SUM(CASE WHEN api_sports_fixture_id IS NULL THEN 1 ELSE 0 END) as sin_vincular
            FROM historical_matches
            GROUP BY league_id ORDER BY total DESC
        """).fetchall()
    logger.info("=" * 65)
    logger.info("ESTADO DE VINCULACIÓN por liga:")
    logger.info(f"  {'Liga':<6} {'Total':>8} {'Vinc.':>8} {'Pend.':>8}  {'%':>6}")
    logger.info("  " + "─" * 55)
    for r in rows:
        pct = r[2] / r[1] * 100 if r[1] > 0 else 0
        logger.info(f"  {r[0]:<6} {r[1]:>8,} {r[2]:>8,} {r[3]:>8,}  {pct:>5.1f}%")
    logger.info("=" * 65)


# ── Main ─────────────────────────────────────────────────────────────────────
def main() -> int:
    parser = argparse.ArgumentParser(
        description="Vincula registros CSV con api_sports_fixture_id de API-Sports"
    )
    parser.add_argument("--leagues", nargs="*", default=None,
                        help=f"Ligas a procesar (default: todas). Ej: PL SA BL1")
    parser.add_argument("--season-from", type=int, default=DEFAULT_SEASON_FROM,
                        help=f"Primera temporada (default {DEFAULT_SEASON_FROM})")
    parser.add_argument("--season-to", type=int, default=None,
                        help="Última temporada (default: actual)")
    parser.add_argument("--delay", type=float, default=0.3,
                        help="Pausa entre llamadas a API-Sports (default 0.3s)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Solo reporta, no modifica la BD")
    parser.add_argument("--summary", action="store_true",
                        help="Muestra estado de vinculación y sale")
    args = parser.parse_args()

    from db import init_db
    init_db()

    if args.summary:
        print_summary()
        return 0

    current_year = date.today().year
    season_to = args.season_to or (current_year if date.today().month >= 7 else current_year)
    leagues = args.leagues or DOMESTIC_LEAGUES

    logger.info("Vinculador CSV→API-Sports")
    logger.info("  Ligas:      %s", ", ".join(leagues))
    logger.info("  Temporadas: %s → %s", args.season_from, season_to)
    logger.info("  Delay:      %.2f s/llamada", args.delay)
    logger.info("  Dry-run:    %s", args.dry_run)
    if args.dry_run:
        logger.info("  [DRY-RUN] No se modificará la BD")

    total_linked = 0
    total_not_found = 0
    grand_total_unlinked = 0

    for league_code in leagues:
        league_linked = 0
        league_not_found = 0

        for season in range(args.season_from, season_to + 1):
            linked, _, not_found = link_league_season(
                league_code, season, args.delay, args.dry_run
            )
            league_linked += linked
            league_not_found += not_found
            if linked > 0 or not_found > 0:
                logger.info(
                    "  %s/%s: vinculados=%d, sin_match=%d",
                    league_code, season, linked, not_found,
                )

        total_linked += league_linked
        total_not_found += league_not_found
        grand_total_unlinked += (league_linked + league_not_found)
        logger.info(
            "%s TOTAL: vinculados=%d, sin_match=%d",
            league_code, league_linked, league_not_found,
        )

    logger.info("=" * 60)
    logger.info("COMPLETADO")
    logger.info("  Vinculados:   %d", total_linked)
    logger.info("  Sin match:    %d (pre-API-Sports o nombre muy diferente)", total_not_found)
    logger.info("  Procesados:   %d", grand_total_unlinked)
    if not args.dry_run and total_linked > 0:
        logger.info("\nSiguiente paso: ejecutar backfills de estadísticas:")
        logger.info("  python run_backfill_statistics_from_apisports.py --continuous")
        logger.info("  python run_backfill_cards_from_apisports.py --continuous")
        logger.info("  python run_backfill_offsides_from_apisports.py --continuous  # stats sin offsides")
        logger.info("  python run_backfill_attendance_via_fixture_id.py --continuous  # attendance+referee (1 call/match)")
        logger.info("  python run_backfill_attendance_referee_from_apisports.py     # bulk por liga (alternativa)")
        logger.info("  python run_daily_update.py --summary-only  (para ver el resultado)")

    print_summary()
    return 0


if __name__ == "__main__":
    sys.exit(main())
