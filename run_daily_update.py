#!/usr/bin/env python3
"""
Script maestro de actualización diaria de historical_matches.

Pasos (en orden):
  1. Nuevos partidos finalizados (últimas 48h) desde API-Sports → historical_matches
  2. Backfill estadísticas (shots, corners, fouls, offsides) para registros nuevos
  3. Backfill tarjetas (yellow/red) para registros nuevos
  4. Backfill árbitro + asistencia para registros nuevos
  5. Backfill xG para registros nuevos (vía /fixtures?id=XXX)
  6. Fallback football-data.org para los que API-Sports devuelve stats vacías

Uso:
  python run_daily_update.py                  # ciclo completo
  python run_daily_update.py --dry-run        # solo reporta sin modificar
  python run_daily_update.py --days-back 7    # últimos N días (default 2)
  python run_daily_update.py --skip-stats     # solo nuevos partidos, sin backfills
  python run_daily_update.py --leagues CL EL  # solo ligas específicas
"""

import argparse
import logging
import sys
import time
from datetime import date, timedelta
from pathlib import Path
from typing import List, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent))

# Cargar variables de entorno ANTES de importar cualquier módulo del proyecto
from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("daily_update")


def step1_new_matches(date_from: str, date_to: str, leagues: Optional[List[str]], dry_run: bool) -> int:
    """Inserta partidos nuevos finalizados desde API-Sports."""
    from data_downloader import update_from_apisports
    if dry_run:
        logger.info("[DRY-RUN] Paso 1: update_from_apisports(%s, %s)", date_from, date_to)
        return 0
    n, err = update_from_apisports(date_from=date_from, date_to=date_to, league_codes=leagues)
    if err:
        logger.warning("Paso 1 error: %s (insertados: %d)", err, n)
    else:
        logger.info("Paso 1 OK: %d partidos insertados/actualizados", n)
    return n


def step2_backfill_statistics(delay: float, batch: int, dry_run: bool) -> int:
    """Backfill shots/corners/fouls para registros con api_sports_fixture_id sin estadísticas."""
    from api_sports_fetcher import get_fixture_statistics
    from db import get_historical_matches_pending_statistics_backfill, update_historical_statistics, get_historical_match_for_fallback

    pending = get_historical_matches_pending_statistics_backfill(limit=batch)
    if not pending:
        logger.info("Paso 2: sin partidos pendientes de estadísticas.")
        return 0
    if dry_run:
        logger.info("[DRY-RUN] Paso 2: %d partidos pendientes de estadísticas.", len(pending))
        return 0

    updated = 0
    for m in pending:
        fid = m["fixture_id"]
        api_fid = m["api_sports_fixture_id"]
        home_tid = m.get("home_team_id")
        away_tid = m.get("away_team_id")
        try:
            home_stats = get_fixture_statistics(api_fid, home_tid)
            time.sleep(delay)
            away_stats = get_fixture_statistics(api_fid, away_tid)
            time.sleep(delay)

            hs = (home_stats or {}).get("shots")
            hst = (home_stats or {}).get("shots_on_target")
            as_ = (away_stats or {}).get("shots")
            ast = (away_stats or {}).get("shots_on_target")
            hc = (home_stats or {}).get("corners")
            ac = (away_stats or {}).get("corners")
            hf = (home_stats or {}).get("fouls")
            af = (away_stats or {}).get("fouls")
            ho = (home_stats or {}).get("offsides")
            ao = (away_stats or {}).get("offsides")

            # Fallback a football-data.org si API-Sports no devuelve shots
            if (hst is None or hst == 0) and (ast is None or ast == 0):
                row = get_historical_match_for_fallback(fid)
                if row and row.get("league_id") and row.get("date"):
                    try:
                        from data_fetcher import get_match_statistics_football_data_org
                        fd = get_match_statistics_football_data_org(
                            row["league_id"], row["date"],
                            row.get("home_team_name") or "", row.get("away_team_name") or "",
                        )
                        if fd and not fd.get("_match_found_no_stats") and (fd.get("home_shots_target") or fd.get("away_shots_target")):
                            hs = fd.get("home_shots") or hs
                            as_ = fd.get("away_shots") or as_
                            hst = fd.get("home_shots_target")
                            ast = fd.get("away_shots_target")
                            hc = fd.get("home_corners") or hc
                            ac = fd.get("away_corners") or ac
                            hf = fd.get("home_fouls") or hf
                            af = fd.get("away_fouls") or af
                            ho = fd.get("home_offsides") or ho
                            ao = fd.get("away_offsides") or ao
                            logger.debug("Fixture %s: estadísticas via football-data.org (fallback)", fid)
                    except Exception as e:
                        logger.debug("Fallback FD stats fixture %s: %s", fid, e)

            if any(x is not None for x in (hs, hst, as_, ast, hc, ac, hf, af)):
                if update_historical_statistics(fid, hs, as_, hst, ast, hc, ac, hf, af, ho, ao):
                    updated += 1
        except Exception as e:
            logger.warning("Error stats fixture %s: %s", fid, e)

    logger.info("Paso 2 OK: %d/%d partidos con estadísticas actualizadas", updated, len(pending))
    return updated


def step3_backfill_cards(delay: float, batch: int, dry_run: bool) -> int:
    """Backfill tarjetas amarillas/rojas."""
    from db import get_historical_matches_pending_card_backfill

    pending = get_historical_matches_pending_card_backfill(limit=batch)
    if not pending:
        logger.info("Paso 3: sin partidos pendientes de tarjetas.")
        return 0
    if dry_run:
        logger.info("[DRY-RUN] Paso 3: %d partidos pendientes de tarjetas.", len(pending))
        return 0

    import subprocess
    cmd = [
        sys.executable,
        str(Path(__file__).resolve().parent / "run_backfill_cards_from_apisports.py"),
        "--batch-size", str(batch),
        "--delay", str(delay),
    ]
    r = subprocess.run(cmd, cwd=str(Path(__file__).resolve().parent), capture_output=False)
    if r.returncode != 0:
        logger.warning("Paso 3: backfill tarjetas terminó con código %d", r.returncode)
    else:
        logger.info("Paso 3 OK: backfill tarjetas completado")
    return 0


def step4_backfill_referee(delay: float, dry_run: bool) -> int:
    """Backfill árbitro y asistencia."""
    if dry_run:
        logger.info("[DRY-RUN] Paso 4: backfill árbitro/asistencia.")
        return 0
    import subprocess
    cmd = [
        sys.executable,
        str(Path(__file__).resolve().parent / "run_backfill_attendance_referee_from_apisports.py"),
        "--delay", str(delay),
    ]
    r = subprocess.run(cmd, cwd=str(Path(__file__).resolve().parent), capture_output=False)
    if r.returncode != 0:
        logger.warning("Paso 4: backfill árbitro terminó con código %d", r.returncode)
    else:
        logger.info("Paso 4 OK: backfill árbitro/asistencia completado")
    return 0


def step5_backfill_xg(delay: float, batch: int, dry_run: bool) -> int:
    """Backfill xG desde API-Sports /fixtures para partidos recientes sin xG."""
    from api_sports_fetcher import _api_get
    from db import get_connection

    with get_connection() as conn:
        rows = conn.execute("""
            SELECT fixture_id, api_sports_fixture_id, league_id, date
            FROM historical_matches
            WHERE api_sports_fixture_id IS NOT NULL
              AND home_xg IS NULL
              AND status = 'FT'
              AND date >= '2021-01-01'
            ORDER BY date DESC
            LIMIT ?
        """, (batch,)).fetchall()

    if not rows:
        logger.info("Paso 5: sin partidos pendientes de xG.")
        return 0
    if dry_run:
        logger.info("[DRY-RUN] Paso 5: %d partidos pendientes de xG.", len(rows))
        return 0

    updated = 0
    for row in rows:
        fid, api_fid, league_id, date_str = row
        try:
            resp = _api_get("fixtures", params={"id": api_fid})
            time.sleep(delay)
            if not resp or not resp.get("response"):
                continue
            ev = (resp["response"] or [None])[0]
            if not ev:
                continue
            stats_list = ev.get("statistics") or []
            home_xg = away_xg = None
            for idx, team_stats in enumerate(stats_list[:2]):
                for stat in (team_stats.get("statistics") or []):
                    if (stat.get("type") or "").lower() in ("expected_goals", "xg", "expected goals"):
                        raw = stat.get("value")
                        try:
                            val = float(raw) if raw is not None and str(raw).strip() not in ("", "None") else None
                        except (TypeError, ValueError):
                            val = None
                        if idx == 0:
                            home_xg = val
                        else:
                            away_xg = val
                        break
            if home_xg is not None or away_xg is not None:
                with get_connection() as conn:
                    conn.execute("""
                        UPDATE historical_matches
                        SET home_xg = COALESCE(?, home_xg),
                            away_xg = COALESCE(?, away_xg)
                        WHERE fixture_id = ?
                    """, (home_xg, away_xg, fid))
                updated += 1
        except Exception as e:
            logger.warning("Error xG fixture %s: %s", fid, e)

    logger.info("Paso 5 OK: %d partidos con xG actualizado", updated)
    return updated


def print_summary() -> None:
    """Imprime resumen del estado de la tabla."""
    from db import get_connection
    with get_connection() as conn:
        total = conn.execute("SELECT COUNT(*) FROM historical_matches").fetchone()[0]
        sin_shots = conn.execute("SELECT COUNT(*) FROM historical_matches WHERE home_shots_target IS NULL AND away_shots_target IS NULL").fetchone()[0]
        sin_arbitro = conn.execute("SELECT COUNT(*) FROM historical_matches WHERE referee IS NULL OR referee = ''").fetchone()[0]
        sin_xg = conn.execute("SELECT COUNT(*) FROM historical_matches WHERE home_xg IS NULL AND away_xg IS NULL").fetchone()[0]
        con_xg = total - sin_xg
        max_date = conn.execute("SELECT MAX(date) FROM historical_matches").fetchone()[0]

    logger.info("=" * 60)
    logger.info("ESTADO historical_matches:")
    logger.info("  Total partidos:         %d", total)
    logger.info("  Último partido:         %s", max_date)
    logger.info("  Sin tiros a puerta:     %d (%.1f%%)", sin_shots, sin_shots / total * 100)
    logger.info("  Sin árbitro:            %d (%.1f%%)", sin_arbitro, sin_arbitro / total * 100)
    logger.info("  Sin xG:                 %d (%.1f%%)", sin_xg, sin_xg / total * 100)
    logger.info("  Con xG:                 %d (%.1f%%)", con_xg, con_xg / total * 100)
    logger.info("=" * 60)


def main() -> int:
    parser = argparse.ArgumentParser(description="Actualización diaria de historical_matches")
    parser.add_argument("--days-back", type=int, default=2, help="Días hacia atrás para buscar partidos nuevos (default 2)")
    parser.add_argument("--batch", type=int, default=100, help="Partidos por lote en backfills (default 100)")
    parser.add_argument("--delay", type=float, default=0.25, help="Pausa entre peticiones API-Sports (default 0.25s Pro)")
    parser.add_argument("--leagues", nargs="*", help="Ligas específicas (ej: CL EL PL). Default: todas")
    parser.add_argument("--skip-stats", action="store_true", help="Solo nuevos partidos, sin backfills")
    parser.add_argument("--dry-run", action="store_true", help="Solo reporta sin modificar")
    parser.add_argument("--summary-only", action="store_true", help="Solo muestra resumen del estado actual")
    args = parser.parse_args()

    from db import init_db
    init_db()

    if args.summary_only:
        print_summary()
        return 0

    date_to = date.today().isoformat()
    date_from = (date.today() - timedelta(days=args.days_back)).isoformat()
    leagues = args.leagues or None

    logger.info("Iniciando actualización diaria: %s → %s", date_from, date_to)
    if leagues:
        logger.info("Ligas: %s", ", ".join(leagues))

    # Paso 1: nuevos partidos
    step1_new_matches(date_from, date_to, leagues, args.dry_run)

    if not args.skip_stats:
        # Paso 2: estadísticas
        step2_backfill_statistics(args.delay, args.batch, args.dry_run)
        # Paso 3: tarjetas
        step3_backfill_cards(args.delay, args.batch, args.dry_run)
        # Paso 4: árbitro/asistencia
        step4_backfill_referee(args.delay, args.dry_run)
        # Paso 5: xG
        step5_backfill_xg(args.delay, args.batch, args.dry_run)

    print_summary()
    return 0


if __name__ == "__main__":
    sys.exit(main())
