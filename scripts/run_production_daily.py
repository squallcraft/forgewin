#!/usr/bin/env python3
"""
Script maestro de producción: actualización diaria completa sin intervención humana.

Orquesta todos los pasos necesarios para mantener historical_matches al día:
  1. Partidos nuevos (últimos N días) desde API-Sports
  2. Backfill estadísticas (shots, corners, fouls, offsides)
  3. Backfill tarjetas (yellow/red)
  4. Backfill offsides (filas con stats pero sin offsides)
  5. Backfill attendance y referee (vía GET /fixtures?id=)
  6. Backfill xG
  7. Resumen final

Orden de backfills: stats → cards → offsides → attendance → xG
(offsides y attendance requieren stats previas en algunos flujos).

Uso:
  python scripts/run_production_daily.py
  python scripts/run_production_daily.py --dry-run
  python scripts/run_production_daily.py --skip-backfills    # solo nuevos partidos
  python scripts/run_production_daily.py --batch-limit 500   # máx partidos por backfill (evitar cron largo)
"""

import argparse
import logging
import subprocess
import sys
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# Cargar variables de entorno ANTES de importar cualquier módulo del proyecto
from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("production_daily")


def step1_new_matches(days_back: int, leagues, dry_run: bool) -> int:
    """Inserta partidos nuevos finalizados desde API-Sports."""
    from data_downloader import update_from_apisports

    date_to = date.today().isoformat()
    date_from = (date.today() - timedelta(days=days_back)).isoformat()

    if dry_run:
        logger.info("[DRY-RUN] Paso 1: update_from_apisports(%s, %s)", date_from, date_to)
        return 0

    n, err = update_from_apisports(date_from=date_from, date_to=date_to, league_codes=leagues)
    if err:
        logger.warning("Paso 1: %s (insertados: %d)", err, n)
    else:
        logger.info("Paso 1 OK: %d partidos insertados/actualizados", n)
    return n


def _run_backfill_script(
    script_name: str, extra_args: list, dry_run: bool, continuous: bool = True
) -> bool:
    """Ejecuta un script de backfill. Con continuous=True procesa hasta vaciar; si no, un solo batch."""
    if dry_run:
        logger.info("[DRY-RUN] Ejecutaría: %s %s", script_name, " ".join(extra_args))
        return True

    cmd = [sys.executable, str(ROOT / script_name), *extra_args]
    if continuous:
        cmd.append("--continuous")
    r = subprocess.run(cmd, cwd=str(ROOT))
    if r.returncode != 0:
        logger.warning("Backfill %s terminó con código %d", script_name, r.returncode)
        return False
    return True


def step2_backfill_statistics(delay: float, batch_limit: Optional[int], dry_run: bool) -> None:
    args = ["--delay", str(delay)]
    continuous = batch_limit is None
    if batch_limit:
        args.extend(["--batch-size", str(batch_limit)])
    _run_backfill_script("run_backfill_statistics_from_apisports.py", args, dry_run, continuous)


def step3_backfill_cards(delay: float, batch_limit: Optional[int], dry_run: bool) -> None:
    args = ["--delay", str(delay)]
    continuous = batch_limit is None
    if batch_limit:
        args.extend(["--batch-size", str(batch_limit)])
    _run_backfill_script("run_backfill_cards_from_apisports.py", args, dry_run, continuous)


def step4_backfill_offsides(delay: float, batch_limit: Optional[int], dry_run: bool) -> None:
    args = ["--delay", str(delay)]
    continuous = batch_limit is None
    if batch_limit:
        args.extend(["--batch-size", str(batch_limit)])
    _run_backfill_script("run_backfill_offsides_from_apisports.py", args, dry_run, continuous)


def step5_backfill_attendance(delay: float, batch_limit: Optional[int], dry_run: bool) -> None:
    args = ["--delay", str(delay)]
    continuous = batch_limit is None
    if batch_limit:
        args.extend(["--batch-size", str(batch_limit)])
    _run_backfill_script("run_backfill_attendance_via_fixture_id.py", args, dry_run, continuous)


def step6_backfill_xg(delay: float, batch: int, dry_run: bool) -> int:
    """Backfill xG en proceso (igual que run_daily_update)."""
    from api_sports_fetcher import _api_get
    from db import get_connection

    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT fixture_id, api_sports_fixture_id, league_id, date
            FROM historical_matches
            WHERE api_sports_fixture_id IS NOT NULL
              AND home_xg IS NULL
              AND status = 'FT'
              AND date >= '2021-01-01'
            ORDER BY date DESC
            LIMIT ?
        """,
            (batch,),
        ).fetchall()

    if not rows:
        logger.info("Paso 6: sin partidos pendientes de xG.")
        return 0
    if dry_run:
        logger.info("[DRY-RUN] Paso 6: %d partidos pendientes de xG.", len(rows))
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
                for stat in team_stats.get("statistics") or []:
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
                    conn.execute(
                        """
                        UPDATE historical_matches
                        SET home_xg = COALESCE(?, home_xg), away_xg = COALESCE(?, away_xg)
                        WHERE fixture_id = ?
                    """,
                        (home_xg, away_xg, fid),
                    )
                updated += 1
        except Exception as e:
            logger.warning("Error xG fixture %s: %s", fid, e)

    logger.info("Paso 6 OK: %d partidos con xG actualizado", updated)
    return updated


def print_summary() -> None:
    """Imprime resumen del estado de historical_matches."""
    from db import get_connection

    with get_connection() as conn:
        total = conn.execute("SELECT COUNT(*) FROM historical_matches").fetchone()[0]
        sin_shots = conn.execute(
            "SELECT COUNT(*) FROM historical_matches WHERE home_shots_target IS NULL AND away_shots_target IS NULL"
        ).fetchone()[0]
        sin_offsides = conn.execute(
            """SELECT COUNT(*) FROM historical_matches
               WHERE api_sports_fixture_id IS NOT NULL
                 AND (home_offsides IS NULL OR away_offsides IS NULL)"""
        ).fetchone()[0]
        sin_attendance = conn.execute(
            """SELECT COUNT(*) FROM historical_matches
               WHERE api_sports_fixture_id IS NOT NULL
                 AND (attendance IS NULL OR referee IS NULL OR referee = '')"""
        ).fetchone()[0]
        sin_arbitro = conn.execute(
            "SELECT COUNT(*) FROM historical_matches WHERE referee IS NULL OR referee = ''"
        ).fetchone()[0]
        sin_xg = conn.execute(
            "SELECT COUNT(*) FROM historical_matches WHERE home_xg IS NULL AND away_xg IS NULL"
        ).fetchone()[0]
        con_xg = total - sin_xg
        max_date = conn.execute("SELECT MAX(date) FROM historical_matches").fetchone()[0]

    logger.info("=" * 60)
    logger.info("ESTADO historical_matches:")
    logger.info("  Total partidos:         %d", total)
    logger.info("  Último partido:         %s", max_date)
    logger.info("  Sin tiros a puerta:     %d (%.1f%%)", sin_shots, sin_shots / total * 100 if total else 0)
    logger.info("  Sin offsides (API):     %d", sin_offsides)
    logger.info("  Sin attendance/referee: %d", sin_attendance)
    logger.info("  Sin árbitro:            %d (%.1f%%)", sin_arbitro, sin_arbitro / total * 100 if total else 0)
    logger.info("  Sin xG:                 %d (%.1f%%)", sin_xg, sin_xg / total * 100 if total else 0)
    logger.info("  Con xG:                 %d (%.1f%%)", con_xg, con_xg / total * 100 if total else 0)
    logger.info("=" * 60)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Actualización diaria de producción (sync + backfills completos)"
    )
    parser.add_argument("--days-back", type=int, default=2, help="Días atrás para partidos nuevos")
    parser.add_argument("--delay", type=float, default=0.25, help="Segundos entre peticiones API-Sports")
    parser.add_argument("--batch-limit", type=int, default=None, help="Máx partidos por backfill (evita cron largo)")
    parser.add_argument("--leagues", nargs="*", help="Ligas específicas (ej: CL EL PL)")
    parser.add_argument("--skip-backfills", action="store_true", help="Solo sync nuevos partidos")
    parser.add_argument("--dry-run", action="store_true", help="Solo simular")
    args = parser.parse_args()

    from db import init_db

    init_db()

    logger.info("Iniciando actualización diaria de producción")
    step1_new_matches(args.days_back, args.leagues or None, args.dry_run)

    if not args.skip_backfills:
        batch = args.batch_limit or 500  # xG batch size
        step2_backfill_statistics(args.delay, args.batch_limit, args.dry_run)
        step3_backfill_cards(args.delay, args.batch_limit, args.dry_run)
        step4_backfill_offsides(args.delay, args.batch_limit, args.dry_run)
        step5_backfill_attendance(args.delay, args.batch_limit, args.dry_run)
        step6_backfill_xg(args.delay, batch, args.dry_run)

    print_summary()
    logger.info("Actualización diaria completada.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
