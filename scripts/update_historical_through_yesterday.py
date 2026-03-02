#!/usr/bin/env python3
"""
Actualiza la tabla histórica hasta ayer y rellena datos (referee, estadísticas, tarjetas).
Ejecutar cuando el backfill de tarjetas en segundo plano haya terminado.

Pasos:
  1) Comprueba que no queden partidos pendientes de backfill de tarjetas (si hay, sale).
  2) Añade partidos nuevos desde el día siguiente al último partido en BD hasta ayer.
  3) Ejecuta backfills: attendance/referee, estadísticas y tarjetas (hasta dejar sin pendientes).
  4) Test: imprime el último partido (partido, equipos, tarjetas, árbitro, corners, goles).

Uso:
  python scripts/update_historical_through_yesterday.py
  python scripts/update_historical_through_yesterday.py --skip-backfills   # solo partidos + test
  python scripts/update_historical_through_yesterday.py --dry-run          # solo comprobaciones y test
"""

import argparse
import logging
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def check_pending_cards() -> int:
    """Devuelve número de partidos pendientes de backfill de tarjetas."""
    from db import get_historical_matches_pending_card_backfill, init_db
    init_db()
    pending = get_historical_matches_pending_card_backfill(limit=99999)
    return len(pending)


def get_last_match_date() -> Optional[str]:
    """Fecha (YYYY-MM-DD) del último partido en historical_matches, o None si está vacía."""
    from db import get_connection
    with get_connection() as conn:
        row = conn.execute(
            "SELECT date FROM historical_matches ORDER BY date DESC LIMIT 1"
        ).fetchone()
    return row[0] if row else None


def update_matches_through_yesterday(dry_run: bool) -> int:
    """
    Inserta/actualiza partidos desde (última_fecha + 1 día) hasta ayer.
    Devuelve número de partidos insertados/actualizados.
    """
    from data_downloader import update_from_apisports

    last = get_last_match_date()
    if not last:
        logger.warning("No hay partidos en historical_matches; no se puede calcular rango.")
        return 0

    try:
        last_d = date.fromisoformat(last)
    except ValueError:
        logger.warning("Fecha última en BD no válida: %s", last)
        return 0

    yesterday = date.today() - timedelta(days=1)
    date_from = (last_d + timedelta(days=1)).isoformat()
    date_to = yesterday.isoformat()

    if date_from > date_to:
        logger.info("Tabla ya actualizada hasta ayer (último partido %s).", last)
        return 0

    if dry_run:
        logger.info("[DRY-RUN] Llamaría a update_from_apisports(%s, %s)", date_from, date_to)
        return 0

    n, err = update_from_apisports(date_from=date_from, date_to=date_to)
    if err:
        logger.warning("update_from_apisports terminó con error: %s (insertados hasta entonces: %d)", err, n)
    else:
        logger.info("Partidos insertados/actualizados: %d (desde %s hasta %s).", n, date_from, date_to)
    return n


def run_backfills(dry_run: bool) -> None:
    """Ejecuta backfill attendance/referee, luego estadísticas, luego tarjetas."""
    if dry_run:
        logger.info("[DRY-RUN] Omitiendo backfills.")
        return

    for name, cmd in [
        ("attendance/referee", [sys.executable, str(ROOT / "run_backfill_attendance_referee_from_apisports.py"), "--delay", "0.25"]),
        ("estadísticas", [sys.executable, str(ROOT / "run_backfill_statistics_from_apisports.py"), "--continuous", "--delay", "0.25"]),
        ("tarjetas", [sys.executable, str(ROOT / "run_backfill_cards_from_apisports.py"), "--continuous", "--delay", "0.25"]),
    ]:
        logger.info("Ejecutando backfill: %s ...", name)
        r = subprocess.run(cmd, cwd=str(ROOT))
        if r.returncode != 0:
            logger.warning("Backfill %s terminó con código %d.", name, r.returncode)
        else:
            logger.info("Backfill %s completado.", name)


def print_last_match_test() -> None:
    """
    Prueba: último partido en la tabla con partido, equipos, tarjetas, árbitro, corners, goles.
    """
    from db import get_connection, init_db
    init_db()

    with get_connection() as conn:
        row = conn.execute(
            """SELECT date, home_team_name, away_team_name,
                      home_yellow, away_yellow, home_red, away_red,
                      referee, home_corners, away_corners, home_goals, away_goals, league_id
               FROM historical_matches
               ORDER BY date DESC, id DESC
               LIMIT 1"""
        ).fetchone()

    if not row:
        logger.info("Prueba último partido: No hay partidos en la tabla.")
        return

    (date_str, home, away, hy, ay, hr, ar, ref, hc, ac, hg, ag, league_id) = row
    home = home or "—"
    away = away or "—"
    ref = ref or "—"
    hy = hy if hy is not None else "—"
    ay = ay if ay is not None else "—"
    hr = hr if hr is not None else "—"
    ar = ar if ar is not None else "—"
    hc = hc if hc is not None else "—"
    ac = ac if ac is not None else "—"
    hg_val = hg if hg is not None else "—"
    ag_val = ag if ag is not None else "—"

    logger.info("========== PRUEBA: último partido (historical_matches) ==========")
    logger.info("  Partido:  %s", date_str)
    logger.info("  Equipos:  %s vs %s (%s)", home, away, league_id or "")
    logger.info("  Tarjetas: local %s amarillas / %s rojas — visitante %s amarillas / %s rojas", hy, hr, ay, ar)
    logger.info("  Árbitro:  %s", ref)
    logger.info("  Corners:  %s - %s", hc, ac)
    logger.info("  Goles:    %s - %s", hg_val, ag_val)
    logger.info("=================================================================")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Actualiza historial hasta ayer, backfills y test del último partido"
    )
    parser.add_argument(
        "--skip-backfills",
        action="store_true",
        help="No ejecutar backfills (solo actualizar partidos y mostrar test)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Solo comprobar tarjetas pendientes, simular actualización y mostrar test",
    )
    args = parser.parse_args()

    # 1) Comprobar que el backfill de tarjetas haya terminado
    pending_cards = check_pending_cards()
    if pending_cards > 0:
        logger.error(
            "Hay %d partidos pendientes de backfill de tarjetas. "
            "Ejecuta este script cuando el proceso en segundo plano haya terminado.",
            pending_cards,
        )
        return 1

    logger.info("Backfill de tarjetas: sin pendientes. Continuando.")

    # 2) Actualizar partidos hasta ayer
    update_matches_through_yesterday(dry_run=args.dry_run)

    # 3) Actualizar data histórica (backfills)
    if not args.skip_backfills and not args.dry_run:
        run_backfills(dry_run=False)
    elif args.dry_run:
        run_backfills(dry_run=True)

    # 4) Test: último partido
    print_last_match_test()

    return 0


if __name__ == "__main__":
    sys.exit(main())
