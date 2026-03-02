#!/usr/bin/env python3
"""
Carga datos históricos de Champions League (CL) y Europa League (EL) en historical_matches.

Fuente: API-Sports (api-sports.io). Requiere API_FOOTBALL_KEY en .env.

Formato homologado con el resto de ligas:
- season = año fin de temporada (ej. 2024 = 2023/24), igual que Premier League, Serie A, La Liga.
- fixture_id en rango 9xx (API-Sports).
- api_sports_fixture_id guardado para estadísticas/backfill.

Uso:
  python run_load_cl_el_historical.py                    # 2010 hasta temporada actual
  python run_load_cl_el_historical.py --from 2015 --to 2024
  python run_load_cl_el_historical.py --dry-run         # solo muestra rango, no descarga
"""

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Carga historial Champions League y Europa League (API-Sports) en historical_matches"
    )
    parser.add_argument("--from", dest="season_from", type=int, default=None,
                        help="Primera temporada (año fin, ej. 2010 = 2009/10)")
    parser.add_argument("--to", dest="season_to", type=int, default=None,
                        help="Última temporada (año fin, ej. 2024 = 2023/24)")
    parser.add_argument("--dry-run", action="store_true", help="Solo mostrar rango, no descargar")
    parser.add_argument("--pause", type=float, default=1.5,
                        help="Segundos de pausa entre temporadas (respetar cuota API). Default 1.5")
    args = parser.parse_args()

    from data_downloader import (
        fetch_and_load_historical_cl_el,
        CL_EL_FIRST_AVAILABLE_SEASON,
        _current_season_year,
    )
    from db import init_db, backfill_ftr_from_goals

    init_db()
    current = _current_season_year()
    first = args.season_from if args.season_from is not None else CL_EL_FIRST_AVAILABLE_SEASON
    last = args.season_to if args.season_to is not None else current

    logger.info(
        "Champions League y Europa League: temporadas %s a %s (año fin = 20xx/xx+1).",
        first, last,
    )
    if args.dry_run:
        logger.info("DRY-RUN: no se descarga. Ejecuta sin --dry-run para cargar.")
        return 0

    n, err = fetch_and_load_historical_cl_el(
        season_from=first,
        season_to=last,
        pause_between_seasons_seconds=args.pause,
    )
    if err:
        logger.error("Error: %s (¿API_FOOTBALL_KEY en .env?)", err)
        return 1
    logger.info("Cargados en historical_matches: %d partidos (CL + EL).", n)

    updated_ftr = backfill_ftr_from_goals()
    if updated_ftr:
        logger.info("Backfill FTR desde goles: %d filas actualizadas.", updated_ftr)

    return 0


if __name__ == "__main__":
    sys.exit(main())
