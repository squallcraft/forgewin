#!/usr/bin/env python3
"""
Descarga el historial completo de football-data.co.uk (desde 1993/94 hasta la
temporada actual) y lo carga en la base de datos. Luego descarga Champions League
y Europa League desde API-Sports (no hay CSV en football-data.co.uk) y rellena FTR.

Disponibilidad:
- CSV: PL, PD, SA, BL1, FL1, DED, PPL, ELC, EL1 (LEAGUE_TO_CSV_CODE)
- API-Sports: CL, EL (por temporadas; primera disponible por defecto 2010)

Para no exceder cuotas:
- Pausa entre cada petición: FOOTBALL_DATA_DOWNLOAD_DELAY (default 1.0 s)
- Descarga en lotes de temporadas con pausa entre lotes (default 10 temporadas, 25 s)
- CL/EL: pausa entre temporadas (default 1 s); requiere API_FOOTBALL_KEY

Uso:
  python run_download_full_historical.py                    # descarga + carga + CL/EL + backfill FTR
  python run_download_full_historical.py --dry-run         # solo imprime rango y sale
  python run_download_full_historical.py --skip-cl-el      # no descargar Champions/Europa League
  python run_download_full_historical.py --batch-size 8 --pause 30
"""

import argparse
import logging
import sys
from pathlib import Path

# Raíz del proyecto
sys.path.insert(0, str(Path(__file__).resolve().parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def main() -> int:
    parser = argparse.ArgumentParser(description="Descarga historial completo football-data.co.uk y carga en BD")
    parser.add_argument("--dry-run", action="store_true", help="Solo mostrar rango de temporadas y número de peticiones estimado")
    parser.add_argument("--batch-size", type=int, default=10, help="Temporadas por lote (pausa entre lotes). Default 10.")
    parser.add_argument("--pause", type=float, default=25.0, help="Segundos de pausa entre lotes. Default 25.")
    parser.add_argument("--csv-dir", type=str, default="data/csv_backup", help="Carpeta donde guardar CSVs")
    parser.add_argument("--skip-cl-el", action="store_true", help="No descargar Champions League ni Europa League (API-Sports)")
    args = parser.parse_args()

    from data_downloader import (
        FD_FIRST_AVAILABLE_SEASON,
        LEAGUE_TO_CSV_CODE,
        get_full_historical_season_range,
        download_full_historical,
        load_csv_rows_into_db,
        fetch_and_load_historical_cl_el,
        CL_EL_FIRST_AVAILABLE_SEASON,
        _current_season_year,
    )
    from db import backfill_ftr_from_goals

    first, last = get_full_historical_season_range()
    n_seasons = last - first + 1
    n_leagues = len(LEAGUE_TO_CSV_CODE)
    # Estimación: una petición por (liga, temporada). Algunas devolverán 404.
    estimated_requests = n_seasons * n_leagues

    logger.info(
        "Rango: temporadas %s a %s (%s años). Ligas: %s. Peticiones estimadas: %s.",
        first, last, n_seasons, list(LEAGUE_TO_CSV_CODE.keys()), estimated_requests,
    )

    if args.dry_run:
        logger.info("DRY-RUN: no se descarga ni se carga. Ejecuta sin --dry-run para descargar.")
        return 0

    save_dir = Path(args.csv_dir)
    save_dir.mkdir(parents=True, exist_ok=True)

    rows = download_full_historical(
        save_dir=save_dir,
        batch_seasons=args.batch_size,
        pause_between_batches_seconds=args.pause,
    )
    logger.info("Descarga completada: %d filas parseadas.", len(rows))

    if not rows:
        logger.warning("No se obtuvo ninguna fila. Revisa conectividad o que las URLs existan.")
        return 0

    n = load_csv_rows_into_db(rows)
    logger.info("Cargados en BD: %d registros en historical_matches.", n)

    if not args.skip_cl_el:
        n_cl_el, err = fetch_and_load_historical_cl_el(
            season_from=CL_EL_FIRST_AVAILABLE_SEASON,
            season_to=_current_season_year(),
            pause_between_seasons_seconds=1.0,
        )
        if err:
            logger.warning("CL/EL: %s (¿API_FOOTBALL_KEY configurada?)", err)
        else:
            logger.info("CL/EL cargados en BD: %d partidos.", n_cl_el)
    else:
        logger.info("CL/EL omitidos (--skip-cl-el).")

    updated_ftr = backfill_ftr_from_goals()
    if updated_ftr:
        logger.info("Backfill FTR desde goles: %d filas actualizadas.", updated_ftr)

    return 0


if __name__ == "__main__":
    sys.exit(main())
