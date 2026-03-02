#!/usr/bin/env python3
"""
Descarga clasificaciones oficiales desde API-Sports y las carga en league_standings.
Las ligas y temporadas se leen desde la BD (no se calculan desde partidos).

Uso:
  python run_download_standings.py                     # ligas por defecto, temporadas 2022-2024
  python run_download_standings.py --seasons 2023 2024
"""

import argparse
import logging
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Ligas a descargar (códigos Forgewin = API-Sports)
DEFAULT_LEAGUES = ["PL", "PD", "SA", "BL1", "FL1", "CL", "EL", "DED", "PPL", "ELC", "EL1"]
DEFAULT_SEASONS = [2022, 2023, 2024]


def main() -> int:
    parser = argparse.ArgumentParser(description="Descarga clasificaciones API-Sports y carga en league_standings")
    parser.add_argument("--leagues", nargs="+", default=DEFAULT_LEAGUES, help="Códigos de liga (PL, PD, SA, ...)")
    parser.add_argument("--seasons", nargs="+", type=int, default=DEFAULT_SEASONS, help="Años de temporada (ej. 2022 2023 2024)")
    parser.add_argument("--delay", type=float, default=1.0, help="Segundos entre peticiones (rate limit)")
    args = parser.parse_args()

    from api_sports_fetcher import get_standings_api_sports, LEAGUE_CODE_TO_ID, API_KEY
    from db import replace_league_standings, init_db

    init_db()
    if not API_KEY:
        logger.error("API_FOOTBALL_KEY no configurada. Añádela en .env")
        return 1

    total = 0
    for league_id in args.leagues:
        if league_id not in LEAGUE_CODE_TO_ID:
            logger.warning("Liga %s no soportada por API-Sports, omitiendo", league_id)
            continue
        for season in args.seasons:
            try:
                rows = get_standings_api_sports(league_id, season)
                if not rows:
                    logger.info("%s %s: sin datos", league_id, season)
                    time.sleep(args.delay)
                    continue
                # Asignar rank secuencial por si la API devuelve varios grupos (ej. CL)
                for i, r in enumerate(rows, start=1):
                    r["rank"] = i
                n = replace_league_standings(league_id, season, rows)
                total += n
                logger.info("%s temporada %s: %d equipos cargados", league_id, season, n)
            except Exception as e:
                logger.exception("Error %s %s: %s", league_id, season, e)
            time.sleep(args.delay)
    logger.info("Total: %d filas en league_standings", total)
    return 0


if __name__ == "__main__":
    sys.exit(main())
