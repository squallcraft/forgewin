#!/usr/bin/env python3
"""
Copia historical_matches → master_table aplicando normalización de nombres.

Orden: 1) entity_aliases (seed si vacío), 2) copy con normalización.

Uso:
  python run_copy_to_master_table.py                  # todas las ligas/temporadas
  python run_copy_to_master_table.py --league PL      # solo Premier League
  python run_copy_to_master_table.py --league SA --season 2022
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
        description="Copia historical_matches a master_table con normalización"
    )
    parser.add_argument("--league", type=str, default=None, help="Liga (PL, SA, etc.)")
    parser.add_argument("--season", type=int, default=None, help="Temporada (año fin)")
    parser.add_argument("--seed-aliases", action="store_true", help="Inserar entity_aliases por defecto")
    args = parser.parse_args()

    from db import init_db, seed_default_entity_aliases, copy_historical_to_master

    init_db()

    if args.seed_aliases:
        n = seed_default_entity_aliases()
        logger.info("Entity aliases insertados: %d", n)

    n = copy_historical_to_master(
        league_id=args.league,
        season=args.season,
    )
    logger.info("Registros copiados/actualizados en master_table: %d", n)
    return 0


if __name__ == "__main__":
    sys.exit(main())
