#!/usr/bin/env python3
"""
Corrige fechas mal parseadas en historical_matches (2093 → 1993, etc.).
Bug: años 2 dígitos se sumaban +2000; debía ser +1900 para 30-99.

Uso:
  python run_fix_malformed_dates.py              # corrige fechas en BD
  python run_fix_malformed_dates.py --dry-run    # solo muestra cuántas se corregirían
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from db import fix_historical_malformed_dates, init_db


def main() -> int:
    parser = argparse.ArgumentParser(description="Corregir fechas 209x → 199x en historical_matches")
    parser.add_argument("--dry-run", action="store_true", help="Solo contar, no actualizar")
    args = parser.parse_args()

    init_db()
    n = fix_historical_malformed_dates(dry_run=args.dry_run)
    if args.dry_run:
        print(f"Se corregirían {n} filas (fechas 209x → 199x). Ejecuta sin --dry-run para aplicar.")
    else:
        print(f"Corregidas {n} filas.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
