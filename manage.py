"""
CLI de gestión ForgeWin: historial rolling, migraciones, etc.
Uso:
  python manage.py update_historical_data --full    # Descarga inicial desde CSV + carga BD + rolling
  python manage.py update_historical_data --weekly  # Actualización semanal (API-Sports delta + rolling)
  python manage.py init_db                         # (opcional) Crear tablas si no existen
"""

import argparse
import logging
import sys
from pathlib import Path

# Raíz del proyecto
sys.path.insert(0, str(Path(__file__).resolve().parent))

# Cargar variables de entorno ANTES de importar cualquier módulo del proyecto
# (db.py lee DATABASE_URL al importarse; si no está cargado cae a SQLite vacío)
from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def cmd_init_db(_: argparse.Namespace) -> int:
    """Crea todas las tablas (incl. historical_matches) si no existen."""
    from db import init_db
    init_db()
    logger.info("init_db: listo")
    return 0


def cmd_update_historical_data(args: argparse.Namespace) -> int:
    dry_run = getattr(args, "dry_run", False)
    if args.full:
        from data_downloader import run_full_initial_download
        from rolling_window import maintain_rolling_window
        save_dir = Path(args.csv_dir) if (getattr(args, "csv_dir", None) and str(args.csv_dir).strip()) else None
        n = run_full_initial_download(save_dir=save_dir)
        deleted, report = maintain_rolling_window(keep_seasons=5, dry_run=dry_run)
        if dry_run:
            logger.info("[DRY-RUN] Se eliminarían %d partidos. Por temporada: %s", report.get("would_delete", 0), report.get("by_season", []))
        else:
            logger.info("Descarga inicial completada: %d registros cargados, %d antiguos eliminados", n, deleted)
        return 0
    if args.weekly:
        from scheduler import run_weekly_job
        result = run_weekly_job(days_back=args.days_back or 7, dry_run=dry_run)
        if dry_run and result.get("dry_run_report"):
            logger.info("[DRY-RUN] Rolling: %s", result["dry_run_report"])
        logger.info("Actualización semanal: insertados=%d, eliminados=%d", result["inserted"], result["deleted_old"])
        if result["api_error"]:
            logger.warning("API-Sports error: %s", result["api_error"])
        return 0
    logger.error("Indica --full o --weekly para update_historical_data")
    return 1


def cmd_maintain_rolling_window(args: argparse.Namespace) -> int:
    """Solo mantiene la ventana rolling (elimina temporadas antiguas)."""
    from rolling_window import maintain_rolling_window

    deleted, report = maintain_rolling_window(
        keep_seasons=getattr(args, "keep_seasons", 5),
        dry_run=getattr(args, "dry_run", False),
    )
    if args.dry_run:
        logger.info("[DRY-RUN] Se eliminarían %d partidos. Por temporada: %s", report.get("would_delete", 0), report.get("by_season", []))
    else:
        logger.info("Rolling window: eliminados %d partidos", deleted)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="ForgeWin: gestión y workers")
    subparsers = parser.add_subparsers(dest="command", help="Comando")

    # init_db
    subparsers.add_parser("init_db", help="Crear tablas en la BD")

    # maintain_rolling_window
    rw = subparsers.add_parser("maintain_rolling_window", help="Eliminar temporadas antiguas (ventana rolling)")
    rw.add_argument("--keep-seasons", type=int, default=5, help="Temporadas a mantener (default 5)")
    rw.add_argument("--dry-run", action="store_true", help="No borrar; solo mostrar qué se eliminaría")

    # update_historical_data
    hist = subparsers.add_parser("update_historical_data", help="Historial rolling: descarga inicial o actualización semanal")
    hist.add_argument("--full", action="store_true", help="Descarga inicial desde football-data.co.uk CSV y carga en BD")
    hist.add_argument("--weekly", action="store_true", help="Actualización semanal (API-Sports delta + ventana rolling)")
    hist.add_argument("--csv-dir", type=str, default="", help="Carpeta donde guardar CSVs (solo con --full)")
    hist.add_argument("--days-back", type=int, default=7, help="Días atrás para partidos nuevos (solo --weekly, default 7)")
    hist.add_argument("--dry-run", action="store_true", help="No borrar datos; solo mostrar qué se eliminaría en rolling")

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        return 0

    if args.command == "init_db":
        return cmd_init_db(args)
    if args.command == "maintain_rolling_window":
        return cmd_maintain_rolling_window(args)
    if args.command == "update_historical_data":
        return cmd_update_historical_data(args)

    parser.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main())
