"""
Worker de actualización de historial rolling: lunes y jueves a las 04:00 (hora Chile).
Ejecuta: 1) Partidos nuevos de la semana desde API-Sports, 2) Mantener ventana (eliminar antiguos).
En producción (Digital Ocean) usar cron. Opcionalmente ejecutar en bucle con zona Chile para desarrollo.
"""

import logging
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

# Asegurar que el proyecto esté en el path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from data_downloader import update_from_apisports
from rolling_window import maintain_rolling_window

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Zona Chile para cron / desarrollo
CHILE_TZ = "America/Santiago"


def run_weekly_job(
    days_back: int = 7,
    use_csv_fallback: bool = True,
    csv_fallback_dir: Optional[Path] = None,
    dry_run: bool = False,
) -> dict:
    """
    Ejecuta una pasada del worker semanal:
    1) Descarga partidos nuevos (finalizados) de los últimos days_back días vía API-Sports.
    2) Si API-Sports falla y use_csv_fallback=True, no hay respaldo automático de CSV (el CSV
       es para carga inicial; el respaldo sería reutilizar el último CSV descargado si se guardó).
    3) Mantiene la ventana rolling (elimina temporadas viejas).
    Devuelve dict con inserted, api_error, deleted_old.
    """
    result = {"inserted": 0, "api_error": None, "deleted_old": 0}
    date_to = date.today().isoformat()
    date_from = (date.today() - timedelta(days=days_back)).isoformat()

    n, err = update_from_apisports(date_from=date_from, date_to=date_to)
    result["inserted"] = n
    result["api_error"] = err
    if err:
        logger.warning("API-Sports devolvió error; intentando fallback CSV: %s", err)
        fallback_dir = Path(csv_fallback_dir or Path(__file__).resolve().parent / "data" / "csv_backup")
        if use_csv_fallback and fallback_dir.exists():
            try:
                from data_downloader import load_from_csv_backup
                loaded = load_from_csv_backup(fallback_dir)
                result["inserted"] = result["inserted"] + loaded
                logger.info("Fallback CSV: cargados %d registros desde %s", loaded, fallback_dir)
            except Exception as e:
                logger.exception("Fallback CSV falló: %s", e)

    deleted, report = maintain_rolling_window(keep_seasons=5, dry_run=dry_run)
    result["deleted_old"] = deleted
    if dry_run:
        result["dry_run_report"] = report
        logger.info("Worker semanal [DRY-RUN]: insertados=%d, se eliminarían=%d", n, report.get("would_delete", 0))
    else:
        logger.info("Worker semanal: insertados=%d, eliminados_antiguos=%d", n, deleted)
    return result


def run_at_04_chile_monday_thursday():
    """
    Para uso con cron en Digital Ocean (o similar):
      0 4 * * 1,4 cd /path/to/project && /path/to/python manage.py update_historical_data --weekly
    O: 0 4 * * 1,4 cd /path && python -m scheduler
    No ejecuta en bucle; una sola pasada. La hora 04:00 la define cron en la máquina (configurar TZ=America/Santiago).
    Si API-Sports falla, carga desde ./data/csv_backup.
    """
    run_weekly_job(days_back=7, use_csv_fallback=True, csv_fallback_dir=Path(__file__).resolve().parent / "data" / "csv_backup")


if __name__ == "__main__":
    run_at_04_chile_monday_thursday()
