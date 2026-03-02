#!/usr/bin/env python3
"""
Backfill de estadísticas (shots, corners, fouls, offsides, tarjetas, xG) en historical_matches
desde API-Sports /fixtures/statistics.

OPTIMIZADO: 1 sola llamada por partido (ambos equipos + xG en el mismo request).
Antes se hacían 2 llamadas por partido → ahora consume 50% menos de cuota.

Solo procesa partidos con api_sports_fixture_id (vinculados a API-Sports).

  python run_backfill_statistics_from_apisports.py              # 50 partidos, 0.25 s
  python run_backfill_statistics_from_apisports.py --continuous  # hasta vaciar la cola
  python run_backfill_statistics_from_apisports.py --dry-run     # solo reporta pendientes
  python run_backfill_statistics_from_apisports.py --season 2024 # solo una temporada
"""

import argparse
import logging
import sys
import time
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Backfill shots, corners, fouls, tarjetas y xG desde API-Sports (1 call/partido)"
    )
    parser.add_argument("--batch-size", type=int, default=50,
                        help="Partidos por lote (default 50)")
    parser.add_argument("--delay", type=float, default=0.25,
                        help="Segundos entre peticiones (default 0.25 para Pro/Ultra)")
    parser.add_argument("--continuous", action="store_true",
                        help="Continuar hasta vaciar la cola completa")
    parser.add_argument("--dry-run", action="store_true",
                        help="Solo reporta cuántos pendientes hay, sin modificar BD")
    parser.add_argument("--max-empty-batches", type=int, default=5,
                        help="Salir tras N lotes consecutivos sin actualizaciones (default 5)")
    parser.add_argument("--season", type=int, default=None,
                        help="Procesar solo partidos de esta temporada")
    parser.add_argument("--league", type=str, default=None,
                        help="Procesar solo partidos de esta liga (ej. PL, SA, CL)")
    args = parser.parse_args()

    from api_sports_fetcher import get_fixture_statistics_both, API_KEY
    from db import init_db, get_connection, update_historical_statistics

    init_db()

    if not API_KEY:
        logger.error("API_FOOTBALL_KEY no configurada en .env")
        return 1

    def get_pending(limit: int):
        filters = ["api_sports_fixture_id IS NOT NULL", "home_shots IS NULL"]
        params = []
        if args.season:
            filters.append("season = ?")
            params.append(args.season)
        if args.league:
            filters.append("league_id = ?")
            params.append(args.league)
        where = " AND ".join(filters)
        with get_connection() as conn:
            rows = conn.execute(
                f"""SELECT fixture_id, api_sports_fixture_id, date, league_id, season,
                           home_team_name, away_team_name
                    FROM historical_matches
                    WHERE {where}
                    ORDER BY season DESC, date DESC
                    LIMIT ?""",
                params + [limit],
            ).fetchall()
        return [
            {"fixture_id": r[0], "api_sports_fixture_id": r[1], "date": r[2],
             "league_id": r[3], "season": r[4],
             "home_team_name": r[5], "away_team_name": r[6]}
            for r in rows
        ]

    def count_pending() -> int:
        filters = ["api_sports_fixture_id IS NOT NULL", "home_shots IS NULL"]
        params = []
        if args.season:
            filters.append("season = ?")
            params.append(args.season)
        if args.league:
            filters.append("league_id = ?")
            params.append(args.league)
        where = " AND ".join(filters)
        with get_connection() as conn:
            return conn.execute(
                f"SELECT COUNT(*) FROM historical_matches WHERE {where}", params
            ).fetchone()[0]

    def update_xg(fixture_id: int, home_xg: Optional[float], away_xg: Optional[float]):
        if home_xg is None and away_xg is None:
            return
        with get_connection() as conn:
            conn.execute(
                """UPDATE historical_matches
                   SET home_xg = COALESCE(home_xg, ?),
                       away_xg = COALESCE(away_xg, ?)
                   WHERE fixture_id = ?""",
                (home_xg, away_xg, fixture_id),
            )

    pending_count = count_pending()

    if args.dry_run:
        logger.info("Partidos pendientes de backfill: %d", pending_count)
        logger.info("Calls API estimadas (1 por partido): %d", pending_count)
        logger.info("Tiempo estimado (%.2fs delay): %.1f horas",
                    args.delay, pending_count * args.delay / 3600)
        return 0

    logger.info("Backfill estadísticas + xG — %d partidos pendientes", pending_count)
    logger.info("Configuración: delay=%.2fs, batch=%d, continuous=%s",
                args.delay, args.batch_size, args.continuous)

    total_updated = 0
    total_xg = 0
    batch_num = 0
    consecutive_empty = 0

    while True:
        pending = get_pending(args.batch_size)
        if not pending:
            logger.info("Cola vacía — backfill completado.")
            break

        batch_num += 1
        updated_this = 0
        xg_this = 0
        logger.info("Lote %d: %d partidos", batch_num, len(pending))

        for m in pending:
            fid = m["fixture_id"]
            api_fid = m["api_sports_fixture_id"]

            try:
                # 1 sola llamada para ambos equipos (ahorra 50% de cuota)
                both = get_fixture_statistics_both(api_fid)
                time.sleep(args.delay)

                if not both:
                    continue  # Sin datos; el contador de empty batches detectará el estancamiento

                h = both.get("home") or {}
                a = both.get("away") or {}

                # Estadísticas
                updated = update_historical_statistics(
                    fixture_id=fid,
                    home_shots=h.get("shots"),
                    away_shots=a.get("shots"),
                    home_shots_target=h.get("shots_on_target"),
                    away_shots_target=a.get("shots_on_target"),
                    home_corners=h.get("corners"),
                    away_corners=a.get("corners"),
                    home_fouls=h.get("fouls"),
                    away_fouls=a.get("fouls"),
                    home_offsides=h.get("offsides"),
                    away_offsides=a.get("offsides"),
                )
                if updated:
                    updated_this += 1
                    total_updated += 1

                # xG (extraído del mismo request, sin llamada extra)
                h_xg = h.get("xg")
                a_xg = a.get("xg")
                if h_xg is not None or a_xg is not None:
                    update_xg(fid, h_xg, a_xg)
                    xg_this += 1
                    total_xg += 1

                # Tarjetas (incluidas en el mismo request)
                h_y = h.get("yellow_cards")
                h_r = h.get("red_cards")
                a_y = a.get("yellow_cards")
                a_r = a.get("red_cards")
                if any(v is not None for v in (h_y, h_r, a_y, a_r)):
                    with get_connection() as conn:
                        conn.execute(
                            """UPDATE historical_matches
                               SET home_yellow = COALESCE(home_yellow, ?),
                                   home_red    = COALESCE(home_red, ?),
                                   away_yellow = COALESCE(away_yellow, ?),
                                   away_red    = COALESCE(away_red, ?)
                               WHERE fixture_id = ?""",
                            (h_y, h_r, a_y, a_r, fid),
                        )

            except Exception as e:
                logger.warning("Error fixture %s (%s/%s %s vs %s): %s",
                               fid, m["league_id"], m["season"],
                               m["home_team_name"], m["away_team_name"], e)
                time.sleep(args.delay)

        remaining = count_pending()
        logger.info(
            "Lote %d: stats=%d, xG=%d | Total: stats=%d, xG=%d | Pendientes: %d",
            batch_num, updated_this, xg_this, total_updated, total_xg, remaining,
        )

        if not args.continuous:
            break
        if remaining == 0:
            logger.info("Cola vacía — backfill completado.")
            break

        if updated_this == 0 and xg_this == 0:
            consecutive_empty += 1
            if consecutive_empty >= args.max_empty_batches:
                logger.info(
                    "Deteniendo: %d lotes consecutivos sin datos de estadísticas. "
                    "Quedan %d pendientes (ligas/temporadas sin datos en API-Sports).",
                    consecutive_empty, remaining,
                )
                break
        else:
            consecutive_empty = 0

        time.sleep(max(2.0, args.delay * 4))

    logger.info("COMPLETADO — stats actualizadas: %d, xG actualizados: %d",
                total_updated, total_xg)
    return 0


if __name__ == "__main__":
    sys.exit(main())
