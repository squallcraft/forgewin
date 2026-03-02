#!/usr/bin/env python3
"""
Prueba: traer estadísticas de tiros (y córners, faltas) para partidos de Champions League
del test anterior (Inter-Bodo, Real Madrid-Benfica, Atalanta-Monaco, PSG-Monaco, etc.).
Intenta API-Sports (con 1 reintento si 0-0) y luego fallback football-data.org.
Muestra el resultado por partido.
"""

import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
except ImportError:
    pass

def main():
    from db import init_db, get_connection, get_historical_match_for_fallback, update_historical_statistics
    from api_sports_fetcher import get_fixture_statistics, API_KEY as APISPORTS_KEY
    from data_fetcher import get_match_statistics_football_data_org
    from config import API_KEY as FD_KEY

    init_db()

    # Partidos CL con 0-0 tiros (los del test: Inter-Bodo, Real Madrid-Benfica, etc.)
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT fixture_id, api_sports_fixture_id, date, home_team_name, away_team_name,
                   home_team_id, away_team_id, home_shots_target, away_shots_target
            FROM historical_matches
            WHERE league_id = 'CL'
              AND api_sports_fixture_id IS NOT NULL
              AND home_team_id IS NOT NULL AND away_team_id IS NOT NULL
              AND COALESCE(home_shots_target, 0) = 0 AND COALESCE(away_shots_target, 0) = 0
            ORDER BY date DESC
            LIMIT 3
        """)
        partidos = [dict(zip([c[0] for c in cur.description], row)) for row in cur.fetchall()]

    if not partidos:
        print("No hay partidos CL con 0-0 tiros a puerta.")
        return

    print("=" * 70)
    print("PRUEBA: Estadísticas de tiros – Champions League (3 partidos, pausa FD)")
    print("=" * 70)
    print(f"Partidos a consultar: {len(partidos)}\n")

    delay = 0.25  # API-Sports (paga): 0.25 s
    from config import REQUEST_DELAY_SECONDS as fd_delay_seconds  # football-data.org (gratuita): pausa mayor
    for m in partidos:
        fid = m["fixture_id"]
        api_fid = m["api_sports_fixture_id"]
        date = m["date"]
        home_name = m["home_team_name"] or ""
        away_name = m["away_team_name"] or ""
        home_tid = m["home_team_id"]
        away_tid = m["away_team_id"]

        print(f"--- {home_name} vs {away_name} ({date}) [fixture_id={fid}] ---")

        # 1) API-Sports
        hst, ast = None, None
        if APISPORTS_KEY:
            try:
                hs = get_fixture_statistics(api_fid, home_tid)
                time.sleep(delay)
                aw = get_fixture_statistics(api_fid, away_tid)
                time.sleep(delay)
                hst = (hs or {}).get("shots_on_target")
                ast = (aw or {}).get("shots_on_target")
                print(f"  API-Sports: tiros a puerta local={hst}, visitante={ast}")
                if (hst == 0 or hst is None) and (ast == 0 or ast is None):
                    print("  API-Sports: 0-0 → reintento una vez...")
                    time.sleep(delay * 2)
                    hs = get_fixture_statistics(api_fid, home_tid)
                    time.sleep(delay)
                    aw = get_fixture_statistics(api_fid, away_tid)
                    hst = (hs or {}).get("shots_on_target")
                    ast = (aw or {}).get("shots_on_target")
                    print(f"  API-Sports (reintento): local={hst}, visitante={ast}")
            except Exception as e:
                print(f"  API-Sports error: {e}")
        else:
            print("  API-Sports: no configurada (API_FOOTBALL_KEY)")

        # 2) Si sigue 0-0, football-data.org (con pausa para evitar 429)
        if (hst == 0 or hst is None) and (ast == 0 or ast is None) and FD_KEY:
            try:
                time.sleep(fd_delay_seconds)
                fd = get_match_statistics_football_data_org("CL", date, home_name, away_name)
                if fd:
                    if fd.get("_match_found_no_stats"):
                        print("  football-data.org: partido encontrado pero sin estadísticas (¿fecha futura?)")
                    else:
                        hst_fd = fd.get("home_shots_target")
                        ast_fd = fd.get("away_shots_target")
                        print(f"  football-data.org: tiros a puerta local={hst_fd}, visitante={ast_fd}")
                        if (hst_fd or ast_fd) and (hst_fd is not None or ast_fd is not None):
                            ok = update_historical_statistics(
                                fixture_id=fid,
                                home_shots=fd.get("home_shots"),
                                away_shots=fd.get("away_shots"),
                                home_shots_target=hst_fd,
                                away_shots_target=ast_fd,
                                home_corners=fd.get("home_corners"),
                                away_corners=fd.get("away_corners"),
                                home_fouls=fd.get("home_fouls"),
                                away_fouls=fd.get("away_fouls"),
                                home_offsides=fd.get("home_offsides"),
                                away_offsides=fd.get("away_offsides"),
                            )
                            if ok:
                                print("  → Actualizado en BD desde football-data.org")
                            else:
                                print("  → No se pudo actualizar BD")
                        else:
                            print("  → football-data.org sin tiros a puerta para este partido")
                else:
                    print("  football-data.org: sin datos o partido no encontrado")
            except Exception as e:
                print(f"  football-data.org error: {e}")
        elif (hst == 0 or hst is None) and (ast == 0 or ast is None):
            print("  football-data.org: no configurada (FOOTBALL_DATA_ORG_TOKEN) o no aplicable")

        if (hst or ast) and (hst != 0 or ast != 0):
            print("  → Ya hay datos (API-Sports); no se usa fallback.")
        print()

    print("=" * 70)
    print("Fin de la prueba.")
    print("=" * 70)


if __name__ == "__main__":
    main()
