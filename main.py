"""
Módulo Principal - CLI para barrido diario y queries.
Uso:
  python main.py --mode=daily_scan
  python main.py --mode=query --query="análisis League One hoy"
  python main.py --mode=query --query="top 3 equipos league one >1 gol últimas 10"
  python main.py --mode=daily_scan --mock   # pruebas sin API key
"""

import argparse
import json
import os
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Union

import pandas as pd

from config import TOP_10_LEAGUE_CODES, LEAGUES, get_league_id
from scraper import get_upcoming_matches
from analyzer import (
    analyze_specific_league,
    top_teams_avg_goals,
    run_query,
)


# Carpeta para logs/histórico
OUTPUT_DIR = Path(__file__).resolve().parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True)


def _save_log(data: Union[list, dict], name: str) -> None:
    """Guarda análisis en JSON y CSV (si es lista de filas) para histórico."""
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base = OUTPUT_DIR / f"{name}_{stamp}"
    with open(f"{base}.json", "w", encoding="utf-8") as f:
        json.dump(data if isinstance(data, list) else data, f, ensure_ascii=False, indent=2)
    if isinstance(data, list) and data and isinstance(data[0], dict):
        try:
            pd.DataFrame(data).to_csv(f"{base}.csv", index=False, encoding="utf-8")
        except Exception:
            pass


def daily_scan(league_codes: Optional[List[str]] = None, use_mock: bool = False) -> pd.DataFrame:
    """Barre partidos de las ligas dadas (códigos football-data.org), calcula probs y muestra tabla."""
    codes = league_codes or TOP_10_LEAGUE_CODES
    all_matches = get_upcoming_matches(codes, days_ahead=3, use_mock=use_mock)
    if not all_matches:
        print("No se encontraron partidos próximos.")
        return pd.DataFrame()

    # Agrupar por competición y analizar cada liga
    by_league: dict[str, list] = {}
    for m in all_matches:
        lcode = m.get("league_id") or ""
        by_league.setdefault(lcode, []).append(m)

    rows = []
    for league_code, _matches in by_league.items():
        _, df = analyze_specific_league(league_code, date_filter="", use_mock=use_mock)
        if not df.empty:
            rows.extend(df.to_dict("records"))

    if not rows:
        print("No se pudo calcular probabilidades (¿API key válida?). Usa --mock para pruebas.")
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    _save_log(rows, "daily_scan")
    print(df.to_string())
    return df


def query_mode(q: str, use_mock: bool = False) -> None:
    """Ejecuta una query en lenguaje natural y muestra resultado."""
    result = run_query(q, use_mock=use_mock)
    if isinstance(result, pd.DataFrame):
        print(result.to_string())
        _save_log(result.to_dict("records"), "query")
    elif isinstance(result, list):
        print(pd.DataFrame(result).to_string())
        _save_log(result, "query")
    else:
        print(result)


def main() -> None:
    parser = argparse.ArgumentParser(description="ForgeWin: estadísticas y análisis de partidos")
    parser.add_argument("--mode", default="daily_scan", choices=["daily_scan", "query"], help="Modo: daily_scan o query")
    parser.add_argument("--query", type=str, default="", help='Query en lenguaje natural, ej: "análisis League One hoy"')
    parser.add_argument("--leagues", type=str, default="", help="Códigos de ligas separados por coma (ej: PL,PD,EL1)")
    parser.add_argument("--mock", action="store_true", help="Usar datos mock (sin API key)")
    args = parser.parse_args()

    if args.mode == "query":
        if not args.query.strip():
            print("Indica --query=\"...\" para modo query.")
            return
        query_mode(args.query.strip(), use_mock=args.mock)
        return

    league_codes = None
    if args.leagues:
        league_codes = [x.strip().upper() for x in args.leagues.split(",") if x.strip()]
        if not league_codes:
            print("--leagues debe ser códigos separados por coma (ej: PL,PD,EL1)")
            return
    daily_scan(league_codes=league_codes, use_mock=args.mock)


if __name__ == "__main__":
    main()
