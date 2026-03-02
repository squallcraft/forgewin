#!/usr/bin/env python3
"""
Listado de historical_matches: columna | cantidad de registros | significado.
Ejecutar manualmente o al final del backfill de tarjetas.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from db import get_connection, init_db

# Columnas y significado (orden lógico)
COLUMNS_AND_MEANING = [
    ("id", "ID autoincremental del registro"),
    ("fixture_id", "Identificador único del partido (hash o API)"),
    ("date", "Fecha del partido (YYYY-MM-DD)"),
    ("league_id", "Código de la competición (PL, PD, SA, BL1, etc.)"),
    ("home_team_id", "ID del equipo local (API-Sports)"),
    ("away_team_id", "ID del equipo visitante (API-Sports)"),
    ("home_team_name", "Nombre del equipo local"),
    ("away_team_name", "Nombre del equipo visitante"),
    ("home_goals", "Goles del equipo local a tiempo completo"),
    ("away_goals", "Goles del equipo visitante a tiempo completo"),
    ("status", "Estado del partido (FT, SCHEDULED, etc.)"),
    ("season", "Año fin de temporada (ej. 2024 = 2023/24)"),
    ("home_xg", "Expected goals equipo local"),
    ("away_xg", "Expected goals equipo visitante"),
    ("api_sports_fixture_id", "ID del partido en API-Sports"),
    ("kickoff_time", "Hora de inicio (ej. 20:00)"),
    ("ftr", "Resultado final: H=local gana, D=empate, A=visitante gana"),
    ("hthg", "Goles del equipo local al medio tiempo"),
    ("htag", "Goles del equipo visitante al medio tiempo"),
    ("htr", "Resultado al medio tiempo (H/D/A)"),
    ("attendance", "Asistencia al estadio"),
    ("referee", "Nombre del árbitro"),
    ("home_shots", "Tiros totales equipo local"),
    ("away_shots", "Tiros totales equipo visitante"),
    ("home_shots_target", "Tiros a puerta equipo local"),
    ("away_shots_target", "Tiros a puerta equipo visitante"),
    ("home_corners", "Corners equipo local"),
    ("away_corners", "Corners equipo visitante"),
    ("home_fouls", "Faltas cometidas por equipo local"),
    ("away_fouls", "Faltas cometidas por equipo visitante"),
    ("home_yellow", "Tarjetas amarillas equipo local"),
    ("away_yellow", "Tarjetas amarillas equipo visitante"),
    ("home_red", "Tarjetas rojas equipo local"),
    ("away_red", "Tarjetas rojas equipo visitante"),
    ("home_offsides", "Fueras de juego equipo local"),
    ("away_offsides", "Fueras de juego equipo visitante"),
    ("created_at", "Fecha de creación del registro en BD"),
]


def run_report() -> None:
    init_db()
    with get_connection() as conn:
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM historical_matches")
        total = c.fetchone()[0]
        print(f"Tabla: historical_matches | Total filas: {total}")
        print()
        print(f"{'Columna':<28} | {'Registros':>12} | Significado")
        print("-" * 100)
        for col, meaning in COLUMNS_AND_MEANING:
            try:
                c.execute(
                    f"SELECT COUNT(*) FROM historical_matches WHERE {col} IS NOT NULL AND CAST({col} AS TEXT) != ''"
                )
                n = c.fetchone()[0]
            except Exception:
                n = 0
            print(f"{col:<28} | {n:>12,} | {meaning}")


if __name__ == "__main__":
    run_report()
