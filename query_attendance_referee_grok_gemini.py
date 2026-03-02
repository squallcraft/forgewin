#!/usr/bin/env python3
"""
Prueba: Grok y Gemini responden usando datos de attendance y referee de la BD.
Fuente: historical_matches (columnas attendance y referee).
Preguntas:
- Asistencia total Bundesliga 2021-2022 y 2022-2023; cuál tuvo más.
- Qué árbitro de Serie A dirigió más partidos.
- Qué árbitro(s) dirigió el Derby della Madonnina (Inter vs Milan) temporada 2023-2024.
Las respuestas se imprimen aquí (stdout).
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from db import (
    get_total_attendance_for_league_season,
    get_referee_match_counts_by_league,
    get_matches_between_teams,
)

# Bundesliga = BL1. Temporada 2021-2022 = season 2022, 2022-2023 = season 2023.
BUNDESLIGA = "BL1"
SERIE_A = "SA"
# Derby della Madonnina: Inter vs Milan (variantes de nombre en BD)
INTER_NAMES = ["Inter", "FC Internazionale", "Internazionale", "Inter Milan"]
MILAN_NAMES = ["Milan", "AC Milan"]


def build_context() -> str:
    """Construye el bloque de datos desde la BD para las preguntas."""
    parts = [
        "**Origen de los datos:** Base de datos ForgeWin, tabla historical_matches. "
        "La información de asistencia está en la columna 'attendance' (espectadores por partido). "
        "El árbitro está en la columna 'referee'. Los datos pueden estar incompletos (solo partidos con dato cargado).",
        "",
    ]

    # Asistencia total Bundesliga 2021-22 y 2022-23
    att_2122 = get_total_attendance_for_league_season(BUNDESLIGA, 2022)
    att_2223 = get_total_attendance_for_league_season(BUNDESLIGA, 2023)
    parts.append("**Bundesliga – Asistencia total por temporada (suma de attendance de todos los partidos con dato):**")
    parts.append(f"  Temporada 2021-2022 (season 2022): {att_2122:,} espectadores" if att_2122 is not None else "  Temporada 2021-2022: (sin datos de asistencia en BD)")
    parts.append(f"  Temporada 2022-2023 (season 2023): {att_2223:,} espectadores" if att_2223 is not None else "  Temporada 2022-2023: (sin datos de asistencia en BD)")
    if att_2122 is not None and att_2223 is not None:
        parts.append(f"  → La temporada con más asistencia fue: {'2022-2023' if att_2223 >= att_2122 else '2021-2022'} ({max(att_2122, att_2223):,} espectadores).")
    parts.append("")

    # Árbitro Serie A que más partidos dirigió
    ref_counts = get_referee_match_counts_by_league(SERIE_A, season=None, limit=10)
    parts.append("**Serie A – Árbitros por número de partidos dirigidos (todos los partidos en BD con referee):**")
    if ref_counts:
        for r in ref_counts:
            parts.append(f"  - {r['referee']}: {r['matches']} partidos")
        parts.append(f"  → El árbitro que más partidos dirigió en la BD es: {ref_counts[0]['referee']} ({ref_counts[0]['matches']} partidos).")
    else:
        parts.append("  (No hay datos de árbitros en BD para Serie A)")
    parts.append("")

    # Derby della Madonnina 2023-2024
    derby = get_matches_between_teams(SERIE_A, INTER_NAMES, MILAN_NAMES, 2024)
    parts.append("**Derby della Madonnina (Inter vs Milan) – Temporada 2023-2024 (season 2024):**")
    if derby:
        for m in derby:
            ref = m.get("referee") or "(sin árbitro en BD)"
            parts.append(f"  {m['date']}: {m['home_team_name']} vs {m['away_team_name']} — Árbitro: {ref}")
        refs = list({m.get("referee") for m in derby if m.get("referee")})
        parts.append(f"  → Árbitro(s) que dirigió/dirigieron los partidos del derby: {', '.join(refs) if refs else '(no hay árbitro registrado en BD)'}.")
    else:
        parts.append("  (No hay partidos del derby en BD para esa temporada)")
    parts.append("")

    return "\n".join(parts)


SYSTEM_PROMPT = """Eres un asistente de datos de fútbol. Responde en español de forma clara y concisa.
Te doy datos extraídos de la base de datos de ForgeWin (asistencia y árbitros en historical_matches).
Responde las preguntas en orden, usando SOLO los datos proporcionados. Si algo no está en el bloque, dilo brevemente."""

USER_PROMPT_TEMPLATE = """
Datos de la base de datos (attendance y referee en historical_matches):

{context}

Responde aquí, en este mismo mensaje, las siguientes preguntas usando SOLO los datos de arriba:

1) Asistencia total de la temporada 2021-2022 de Bundesliga.
2) Asistencia total de la temporada 2022-2023 de Bundesliga.
3) ¿Cuál temporada tuvo más asistencia?

4) ¿Qué árbitro de la Serie A dirigió más partidos?

5) ¿Qué árbitro(s) dirigió los partidos del Derby della Madonnina (Inter vs Milan) temporada 2023-2024?

Responde las 5 preguntas en orden, numeradas."""


def main() -> int:
    from config import GROK_API_KEY, GEMINI_API_KEY
    from grok_client import ask_grok_custom
    from gemini_client import ask_gemini_custom

    context = build_context()
    user_content = USER_PROMPT_TEMPLATE.format(context=context.strip())

    print("=" * 70)
    print("CONTEXTO ENVIADO (desde BD – attendance y referee)")
    print("=" * 70)
    print(context)
    print()
    print("=" * 70)
    print("PREGUNTAS ENVIADAS A GROK Y GEMINI")
    print("=" * 70)
    print("1) Asistencia total Bundesliga 2021-2022")
    print("2) Asistencia total Bundesliga 2022-2023")
    print("3) ¿Cuál temporada tuvo más asistencia?")
    print("4) ¿Qué árbitro de la Serie A dirigió más partidos?")
    print("5) ¿Qué árbitro(s) dirigió el Derby della Madonnina 2023-2024?")
    print()

    has_grok = bool(GROK_API_KEY)
    has_gemini = bool(GEMINI_API_KEY)
    if not has_grok and not has_gemini:
        print("ERROR: Configura GROK_API_KEY (XAI) y/o GEMINI_API_KEY en .env")
        return 1

    if has_grok:
        print("=" * 70)
        print("RESPUESTA GROK (Alfred) – responde aquí")
        print("=" * 70)
        try:
            resp = ask_grok_custom(SYSTEM_PROMPT, user_content, timeout=120)
            print(resp or "(Sin respuesta)")
        except Exception as e:
            print(f"Error: {e}")
        print()

    if has_gemini:
        print("=" * 70)
        print("RESPUESTA GEMINI (Reginald) – responde aquí")
        print("=" * 70)
        try:
            resp = ask_gemini_custom(SYSTEM_PROMPT, user_content, timeout=120)
            print(resp or "(Sin respuesta)")
        except Exception as e:
            print(f"Error: {e}")
        print()

    return 0


if __name__ == "__main__":
    sys.exit(main())
