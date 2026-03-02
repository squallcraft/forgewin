#!/usr/bin/env python3
"""
Prueba: misma batería de preguntas sobre la BD histórica, respondidas por Grok y por Gemini.
Fuente de datos: historical_matches (clasificaciones, puntos, tarjetas amarillas).
Compara respuestas de ambos modelos usando exactamente el mismo contexto.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from db import get_league_standings, get_team_yellow_cards_ranking

# Ligas: PL=Premier League, SA=Serie A, PD=La Liga, CL=Champions League
LEAGUES = {"PL": "Premier League", "SA": "Serie A", "PD": "La Liga", "CL": "Champions League"}


def build_context_from_db() -> str:
    """Construye el bloque de datos desde la BD histórica para las 5 preguntas."""
    parts = []

    # 1–3: Últimos 3 campeones de PL, SA y CL (temporadas 2024, 2023, 2022 = 1º de cada temporada). Fuente: league_standings.
    for league_id, league_name in [("PL", "Premier League"), ("SA", "Serie A"), ("CL", "Champions League")]:
        champs = []
        for season in (2024, 2023, 2022):
            standings = get_league_standings(league_id, season)
            if standings:
                champs.append(f"  Temporada {season} (ej. {season-1}/{str(season)[2:]}): {standings[0]['team_name']} ({standings[0]['points']} pts)")
            else:
                champs.append(f"  Temporada {season}: (sin datos en BD)")
        parts.append(f"**{league_name}** – Campeón (1º clasificación) por temporada:\n" + "\n".join(champs))

    # 4: Puntos del top 3 de PL, SA y La Liga temporada 2024 (fuente: league_standings)
    parts.append("\n**Puntos del top 3 – temporada 2024 (2023/24):**")
    for league_id, league_name in [("PL", "Premier League"), ("SA", "Serie A"), ("PD", "La Liga")]:
        standings = get_league_standings(league_id, 2024)
        top3 = standings[:3] if standings else []
        if top3:
            parts.append(f"  {league_name}: " + " | ".join(f"{t['team_name']} {t['points']} pts" for t in top3))
        else:
            parts.append(f"  {league_name}: (sin datos en BD)")

    # 5: Equipo con más tarjetas amarillas en PL y Serie A temporada 2022
    parts.append("\n**Tarjetas amarillas – temporada 2022 (2021/22), equipo con más:**")
    for league_id, league_name in [("PL", "Premier League"), ("SA", "Serie A")]:
        ranking = get_team_yellow_cards_ranking(league_id, 2022, limit=1)
        if ranking:
            parts.append(f"  {league_name}: {ranking[0]['team_name']} ({ranking[0]['yellow_cards']} amarillas)")
        else:
            parts.append(f"  {league_name}: (sin datos de amarillas en BD)")

    return "\n".join(parts)


SYSTEM_PROMPT = """Eres un asistente de datos de fútbol. Responde en español de forma clara y concisa.
Te voy a dar datos extraídos de la base de datos histórica de Forgewin (partidos, clasificaciones, tarjetas).
Responde las 5 preguntas en orden, numeradas (1, 2, 3, 4, 5). Usa SOLO los datos proporcionados en el bloque.
Para la pregunta 4, haz un listado con el equipo a la izquierda y el puntaje a la derecha.
Si para alguna pregunta no hay datos en el bloque, indícalo brevemente."""

USER_QUESTIONS = """
Preguntas a responder usando los datos de la BD anteriores:

1) Últimos 3 campeones de la Premier League (orden: más reciente primero).
2) Últimos 3 campeones de la Serie A (orden: más reciente primero).
3) Últimos 3 campeones de la Champions League (orden: más reciente primero).
4) Puntos logrados por los top 3 de Premier League, Serie A y La Liga, temporada 2024. Listado: equipo a la izquierda, puntaje a la derecha.
5) Equipo con mayor cantidad de tarjetas amarillas en Premier League y en Serie A, temporada 2022.

Responde las 5 preguntas en orden, usando solo los datos proporcionados."""


def main() -> int:
    from config import GROK_API_KEY, GEMINI_API_KEY
    from grok_client import ask_grok_custom
    from gemini_client import ask_gemini_custom

    context = build_context_from_db()
    user_content = "Datos de la base de datos histórica Forgewin:\n\n" + context.strip() + "\n\n" + USER_QUESTIONS.strip()

    print("=" * 60)
    print("CONTEXTO ENVIADO (desde BD histórica)")
    print("=" * 60)
    print(context)
    print()
    print("=" * 60)
    print("PREGUNTAS")
    print("=" * 60)
    print(USER_QUESTIONS.strip())
    print()

    has_grok = bool(GROK_API_KEY)
    has_gemini = bool(GEMINI_API_KEY)
    if not has_grok and not has_gemini:
        print("ERROR: Configura GROK_API_KEY (XAI) y/o GEMINI_API_KEY en .env")
        return 1

    if has_grok:
        print("=" * 60)
        print("RESPUESTA GROK (Alfred)")
        print("=" * 60)
        try:
            resp_grok = ask_grok_custom(SYSTEM_PROMPT, user_content, timeout=120)
            print(resp_grok or "(Sin respuesta)")
        except Exception as e:
            print(f"Error: {e}")
        print()

    if has_gemini:
        print("=" * 60)
        print("RESPUESTA GEMINI (Reginald)")
        print("=" * 60)
        try:
            resp_gemini = ask_gemini_custom(SYSTEM_PROMPT, user_content, timeout=120)
            print(resp_gemini or "(Sin respuesta)")
        except Exception as e:
            print(f"Error: {e}")
        print()

    return 0


if __name__ == "__main__":
    sys.exit(main())
