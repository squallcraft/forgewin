#!/usr/bin/env python3
"""
Mismas preguntas que query_pl_season_gemini.py pero respondidas por Grok (Alfred).
Usa la BD histórica PL temporada 2024 y el mismo contexto.
"""

import sys

from config import GROK_API_KEY, GROK_MODEL_FAST
from db import (
    get_team_season_wins,
    get_league_standings,
    get_top_teams_by_attendance,
)
from grok_client import ask_grok_custom


LEAGUE_ID = "PL"
SEASON = 2024

SYSTEM_PROMPT = """Eres un asistente de datos de fútbol. Responde en español de forma clara y concisa.
Te voy a dar datos extraídos de la base de datos de la Premier League temporada 2023/24 (season 2024).
Responde las 4 preguntas en orden, numeradas (1, 2, 3, 4), usando solo los datos proporcionados cuando estén disponibles.
Si para alguna pregunta no hay datos en el bloque, indícalo brevemente y responde con tu conocimiento si lo tienes."""


def main() -> None:
    # Mismo contexto que query_pl_season_gemini.py
    arsenal_wins = get_team_season_wins("Arsenal", LEAGUE_ID, SEASON)
    standings = get_league_standings(LEAGUE_ID, SEASON)
    top3 = standings[:3] if standings else []
    top3_points_sum = sum(t["points"] for t in top3)
    attendance_top = get_top_teams_by_attendance(LEAGUE_ID, limit=3, seasons=[SEASON])
    champion = standings[0]["team_name"] if standings else None

    context = f"""Datos de la base de datos (Premier League, temporada 2023/24, season={SEASON}):

1) Victorias de Arsenal: {arsenal_wins} partidos ganados.

2) Clasificación (top 3 por puntos):
{chr(10).join(f"   - {t['team_name']}: {t['points']} pts (W{t['wins']} D{t['draws']} L{t['losses']})" for t in top3)}
   Suma de puntos del top 3: {top3_points_sum}.

3) Top 3 equipos por asistencia total (como local):
{"   " + chr(10).join(f"   - {t['team_name']}: {t['total_attendance']:,} espectadores ({t['matches']} partidos)" for t in attendance_top) if attendance_top else "   (No hay datos de asistencia en la BD para esta temporada.)"}

4) Campeón de la temporada (1º en clasificación): {champion or "(no calculado)"}.
"""

    questions = """
Preguntas a responder:

1) ¿Cuántos partidos ganó Arsenal en esa temporada?
2) ¿Cuántos puntos sumaron en total el top 3 de la Premier League esa temporada?
3) ¿Cuáles son los 3 equipos de la Premier con más público (asistencia)?
4) ¿Quién fue el campeón de la Premier League esa temporada?

Responde las 4 preguntas en orden, usando los datos de la BD cuando estén disponibles."""

    user_content = context.strip() + "\n\n" + questions.strip()

    if not GROK_API_KEY:
        print("ERROR: XAI_API_KEY o GROK_API_KEY no configurada en .env")
        print("\n--- Datos de la BD (respuestas sin Grok) ---")
        print(context)
        sys.exit(1)

    # ask_grok_custom usa GROK_MODEL_FAST por defecto
    print(f"[Modelo: {GROK_MODEL_FAST}]\n")
    response = ask_grok_custom(SYSTEM_PROMPT, user_content, timeout=90)
    print(response or "(Sin respuesta de Grok)")


if __name__ == "__main__":
    main()
