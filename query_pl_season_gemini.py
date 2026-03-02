#!/usr/bin/env python3
"""
Consulta la BD histórica (PL temporada 2024) y pide a Gemini que responda
las preguntas usando esos datos. Liga descargada y cargada previamente.
"""

import sys

from config import GEMINI_API_KEY, GEMINI_MODEL
from db import (
    get_team_season_wins,
    get_league_standings,
    get_top_teams_by_attendance,
)
from gemini_client import ask_gemini_custom


LEAGUE_ID = "PL"
SEASON = 2024

SYSTEM_PROMPT = """Eres un asistente de datos de fútbol. Responde en español de forma clara y concisa.
Te voy a dar datos extraídos de la base de datos de la Premier League temporada 2023/24 (season 2024).
Responde las 4 preguntas en orden, numeradas (1, 2, 3, 4), usando solo los datos proporcionados cuando estén disponibles.
Si para alguna pregunta no hay datos en el bloque, indícalo brevemente y responde con tu conocimiento si lo tienes."""


def main() -> None:
    # 1) Victorias de Arsenal
    arsenal_wins = get_team_season_wins("Arsenal", LEAGUE_ID, SEASON)

    # 2) Clasificación y puntos del top 3
    standings = get_league_standings(LEAGUE_ID, SEASON)
    top3 = standings[:3] if standings else []
    top3_points_sum = sum(t["points"] for t in top3)
    top3_names = [t["team_name"] for t in top3]

    # 3) Top 3 por asistencia (puede estar vacío si no hay columna attendance en CSV)
    attendance_top = get_top_teams_by_attendance(LEAGUE_ID, limit=3, seasons=[SEASON])

    # 4) Campeón = primero en la clasificación
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

    if not GEMINI_API_KEY:
        print("ERROR: GEMINI_API_KEY no configurada en .env")
        print("\n--- Datos de la BD (respuestas sin Gemini) ---")
        print(context)
        sys.exit(1)

    print(f"[Modelo: {GEMINI_MODEL}]\n")
    response = ask_gemini_custom(SYSTEM_PROMPT, user_content, timeout=60)
    print(response or "(Sin respuesta de Gemini)")


if __name__ == "__main__":
    main()
