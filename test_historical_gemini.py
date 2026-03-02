#!/usr/bin/env python3
"""
Prueba: consultas sobre la base histórica + Gemini.
- Datos que están en la BD se pasan como contexto.
- Si algo no está en la BD, se indica y Gemini responde con su conocimiento/API.
"""

import sys

from config import GEMINI_API_KEY
from db import (
    get_referee_losses_for_team,
    get_top_teams_by_attendance,
)
from rolling_window import get_current_window_seasons
from gemini_client import ask_gemini_custom


SYSTEM_PROMPT = """Eres un asistente de datos de fútbol. Responde en español de forma clara y breve.

Te voy a hacer 3 preguntas. Para cada una te indico:
- Si hay datos de la base de datos histórica: úsalos para responder.
- Si dice "No disponible en la base de datos": responde usando tu propio conocimiento (o lo que sepas sobre el tema).

Responde las 3 preguntas en orden, numeradas (1, 2, 3), con una respuesta concisa para cada una."""


def main() -> None:
    seasons = get_current_window_seasons()  # últimas 5 temporadas
    print("Temporadas en ventana:", seasons, file=sys.stderr)

    # 1) Árbitro con el que más partidos ha perdido el AC Milan (últimas 5 temporadas)
    # En football-data.co.uk Serie A suele venir como "Milan"
    referee_data = get_referee_losses_for_team(
        team_names=["Milan", "AC Milan"],
        league_id="SA",
        seasons=seasons,
        limit=5,
    )
    if referee_data:
        q1_context = "Datos en la base de datos (árbitro y número de derrotas del Milan/AC Milan):\n" + "\n".join(
            f"- {r['referee']}: {r['losses']} derrotas" for r in referee_data
        )
        q1_answer = f"El árbitro con más derrotas del Milan en la BD es: {referee_data[0]['referee']} ({referee_data[0]['losses']} derrotas)."
    else:
        q1_context = "No disponible en la base de datos (no hay partidos de AC Milan con árbitro en las últimas 5 temporadas, o la liga SA no está cargada)."
        q1_answer = ""

    # 2) Top 3 equipos Premier League con más público
    attendance_data = get_top_teams_by_attendance(
        league_id="PL",
        limit=3,
        seasons=seasons,
    )
    if attendance_data:
        q2_context = "Datos en la base de datos (equipo, asistencia total como local, partidos):\n" + "\n".join(
            f"- {t['team_name']}: {t['total_attendance']:,} espectadores en {t['matches']} partidos"
            for t in attendance_data
        )
        q2_answer = f"Top 3 en la BD: 1) {attendance_data[0]['team_name']}, 2) {attendance_data[1]['team_name'] if len(attendance_data) > 1 else '-'}, 3) {attendance_data[2]['team_name'] if len(attendance_data) > 2 else '-'}."
    else:
        q2_context = "No disponible en la base de datos (no hay datos de asistencia para Premier League en la BD, o la liga PL no está cargada)."
        q2_answer = ""

    # 3) Última vez que AC Milan ganó el Scudetto
    # No guardamos campeones de liga en la BD, solo partidos
    q3_context = "No disponible en la base de datos (la base solo tiene resultados de partidos, no historial de campeones de liga). Responde con tu conocimiento."

    # Construir mensaje de usuario
    user_content = """Preguntas:

1) ¿Con qué árbitro ha perdido más partidos el AC Milan en las últimas 5 temporadas?
""" + q1_context + """

2) ¿Top 3 equipos de la Premier League con más público?
""" + q2_context + """

3) ¿Cuándo fue la última vez que el AC Milan ganó el Scudetto?
""" + q3_context + """

Responde las 3 preguntas usando los datos de la BD cuando estén disponibles; si no, usa tu conocimiento."""

    if not referee_data and not attendance_data:
        print("Aviso: La BD no devolvió datos de árbitro ni asistencia. Carga historial desde CSV (football-data.co.uk) para que las preguntas 1 y 2 usen la BD.", file=sys.stderr)
    print("\n--- Contexto enviado a Gemini ---", file=sys.stderr)
    print(user_content[:1500] + ("..." if len(user_content) > 1500 else ""), file=sys.stderr)
    print("---\n", file=sys.stderr)

    if not GEMINI_API_KEY:
        print("ERROR: GEMINI_API_KEY (o GOOGLE_API_KEY) no configurada en .env. No se puede llamar a Gemini.")
        print("\nResumen de lo que la BD devolvió:")
        print("1) Árbitro pérdidas Milan:", referee_data[:1] if referee_data else "Sin datos")
        print("2) Top 3 PL asistencia:", attendance_data if attendance_data else "Sin datos")
        print("3) Scudetto: no está en la BD (responder con conocimiento externo).")
        sys.exit(1)

    response = ask_gemini_custom(SYSTEM_PROMPT, user_content, timeout=90)
    print(response or "(Sin respuesta de Gemini)")


if __name__ == "__main__":
    main()
