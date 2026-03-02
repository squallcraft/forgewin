#!/usr/bin/env python3
"""
Prueba: Gemini responde cuántas tarjetas hubo en Serie A en el último mes.
        Grok responde cuántos goles hubo en el último partido del Arsenal.
Datos desde historical_matches.
"""

import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from db import get_connection, init_db

init_db()


def get_serie_a_cards_last_month():
    """Tarjetas (amarillas + rojas) en Serie A en el último mes (últimos 30 días con datos)."""
    with get_connection() as conn:
        c = conn.cursor()
        c.execute(
            """SELECT MAX(date) FROM historical_matches WHERE league_id = 'SA'"""
        )
        max_date_row = c.fetchone()
    if not max_date_row or not max_date_row[0]:
        return None, None, []
    try:
        max_d = date.fromisoformat(max_date_row[0])
    except ValueError:
        return None, None, []
    date_from = (max_d - timedelta(days=30)).isoformat()
    date_to = max_d.isoformat()
    with get_connection() as conn:
        c = conn.cursor()
        c.execute(
            """SELECT date,
                      COALESCE(home_yellow, 0) + COALESCE(away_yellow, 0) AS amarillas,
                      COALESCE(home_red, 0) + COALESCE(away_red, 0) AS rojas,
                      home_team_name, away_team_name
               FROM historical_matches
               WHERE league_id = 'SA' AND date >= ? AND date <= ?
               ORDER BY date DESC""",
            (date_from, date_to),
        )
        rows = c.fetchall()
    if not rows:
        return None, None, []
    total_yellow = sum(r[1] for r in rows)
    total_red = sum(r[2] for r in rows)
    sample = [(r[0], r[1], r[2], r[3], r[4]) for r in rows[:15]]
    return total_yellow, total_red, sample


def get_arsenal_last_match():
    """Último partido del Arsenal: fecha, equipos, goles."""
    with get_connection() as conn:
        c = conn.cursor()
        c.execute(
            """SELECT date, home_team_name, away_team_name, home_goals, away_goals, league_id
               FROM historical_matches
               WHERE LOWER(home_team_name) LIKE '%arsenal%' OR LOWER(away_team_name) LIKE '%arsenal%'
               ORDER BY date DESC
               LIMIT 1"""
        )
        return c.fetchone()


def main():
    from config import GEMINI_API_KEY, GROK_API_KEY
    from gemini_client import ask_gemini_custom
    from grok_client import ask_grok_custom

    print("=" * 60)
    print("PRUEBA 1: Gemini – Tarjetas en Serie A (último mes)")
    print("=" * 60)

    total_y, total_r, sample = get_serie_a_cards_last_month()
    if total_y is None or not sample:
        print("No hay partidos de Serie A en el último mes en la BD.")
    else:
        context_lines = [
            f"Partidos de Serie A en los últimos ~30 días: {len(sample)} partidos (muestra hasta 15).",
            "Por partido: fecha, amarillas (total del partido), rojas (total del partido), local, visitante.",
            "",
        ]
        for r in sample:
            context_lines.append(f"  {r[0]} | amarillas: {r[1]}, rojas: {r[2]} | {r[3]} vs {r[4]}")
        context_lines.append("")
        context_lines.append(f"Totales en la BD para ese periodo: {total_y} amarillas, {total_r} rojas.")

        context = "\n".join(context_lines)
        user_content = (
            "Datos de la base de datos (Serie A, último mes):\n\n" + context + "\n\n"
            "Pregunta: ¿Cuántas tarjetas (amarillas y rojas) tuvo la Serie A en total en el último mes? "
            "Responde en una frase con los números."
        )
        if GEMINI_API_KEY:
            try:
                resp = ask_gemini_custom(
                    "Eres un asistente de datos de fútbol. Responde en español de forma clara y breve usando solo los datos proporcionados.",
                    user_content,
                    timeout=60,
                )
                print(resp or "(Sin respuesta)")
            except Exception as e:
                print(f"Error Gemini: {e}")
        else:
            print("GEMINI_API_KEY no configurada en .env")

    print()
    print("=" * 60)
    print("PRUEBA 2: Grok – Goles en el último partido del Arsenal")
    print("=" * 60)

    row = get_arsenal_last_match()
    if not row:
        print("No se encontró último partido del Arsenal en la BD.")
    else:
        date_str, home, away, hg, ag, league = row
        context = (
            f"Último partido del Arsenal en la BD: {date_str}. "
            f"{home} vs {away} ({league or 'N/A'}). Resultado: {hg} - {ag} (local - visitante)."
        )
        user_content = (
            "Datos de la base de datos:\n\n" + context + "\n\n"
            "Pregunta: ¿Cuántos goles hubo en total en el último partido del Arsenal? Responde en una frase."
        )
        if GROK_API_KEY:
            try:
                resp = ask_grok_custom(
                    "Eres un asistente de datos de fútbol. Responde en español de forma clara y breve usando solo los datos proporcionados.",
                    user_content,
                    timeout=60,
                )
                print(resp or "(Sin respuesta)")
            except Exception as e:
                print(f"Error Grok: {e}")
        else:
            print("GROK_API_KEY / XAI_API_KEY no configurada en .env")

    print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
