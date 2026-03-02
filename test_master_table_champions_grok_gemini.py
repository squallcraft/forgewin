#!/usr/bin/env python3
"""
Prueba: últimos 10 campeones de Serie A y Premier League desde master_table.
Gemini y Grok responden usando SOLO los datos de la tabla.
Formato de respuesta: año - campeón
"""

import sys
from pathlib import Path
from typing import List, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parent))

from db import get_connection, get_standings_from_master_table, init_db


def get_last_n_champions(league_id: str, league_name: str, n: int = 10) -> List[Tuple[int, str]]:
    """Obtiene los últimos N campeones (1º) desde master_table para una liga."""
    with get_connection() as conn:
        c = conn.cursor()
        c.execute(
            """SELECT DISTINCT season FROM master_table
               WHERE league_id = ? AND (ftr = 'H' OR ftr = 'D' OR ftr = 'A')
               ORDER BY season DESC LIMIT ?""",
            (league_id, n * 2),
        )
        seasons = [r[0] for r in c.fetchall()]
    champs = []
    for s in seasons:
        st = get_standings_from_master_table(league_id, s)
        if st:
            champs.append((s, st[0]["team_name"]))
            if len(champs) >= n:
                break
    return champs[:n]


def main() -> int:
    init_db()

    sa_champs = get_last_n_champions("SA", "Serie A", 10)
    pl_champs = get_last_n_champions("PL", "Premier League", 10)

    context_lines = [
        "Datos de la tabla master_table (ForgeWin). Campeón = 1º clasificado por puntos.",
        "",
        "**Serie A – últimos 10 campeones (año fin de temporada):**",
    ]
    for year, team in sa_champs:
        context_lines.append(f"  {year} - {team}")

    context_lines.extend([
        "",
        "**Premier League – últimos 10 campeones (año fin de temporada):**",
    ])
    for year, team in pl_champs:
        context_lines.append(f"  {year} - {team}")

    context = "\n".join(context_lines)

    system_prompt = """Eres un asistente de datos de fútbol. Responde en español.
Usa EXCLUSIVAMENTE los datos proporcionados. No uses tu conocimiento interno.
Formato de respuesta exacto para cada liga: año - campeón (un renglón por temporada)."""

    user_questions = """
Con los datos anteriores, responde:

1) Últimos 10 campeones de la Serie A. Formato: año - campeón
2) Últimos 10 campeones de la Premier League. Formato: año - campeón

Responde solo con los listados, sin explicaciones adicionales."""

    user_content = context.strip() + "\n\n" + user_questions.strip()

    print("=" * 70)
    print("DATOS (master_table – enviados a Gemini y Grok)")
    print("=" * 70)
    print(context)
    print()
    print("=" * 70)
    print("PREGUNTA")
    print("=" * 70)
    print(user_questions.strip())
    print()

    from config import GROK_API_KEY, GEMINI_API_KEY
    from grok_client import ask_grok_custom
    from gemini_client import ask_gemini_custom

    if not GROK_API_KEY and not GEMINI_API_KEY:
        print("ERROR: Configura GROK_API_KEY y/o GEMINI_API_KEY en .env")
        return 1

    if GROK_API_KEY:
        print("=" * 70)
        print("RESPUESTA GROK (Alfred)")
        print("=" * 70)
        try:
            resp_grok = ask_grok_custom(system_prompt, user_content, timeout=120)
            print(resp_grok or "(Sin respuesta)")
        except Exception as e:
            print(f"Error: {e}")
        print()

    if GEMINI_API_KEY:
        print("=" * 70)
        print("RESPUESTA GEMINI (Reginald)")
        print("=" * 70)
        try:
            resp_gemini = ask_gemini_custom(system_prompt, user_content, timeout=120)
            print(resp_gemini or "(Sin respuesta)")
        except Exception as e:
            print(f"Error: {e}")
        print()

    return 0


if __name__ == "__main__":
    sys.exit(main())
