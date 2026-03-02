#!/usr/bin/env python3
"""
Test: Grok y Gemini responden sobre árbitros y derrotas de Barcelona/Real Madrid.
Preguntas:
1) ¿Qué árbitro participó en más derrotas del Barcelona durante temporadas 2020-2025?
2) Ese árbitro, ¿en cuántas derrotas del Real Madrid participó?
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from db import get_referee_losses_for_team

PD = "PD"  # La Liga
SEASONS = [2020, 2021, 2022, 2023, 2024, 2025]
BARCELONA_NAMES = ["Barcelona", "FC Barcelona"]
MADRID_NAMES = ["Real Madrid"]


def build_context() -> str:
    """Construye contexto desde la BD."""
    barca = get_referee_losses_for_team(BARCELONA_NAMES, PD, SEASONS, limit=15)
    madrid = get_referee_losses_for_team(MADRID_NAMES, PD, SEASONS, limit=30)

    parts = [
        "**Origen de los datos:** Base de datos ForgeWin, tabla historical_matches "
        "(columnas referee, ftr, home_team_name, away_team_name). La Liga = PD.",
        "",
        "**Árbitros con más derrotas del Barcelona (La Liga, temporadas 2020-2025):**",
    ]
    if barca:
        for r in barca:
            parts.append(f"  - {r['referee']}: {r['losses']} derrotas")
        parts.append(f"  → El árbitro con más derrotas del Barcelona: {barca[0]['referee']} ({barca[0]['losses']} derrotas).")
    else:
        parts.append("  (Sin datos)")
    parts.append("")
    parts.append("**Árbitros con más derrotas del Real Madrid (La Liga, temporadas 2020-2025):**")
    if madrid:
        for r in madrid:
            parts.append(f"  - {r['referee']}: {r['losses']} derrotas")
    else:
        parts.append("  (Sin datos)")

    return "\n".join(parts)


def main() -> int:
    from config import GROK_API_KEY, GEMINI_API_KEY
    from grok_client import ask_grok_custom
    from gemini_client import ask_gemini_custom

    context = build_context()
    user_content = f"""Datos de la base de datos ForgeWin:

{context.strip()}

Responde aquí, usando SOLO los datos de arriba:

Pregunta 1: ¿Qué árbitro participó en más derrotas del Barcelona durante las temporadas 2020 a 2025?

Pregunta 2: Según la respuesta a la pregunta 1, ese árbitro, ¿en cuántas derrotas del Real Madrid participó?

Responde las 2 preguntas numeradas."""

    print("=" * 70)
    print("CONTEXTO ENVIADO")
    print("=" * 70)
    print(context)
    print()
    print("=" * 70)
    print("PREGUNTAS")
    print("=" * 70)
    print("1) ¿Qué árbitro participó en más derrotas del Barcelona (2020-2025)?")
    print("2) Ese árbitro, ¿en cuántas derrotas del Real Madrid participó?")
    print()

    if not GROK_API_KEY and not GEMINI_API_KEY:
        print("ERROR: Configura GROK_API_KEY y/o GEMINI_API_KEY en .env")
        return 1

    if GROK_API_KEY:
        print("=" * 70)
        print("RESPUESTA GROK (Alfred) – responde aquí")
        print("=" * 70)
        try:
            r = ask_grok_custom(
                "Responde en español de forma clara. Usa SOLO los datos proporcionados.",
                user_content,
                timeout=120,
            )
            print(r or "(Sin respuesta)")
        except Exception as e:
            print(f"Error: {e}")
        print()

    if GEMINI_API_KEY:
        print("=" * 70)
        print("RESPUESTA GEMINI (Reginald) – responde aquí")
        print("=" * 70)
        try:
            r = ask_gemini_custom(
                "Responde en español de forma clara. Usa SOLO los datos proporcionados.",
                user_content,
                timeout=120,
            )
            print(r or "(Sin respuesta)")
        except Exception as e:
            print(f"Error: {e}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
