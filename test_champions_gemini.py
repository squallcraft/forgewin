#!/usr/bin/env python3
"""
Prueba: campeones usando get_league_standings (con fallback a historical_matches) vs respuesta de Gemini.

1. Usa get_league_standings() para obtener campeones (1º clasificado):
   - Premier League 2000-2005
   - Bundesliga 2000-2005
   - Serie A 2020-2024

   (Para 2000-2005 league_standings suele estar vacía → fallback a historical_matches)

2. Pasa esos datos exactos a Gemini como contexto.

3. Pide a Gemini que responda las 3 preguntas usando SOLO esos datos.

4. Compara la respuesta de Gemini con nuestros datos.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from db import get_league_standings, init_db
from gemini_client import ask_gemini_custom


def main() -> int:
    init_db()

    # Datos vía get_league_standings (league_standings o fallback a historical_matches)
    pl_champs = []
    for season in range(2000, 2006):  # 2000, 2001, 2002, 2003, 2004, 2005
        st = get_league_standings("PL", season)
        if st:
            pl_champs.append((season, st[0]["team_name"], st[0]["points"]))
        else:
            pl_champs.append((season, "(sin datos)", 0))

    bl1_champs = []
    for season in range(2000, 2006):
        st = get_league_standings("BL1", season)
        if st:
            bl1_champs.append((season, st[0]["team_name"], st[0]["points"]))
        else:
            bl1_champs.append((season, "(sin datos)", 0))

    sa_champs = []
    for season in range(2020, 2025):  # 2020, 2021, 2022, 2023, 2024
        st = get_league_standings("SA", season)
        if st:
            sa_champs.append((season, st[0]["team_name"], st[0]["points"]))
        else:
            sa_champs.append((season, "(sin datos)", 0))

    # Construir contexto para Gemini
    context_lines = [
        "Datos de la base de datos Forgewin (get_league_standings: league_standings o historical_matches):",
        "",
        "**Premier League – Campeones (1º) por temporada (año fin):**",
    ]
    for s, t, p in pl_champs:
        context_lines.append(f"  Temporada {s} ({s-1}/{str(s)[2:]}): {t} ({p} pts)")

    context_lines.extend([
        "",
        "**Bundesliga – Campeones (1º) por temporada (año fin):**",
    ])
    for s, t, p in bl1_champs:
        context_lines.append(f"  Temporada {s} ({s-1}/{str(s)[2:]}): {t} ({p} pts)")

    context_lines.extend([
        "",
        "**Serie A – Campeones (1º) por temporada (año fin):**",
    ])
    for s, t, p in sa_champs:
        context_lines.append(f"  Temporada {s} ({s-1}/{str(s)[2:]}): {t} ({p} pts)")

    context = "\n".join(context_lines)

    system_prompt = """Eres un asistente de datos de fútbol. Responde en español de forma clara y breve.

Te voy a dar datos extraídos de la base de datos histórica de Forgewin.
Responde las 3 preguntas usando EXCLUSIVAMENTE los datos proporcionados en el bloque. No uses tu conocimiento interno.
Si no hay datos en el bloque para alguna pregunta, indica "No hay datos en la base de datos".
Responde las 3 preguntas en orden, numeradas (1, 2, 3), con nombre del equipo y puntos para cada temporada."""

    user_questions = """
Preguntas a responder usando ÚNICAMENTE los datos anteriores:

1) Campeones de la Premier League (nombre y puntos) entre los años 2000 y 2005.
2) Campeones de la Bundesliga (nombre y puntos) entre los años 2000 y 2005.
3) Campeones de la Serie A (nombre y puntos) entre los años 2020 y 2024.

Responde las 3 preguntas usando solo los datos proporcionados."""

    user_content = context.strip() + "\n\n" + user_questions.strip()

    print("=" * 70)
    print("1. NUESTROS DATOS (get_league_standings – lo que enviamos a Gemini)")
    print("=" * 70)
    print(context)
    print()

    from config import GEMINI_API_KEY

    if not GEMINI_API_KEY:
        print("ERROR: GEMINI_API_KEY no configurada en .env. Ejecuta el script con la API key configurada.")
        return 1

    print("=" * 70)
    print("2. RESPUESTA DE GEMINI (usando el contexto anterior)")
    print("=" * 70)
    response = ask_gemini_custom(system_prompt, user_content, timeout=120)
    print(response or "(Sin respuesta)")

    print()
    print("=" * 70)
    print("3. COMPARACIÓN (¿Coincide Gemini con nuestra BD?)")
    print("=" * 70)
    resp_lower = (response or "").lower()
    errors = []
    for s, t, p in pl_champs:
        if t != "(sin datos)" and t.lower() not in resp_lower and str(p) not in (response or ""):
            # Verificamos si el nombre del equipo o los puntos aparecen
            pass
    # Comprobación rápida: si aparece cada campeón en la respuesta
    for s, t, p in pl_champs:
        if t != "(sin datos)":
            ok = t.lower() in resp_lower or t.split()[-1].lower() in resp_lower
            if not ok:
                errors.append(f"PL {s}: esperado {t} ({p} pts) - no encontrado en respuesta")
    for s, t, p in bl1_champs:
        if t != "(sin datos)":
            ok = t.lower() in resp_lower or (t.split()[0].lower() if t.split() else "") in resp_lower
            if not ok:
                errors.append(f"BL1 {s}: esperado {t} ({p} pts) - no encontrado en respuesta")
    for s, t, p in sa_champs:
        if t != "(sin datos)":
            ok = t.lower() in resp_lower or t.split()[-1].lower() in resp_lower
            if not ok:
                errors.append(f"SA {s}: esperado {t} ({p} pts) - no encontrado en respuesta")

    if not errors:
        print("Los campeones que enviamos parecen estar citados en la respuesta de Gemini.")
    else:
        print("Posibles discrepancias (campeones no encontrados en respuesta):")
        for e in errors[:10]:
            print("  -", e)

    return 0


if __name__ == "__main__":
    sys.exit(main())
