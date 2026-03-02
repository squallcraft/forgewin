#!/usr/bin/env python3
"""
Prueba: selección de partidos de diferentes ligas → análisis → PDF.
Verifica que el flujo (partidos de varias ligas → analyze_matches → generar PDF) funcione
sin depender de Grok/Gemini. El PDF se genera con texto de prueba si no hay propuestas reales.
"""

import os
import sys
from pathlib import Path

# Asegurar que el proyecto está en el path
sys.path.insert(0, str(Path(__file__).resolve().parent))

def main():
    print("=" * 60)
    print("PRUEBA: Partidos de diferentes ligas → Análisis → PDF")
    print("=" * 60)

    from scraper import get_upcoming_matches
    from analyzer import analyze_matches
    from config import LEAGUES

    use_mock = True
    league_codes = ["PL", "PD", "CL"]  # Premier, La Liga, Champions
    league_names = {c: next((n for n, code in LEAGUES.items() if code == c), c) for c in league_codes}

    # 1) Obtener partidos por liga (mock devuelve 5 por liga; ids pueden repetirse entre ligas en mock)
    print("\n1. Obteniendo partidos por liga (use_mock=True)...")
    all_by_league = {}
    for code in league_codes:
        matches = get_upcoming_matches([code], days_ahead=7, use_mock=use_mock)
        all_by_league[code] = matches
        print(f"   - {league_names.get(code, code)}: {len(matches)} partidos")

    # 2) Construir una selección "multi-liga": 2 de PL, 2 de PD, 2 de CL
    # En mock los id pueden repetirse; usamos posición para simular distintos partidos
    selected_match_dicts = []
    selected_fixture_ids = []
    for i, code in enumerate(league_codes):
        matches = all_by_league.get(code, [])
        # Tomar hasta 2 partidos por liga; si el mock repite ids, hacer id único por liga
        for j, m in enumerate(matches[:2]):
            m_copy = dict(m)
            # Asegurar fixture_id único entre ligas (mock puede dar 900000, 900001 para todas)
            m_copy["fixture_id"] = (900000 + i * 100 + j)
            m_copy["league_id"] = code
            m_copy["league_name"] = league_names.get(code, code)
            m_copy["league_code"] = code
            selected_match_dicts.append(m_copy)
            selected_fixture_ids.append(m_copy["fixture_id"])

    print(f"\n2. Selección multi-liga: {len(selected_fixture_ids)} partidos")
    for m in selected_match_dicts:
        print(f"   - [{m.get('league_name')}] {m.get('home_team')} vs {m.get('away_team')} (id={m.get('fixture_id')})")

    # 3) Ejecutar analyze_matches (calcula probs por partido)
    print("\n3. Ejecutando analyze_matches(selected_match_dicts, use_mock=True)...")
    try:
        rows = analyze_matches(selected_match_dicts, use_mock=use_mock)
        print(f"   OK: {len(rows)} filas con probabilidades")
        for r in rows:
            print(f"   - {r.get('home')} vs {r.get('away')} | liga={r.get('league')} | fixture_id={r.get('fixture_id')}")
    except Exception as e:
        print(f"   ERROR en analyze_matches: {e}")
        import traceback
        traceback.print_exc()
        return 1

    # 4) Preparar datos para el PDF (como en la app)
    fixture_to_match = {}
    for r in rows:
        fid = r.get("fixture_id")
        if fid is not None:
            fixture_to_match[fid] = {
                "home_team": r.get("home"),
                "away_team": r.get("away"),
                "date": r.get("date"),
                "league": r.get("league"),
                "league_name": r.get("league"),
            }

    match_ids_for_pdf = [r.get("fixture_id") for r in rows if r.get("fixture_id") is not None]
    if not match_ids_for_pdf:
        match_ids_for_pdf = selected_fixture_ids

    # 5) Generar PDF con propuestas de prueba (sin llamar a Grok/Gemini)
    print("\n4. Generando PDF (propuestas de prueba)...")
    prop_grok = {"grok_analysis": "Análisis de prueba Alfred – partidos de varias ligas.\n\nOK."}
    prop_gemini = {"grok_analysis": "Análisis de prueba Reginald – partidos de varias ligas.\n\nOK."}
    consensus = {"analysis": "Propuesta General 1+2 de prueba – consenso multi-liga.\n\nOK."}

    try:
        from pdf_report import generate_proposal_pdf_three_options
        out_dir = Path(__file__).resolve().parent / "generated_pdfs"
        full_id = "test_multi_league"
        path = generate_proposal_pdf_three_options(
            full_id,
            fixture_to_match,
            match_ids_for_pdf,
            prop_grok,
            prop_gemini,
            consensus=consensus,
            output_dir=str(out_dir),
        )
        print(f"   OK: PDF generado en {path}")
        print(f"   Existe: {os.path.exists(path)}")
    except Exception as e:
        print(f"   ERROR generando PDF: {e}")
        import traceback
        traceback.print_exc()
        return 1

    print("\n" + "=" * 60)
    print("RESULTADO: Selección de partidos de diferentes ligas funciona correctamente.")
    print("  - Partidos de PL, PD y CL combinados en una sola selección.")
    print("  - analyze_matches procesó todos sin error.")
    print("  - El PDF se generó con la lista de partidos multi-liga.")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
