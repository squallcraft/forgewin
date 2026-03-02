#!/usr/bin/env python3
"""
Prueba interna V3: análisis del partido Inter de Milan vs Bodo (Bodø/Glimt)
con Gemini y Grok por separado. Escribe un documento MD por modelo.
Ejecutar desde la raíz del proyecto:
  python scripts/run_v3_test_inter_bodo.py
  o: cd football-betting-analysis && source venv/bin/activate && python scripts/run_v3_test_inter_bodo.py
"""

import logging
import sys
from pathlib import Path

# Raíz del proyecto
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# Cargar .env si existe
try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
except ImportError:
    pass

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

# Partido de prueba: Inter Milan vs Bodo (Bodø/Glimt) — Champions League (tenemos datos históricos CL)
FIXTURE_ID_TEST = 999001
HOME_TEAM = "Inter Milan"
AWAY_TEAM = "Bodo Glimt"
LEAGUE_NAME = "Champions League"
LEAGUE_CODE = "CL"
MATCH_DATE = "2026-02-18"  # fecha del partido en BD

# Probabilidades/cuotas de ejemplo (para el bloque de contexto)
PROB_HOME = 0.72
PROB_DRAW = 0.18
PROB_AWAY = 0.10
XG = 2.8
CS_HOME = 0.55
CS_AWAY = 0.25
BTTS = 0.45
OVER25 = 0.65


# IDs en nuestra BD (historical_matches CL) para que el contexto enriquecido encuentre datos
INTER_TEAM_ID_BD = 505
BODO_TEAM_ID_BD = 327


def build_match_data():
    """Un solo partido: Inter Milan vs Bodo Glimt. Incluye team_id de la BD para contexto enriquecido."""
    return [
        {
            "fixture_id": FIXTURE_ID_TEST,
            "home_team": HOME_TEAM,
            "away_team": AWAY_TEAM,
            "home": HOME_TEAM,
            "away": AWAY_TEAM,
            "home_team_id": INTER_TEAM_ID_BD,
            "away_team_id": BODO_TEAM_ID_BD,
            "league_name": LEAGUE_NAME,
            "league": LEAGUE_NAME,
            "league_code": LEAGUE_CODE,
            "league_id": LEAGUE_CODE,
            "date": MATCH_DATE,
            "prob_home_win": PROB_HOME,
            "prob_draw": PROB_DRAW,
            "prob_away_win": PROB_AWAY,
            "expected_goals": XG,
            "clean_sheet_home": CS_HOME,
            "clean_sheet_away": CS_AWAY,
            "prob_btts": BTTS,
            "prob_over25": OVER25,
        }
    ]


def resolve_team_ids(match_data_list):
    """Rellena home_team_id y away_team_id si la API está disponible."""
    try:
        from api_sports_fetcher import get_team_id_by_name
    except ImportError:
        return
    for m in match_data_list:
        if m.get("home_team_id") is None:
            m["home_team_id"] = get_team_id_by_name(m.get("home_team") or m.get("home"))
        if m.get("away_team_id") is None:
            m["away_team_id"] = get_team_id_by_name(m.get("away_team") or m.get("away"))


def build_db_context(match_data_list):
    """Contexto de forma reciente y H2H (simplificado, sin Streamlit)."""
    if not match_data_list:
        return None
    get_recent_form_historical = None
    get_h2h_historical = None
    try:
        from historical_analyzer import get_recent_form as get_recent_form_historical, get_head_to_head as get_h2h_historical
    except ImportError:
        pass
    try:
        from api_sports_fetcher import get_h2h_api_sports
    except ImportError:
        get_h2h_api_sports = None
    try:
        from data_fetcher import get_h2h
    except ImportError:
        get_h2h = None

    parts = []
    for m in match_data_list:
        home = m.get("home_team") or m.get("home") or "Local"
        away = m.get("away_team") or m.get("away") or "Visitante"
        home_id = m.get("home_team_id")
        away_id = m.get("away_team_id")
        lcode = m.get("league_code") or m.get("league_id") or ""
        fid = m.get("fixture_id")
        part = [f"{home} vs {away}:"]

        form_h = None
        if get_recent_form_historical and lcode and home_id:
            try:
                form_h = get_recent_form_historical(team_id=home_id, team_name=home, league_id=lcode, last_n=5, use_master_checked=True)
            except Exception:
                pass
        if form_h:
            goles = [str(f.get("goals_for", "?")) for f in form_h]
            part.append(f"  Forma local (goles a favor últimos 5): {', '.join(goles)}")

        form_a = None
        if get_recent_form_historical and lcode and away_id:
            try:
                form_a = get_recent_form_historical(team_id=away_id, team_name=away, league_id=lcode, last_n=5, use_master_checked=True)
            except Exception:
                pass
        if form_a:
            goles = [str(f.get("goals_for", "?")) for f in form_a]
            part.append(f"  Forma visitante (goles a favor últimos 5): {', '.join(goles)}")

        h2h = []
        if get_h2h_historical and lcode and home_id and away_id:
            try:
                h2h = get_h2h_historical(home_id=home_id, away_id=away_id, home_name=home, away_name=away, league_id=lcode, last_n=5, use_master_checked=True)
            except Exception:
                pass
        if not h2h and home_id and away_id and get_h2h_api_sports:
            try:
                h2h = get_h2h_api_sports(home_id, away_id, limit=5)
            except Exception:
                pass
        if not h2h and fid and get_h2h:
            try:
                h2h = get_h2h(fid, limit=5, use_mock=False)
            except Exception:
                pass
        if h2h:
            h2h_parts = []
            for x in h2h:
                if not isinstance(x, dict):
                    continue
                h_name, a_name = x.get("home_team_name"), x.get("away_team_name")
                hg, ag = x.get("home_goals", "?"), x.get("away_goals", "?")
                if h_name and a_name:
                    h2h_parts.append(f"{h_name} {hg}-{ag} {a_name}")
                else:
                    h2h_parts.append(f"{hg}-{ag} (local-visitante en ese partido)")
            part.append(f"  H2H (resultados): {'; '.join(h2h_parts)}")

        parts.append("\n".join(part))
    return "\n\n".join(parts) if parts else None


def main():
    out_dir = ROOT / "docs"
    out_dir.mkdir(exist_ok=True)
    path_gemini = out_dir / "v3_test_inter_bodo_gemini.md"
    path_grok = out_dir / "v3_test_inter_bodo_grok.md"

    match_data = build_match_data()
    resolve_team_ids(match_data)
    db_context = build_db_context(match_data)

    try:
        from enriched_context_v3 import build_enriched_context_for_matches
        enriched_context = build_enriched_context_for_matches(match_data)
    except Exception as e:
        log.warning("enriched_context no disponible: %s", e)
        enriched_context = ""

    from grok_client import ask_grok_proposal_analysis_v3
    from gemini_client import ask_gemini_proposal_analysis_v3

    # Gemini V3
    log.info("Llamando a Gemini V3 (Inter vs Bodo)...")
    try:
        result_gemini = ask_gemini_proposal_analysis_v3(match_data, db_context=db_context, enriched_context=enriched_context or None)
        analysis_gemini = result_gemini.get("analysis", "")
        path_gemini.write_text("# Análisis V3 — Inter Milan vs Bodo Glimt (Gemini)\n\n" + (analysis_gemini or "(sin análisis)"), encoding="utf-8")
        log.info("Escrito: %s", path_gemini)
    except Exception as e:
        log.exception("Error Gemini V3")
        path_gemini.write_text("# Análisis V3 — Inter Milan vs Bodo Glimt (Gemini)\n\nError: " + str(e), encoding="utf-8")

    # Grok V3
    log.info("Llamando a Grok V3 (Inter vs Bodo)...")
    try:
        result_grok = ask_grok_proposal_analysis_v3(match_data, db_context=db_context, enriched_context=enriched_context or None)
        analysis_grok = result_grok.get("analysis", "")
        path_grok.write_text("# Análisis V3 — Inter Milan vs Bodo Glimt (Grok)\n\n" + (analysis_grok or "(sin análisis)"), encoding="utf-8")
        log.info("Escrito: %s", path_grok)
    except Exception as e:
        log.exception("Error Grok V3")
        path_grok.write_text("# Análisis V3 — Inter Milan vs Bodo Glimt (Grok)\n\nError: " + str(e), encoding="utf-8")

    print("\nResumen:")
    print("  Gemini:", path_gemini)
    print("  Grok: ", path_grok)


if __name__ == "__main__":
    main()
