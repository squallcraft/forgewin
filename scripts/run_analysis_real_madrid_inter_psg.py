#!/usr/bin/env python3
"""
Análisis V3 y V2 para 3 partidos CL: Real Madrid, Inter de Milán, PSG.
Partidos: Benfica vs Real Madrid, Bodo/Glimt vs Inter, Monaco vs PSG.
Mismo flujo que run_cl_today_v3_v2 (contexto, enriched, Grok V3, Gemini V3, Grok V2, Gemini V2).
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
except ImportError:
    pass

# Partidos desde historical_matches (fixture_id, equipos, ids)
MATCHES_FROM_DB = [
    {"fixture_id": 901515515, "home_team": "Benfica", "away_team": "Real Madrid", "home_team_id": 211, "away_team_id": 541, "date": "2026-02-17"},
    {"fixture_id": 901515519, "home_team": "Bodo/Glimt", "away_team": "Inter", "home_team_id": 327, "away_team_id": 505, "date": "2026-02-18"},
    {"fixture_id": 901515517, "home_team": "Monaco", "away_team": "Paris Saint Germain", "home_team_id": 91, "away_team_id": 85, "date": "2026-02-17"},
]


def _format_h2h(h2h):
    parts = []
    for x in (h2h or []):
        if not isinstance(x, dict):
            continue
        h_name, a_name = x.get("home_team_name"), x.get("away_team_name")
        hg, ag = x.get("home_goals", "?"), x.get("away_goals", "?")
        if h_name and a_name:
            parts.append(f"{h_name} {hg}-{ag} {a_name}")
        else:
            parts.append(f"{hg}-{ag}")
    return "; ".join(parts)


def build_db_context(match_data_list):
    if not match_data_list:
        return None
    try:
        from historical_analyzer import get_recent_form as get_recent_form_historical, get_head_to_head as get_h2h_historical
    except ImportError:
        get_recent_form_historical = get_h2h_historical = None
    try:
        from api_sports_fetcher import get_h2h_api_sports, get_team_id_by_name, FALLBACK_FIXTURE_ID_MIN
    except ImportError:
        get_h2h_api_sports = get_team_id_by_name = None
        FALLBACK_FIXTURE_ID_MIN = 900_000_000
    try:
        from data_fetcher import get_h2h
    except ImportError:
        get_h2h = None

    cache_team_id = {}
    cache_h2h = {}
    parts = []
    for m in match_data_list:
        home = m.get("home_team") or m.get("home") or "Local"
        away = m.get("away_team") or m.get("away") or "Visitante"
        fid = m.get("fixture_id")
        home_id = m.get("home_team_id")
        away_id = m.get("away_team_id")
        lcode = m.get("league_code") or m.get("league_id") or "CL"
        part = [f"{home} vs {away}:"]
        try:
            use_fallback = fid is not None and int(fid) >= FALLBACK_FIXTURE_ID_MIN
        except (TypeError, ValueError):
            use_fallback = False
        as_home_id, as_away_id = home_id, away_id
        if not use_fallback and get_team_id_by_name:
            if home not in cache_team_id:
                cache_team_id[home] = get_team_id_by_name(home)
            if away not in cache_team_id:
                cache_team_id[away] = get_team_id_by_name(away)
            as_home_id = cache_team_id.get(home) or home_id
            as_away_id = cache_team_id.get(away) or away_id
        if get_recent_form_historical and lcode:
            try:
                form_h = get_recent_form_historical(team_id=as_home_id, team_name=home, league_id=lcode, last_n=5, use_master_checked=True)
                if form_h:
                    part.append(f"  Forma local (goles últimos 5): {', '.join(str(f.get('goals_for', '?')) for f in form_h)}")
            except Exception:
                pass
            try:
                form_a = get_recent_form_historical(team_id=as_away_id, team_name=away, league_id=lcode, last_n=5, use_master_checked=True)
                if form_a:
                    part.append(f"  Forma visitante (goles últimos 5): {', '.join(str(f.get('goals_for', '?')) for f in form_a)}")
            except Exception:
                pass
        h2h = []
        if get_h2h_historical and lcode:
            try:
                h2h = get_h2h_historical(home_id=as_home_id, away_id=as_away_id, home_name=home, away_name=away, league_id=lcode, last_n=5, use_master_checked=True)
            except Exception:
                pass
        if not h2h and as_home_id and as_away_id and get_h2h_api_sports:
            h2h_key = (min(as_home_id, as_away_id), max(as_home_id, as_away_id))
            if h2h_key not in cache_h2h:
                cache_h2h[h2h_key] = get_h2h_api_sports(as_home_id, as_away_id, limit=5)
            h2h = cache_h2h[h2h_key]
        if not h2h and fid and get_h2h and not use_fallback:
            try:
                h2h = get_h2h(fid, limit=5, use_mock=False)
            except Exception:
                pass
        if h2h:
            part.append(f"  H2H (resultados): {_format_h2h(h2h)}")
        parts.append("\n".join(part))
    return "\n\n".join(parts) if parts else None


def main():
    from db import init_db
    init_db()

    match_data = []
    for m in MATCHES_FROM_DB:
        match_data.append({
            "fixture_id": m["fixture_id"],
            "home_team": m["home_team"],
            "away_team": m["away_team"],
            "home": m["home_team"],
            "away": m["away_team"],
            "home_team_id": m["home_team_id"],
            "away_team_id": m["away_team_id"],
            "league_code": "CL",
            "league_id": "CL",
            "league_name": "Champions League",
            "league": "Champions League",
            "date": m.get("date", ""),
            "prob_home_win": None,
            "prob_draw": None,
            "prob_away_win": None,
            "expected_goals": None,
            "clean_sheet_home": None,
            "clean_sheet_away": None,
            "prob_btts": None,
            "prob_over25": None,
        })

    db_ctx = build_db_context(match_data)
    try:
        from enriched_context_v3 import build_enriched_context_for_matches
        enriched_ctx = build_enriched_context_for_matches(match_data) or ""
    except Exception:
        enriched_ctx = ""

    from grok_client import ask_grok_proposal_analysis_v3, ask_grok_proposal_analysis
    from gemini_client import ask_gemini_proposal_analysis_v3, ask_gemini_proposal_analysis

    print("=" * 60)
    print("PARTIDOS A ANALIZAR (Real Madrid, Inter, PSG):")
    for m in match_data:
        print(f"  {m['home_team']} vs {m['away_team']}")
    print("=" * 60)

    print("\n### ANÁLISIS V3 (asedio, fricción, HT/FT)\n")
    try:
        r_grok_v3 = ask_grok_proposal_analysis_v3(match_data, db_context=db_ctx, enriched_context=enriched_ctx or None)
        print("--- Alfred (Grok) V3 ---\n")
        text = r_grok_v3.get("analysis", "(vacío)")
        print(text[:12000] if len(text) > 12000 else text)
        if len(text) > 12000:
            print("\n... [truncado]")
    except Exception as e:
        print("Error Grok V3:", e)
    try:
        r_gemini_v3 = ask_gemini_proposal_analysis_v3(match_data, db_context=db_ctx, enriched_context=enriched_ctx or None)
        print("\n--- Reginald (Gemini) V3 ---\n")
        text = r_gemini_v3.get("analysis", "(vacío)")
        print(text[:12000] if len(text) > 12000 else text)
        if len(text) > 12000:
            print("\n... [truncado]")
    except Exception as e:
        print("Error Gemini V3:", e)

    print("\n" + "=" * 60)
    print("### ANÁLISIS V2 (forma, H2H, probs)\n")
    try:
        r_grok_v2 = ask_grok_proposal_analysis(match_data, db_context=db_ctx)
        print("--- Alfred (Grok) V2 ---\n")
        text = r_grok_v2.get("analysis", "(vacío)")
        print(text[:8000] if len(text) > 8000 else text)
        if len(text) > 8000:
            print("\n... [truncado]")
    except Exception as e:
        print("Error Grok V2:", e)
    try:
        r_gemini_v2 = ask_gemini_proposal_analysis(match_data, db_context=db_ctx)
        print("\n--- Reginald (Gemini) V2 ---\n")
        text = r_gemini_v2.get("analysis", "(vacío)")
        print(text[:8000] if len(text) > 8000 else text)
        if len(text) > 8000:
            print("\n... [truncado]")
    except Exception as e:
        print("Error Gemini V2:", e)


if __name__ == "__main__":
    main()
