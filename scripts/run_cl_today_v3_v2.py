#!/usr/bin/env python3
"""
Testing: análisis V3 y V2 de todos los partidos de Champions League del día de hoy.
Obtiene partidos CL de la BD (fecha local Chile), construye contexto y llama a Gemini + Grok.
Uso: python scripts/run_cl_today_v3_v2.py
"""

import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
except ImportError:
    pass

from db import init_db, get_matches_by_local_date, get_connection
from config import TOP_10_LEAGUE_CODES


def _format_h2h(h2h):
    parts = []
    for x in h2h:
        if not isinstance(x, dict):
            continue
        h_name, a_name = x.get("home_team_name"), x.get("away_team_name")
        hg, ag = x.get("home_goals", "?"), x.get("away_goals", "?")
        if h_name and a_name:
            parts.append(f"{h_name} {hg}-{ag} {a_name}")
        else:
            parts.append(f"{hg}-{ag} (local-visitante en ese partido)")
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
        lcode = m.get("league_code") or m.get("league_id") or ""
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
    init_db()
    # Hoy en zona Chile (igual que la app)
    from zoneinfo import ZoneInfo
    from datetime import datetime
    tz = ZoneInfo("America/Santiago")
    today_local = datetime.now(tz).date()
    todays = get_matches_by_local_date(TOP_10_LEAGUE_CODES, today_local)
    cl_matches = [m for m in todays if (m.get("league_code") or m.get("league_id")) == "CL"]
    if not cl_matches:
        print(f"No hay partidos de Champions League para la fecha local {today_local} (America/Santiago).")
        print("Partidos en BD hoy:", len(todays), "— ligas:", list(set(m.get("league_code") or m.get("league_id") for m in todays)))
        return

    match_data = []
    for m in cl_matches:
        date_str = (m.get("match_date_utc") or m.get("date") or "")[:10] if (m.get("match_date_utc") or m.get("date")) else ""
        match_data.append({
            "fixture_id": m.get("fixture_id"),
            "home_team": m.get("home_team"),
            "away_team": m.get("away_team"),
            "home": m.get("home_team"),
            "away": m.get("away_team"),
            "home_team_id": m.get("home_team_id"),
            "away_team_id": m.get("away_team_id"),
            "league_code": "CL",
            "league_id": "CL",
            "league_name": m.get("league_name") or "Champions League",
            "league": m.get("league_name") or "Champions League",
            "date": date_str,
            "prob_home_win": m.get("prob_home_win"),
            "prob_draw": m.get("prob_draw"),
            "prob_away_win": m.get("prob_away_win"),
            "expected_goals": m.get("expected_goals"),
            "clean_sheet_home": m.get("clean_sheet_home"),
            "clean_sheet_away": m.get("clean_sheet_away"),
            "prob_btts": m.get("prob_btts"),
            "prob_over25": m.get("prob_over25"),
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
    print(f"PARTIDOS CL DEL DÍA ({today_local}): {len(match_data)}")
    for m in match_data:
        print(f"  {m['home_team']} vs {m['away_team']}")
    print("=" * 60)

    # ----- V3 -----
    print("\n### ANÁLISIS V3 (con asedio, fricción, HT/FT)\n")
    try:
        r_grok_v3 = ask_grok_proposal_analysis_v3(match_data, db_context=db_ctx, enriched_context=enriched_ctx or None)
        print("--- Alfred (Grok) V3 ---\n")
        print(r_grok_v3.get("analysis", "(vacío)")[:8000])
        if len(r_grok_v3.get("analysis", "")) > 8000:
            print("\n... [truncado]")
    except Exception as e:
        print("Error Grok V3:", e)
    try:
        r_gemini_v3 = ask_gemini_proposal_analysis_v3(match_data, db_context=db_ctx, enriched_context=enriched_ctx or None)
        print("\n--- Reginald (Gemini) V3 ---\n")
        print(r_gemini_v3.get("analysis", "(vacío)")[:8000])
        if len(r_gemini_v3.get("analysis", "")) > 8000:
            print("\n... [truncado]")
    except Exception as e:
        print("Error Gemini V3:", e)

    # ----- V2 -----
    print("\n" + "=" * 60)
    print("### ANÁLISIS V2 (forma, H2H, probs)\n")
    try:
        r_grok_v2 = ask_grok_proposal_analysis(match_data, db_context=db_ctx)
        print("--- Alfred (Grok) V2 ---\n")
        print(r_grok_v2.get("analysis", "(vacío)")[:8000])
        if len(r_grok_v2.get("analysis", "")) > 8000:
            print("\n... [truncado]")
    except Exception as e:
        print("Error Grok V2:", e)
    try:
        r_gemini_v2 = ask_gemini_proposal_analysis(match_data, db_context=db_ctx)
        print("\n--- Reginald (Gemini) V2 ---\n")
        print(r_gemini_v2.get("analysis", "(vacío)")[:8000])
        if len(r_gemini_v2.get("analysis", "")) > 8000:
            print("\n... [truncado]")
    except Exception as e:
        print("Error Gemini V2:", e)


if __name__ == "__main__":
    main()
