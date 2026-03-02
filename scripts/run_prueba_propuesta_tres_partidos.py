#!/usr/bin/env python3
"""
Prueba aislada con la propuesta (xG histórico, forma casa/fuera, offsides) para 3 partidos de Champions:
  1) Real Madrid vs Benfica
  2) Atalanta vs Monaco
  3) PSG vs Monaco
Ejecutar desde la raíz: python scripts/run_prueba_propuesta_tres_partidos.py
"""

import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
except ImportError:
    pass

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

LEAGUE_NAME = "Champions League"
LEAGUE_CODE = "CL"
MATCH_DATE = "2026-02-18"

# API-Sports team IDs (Champions League)
MATCHES = [
    {
        "fixture_id": 999002,
        "home_team": "Real Madrid",
        "away_team": "Benfica",
        "home_team_id": 541,
        "away_team_id": 211,
        "prob_home_win": 0.68,
        "prob_draw": 0.20,
        "prob_away_win": 0.12,
        "expected_goals": 2.7,
        "clean_sheet_home": 0.48,
        "clean_sheet_away": 0.22,
        "prob_btts": 0.48,
        "prob_over25": 0.62,
    },
    {
        "fixture_id": 999003,
        "home_team": "Atalanta",
        "away_team": "Monaco",
        "home_team_id": 499,
        "away_team_id": 91,
        "prob_home_win": 0.45,
        "prob_draw": 0.28,
        "prob_away_win": 0.27,
        "expected_goals": 2.5,
        "clean_sheet_home": 0.32,
        "clean_sheet_away": 0.30,
        "prob_btts": 0.55,
        "prob_over25": 0.58,
    },
    {
        "fixture_id": 999004,
        "home_team": "Paris Saint Germain",
        "away_team": "Monaco",
        "home_team_id": 85,
        "away_team_id": 91,
        "prob_home_win": 0.62,
        "prob_draw": 0.22,
        "prob_away_win": 0.16,
        "expected_goals": 2.8,
        "clean_sheet_home": 0.42,
        "clean_sheet_away": 0.28,
        "prob_btts": 0.52,
        "prob_over25": 0.64,
    },
]


def build_match_data(m):
    return [
        {
            "fixture_id": m["fixture_id"],
            "home_team": m["home_team"],
            "away_team": m["away_team"],
            "home": m["home_team"],
            "away": m["away_team"],
            "home_team_id": m["home_team_id"],
            "away_team_id": m["away_team_id"],
            "league_name": LEAGUE_NAME,
            "league": LEAGUE_NAME,
            "league_code": LEAGUE_CODE,
            "league_id": LEAGUE_CODE,
            "date": MATCH_DATE,
            "prob_home_win": m["prob_home_win"],
            "prob_draw": m["prob_draw"],
            "prob_away_win": m["prob_away_win"],
            "expected_goals": m["expected_goals"],
            "clean_sheet_home": m["clean_sheet_home"],
            "clean_sheet_away": m["clean_sheet_away"],
            "prob_btts": m["prob_btts"],
            "prob_over25": m["prob_over25"],
        }
    ]


def build_db_context(match_data_list):
    if not match_data_list:
        return None
    get_recent_form_historical = get_h2h_historical = get_h2h_api_sports = get_h2h = None
    try:
        from historical_analyzer import get_recent_form as get_recent_form_historical, get_head_to_head as get_h2h_historical
    except ImportError:
        pass
    try:
        from api_sports_fetcher import get_h2h_api_sports
    except ImportError:
        pass
    try:
        from data_fetcher import get_h2h
    except ImportError:
        pass

    parts = []
    for m in match_data_list:
        home = m.get("home_team") or m.get("home") or "Local"
        away = m.get("away_team") or m.get("away") or "Visitante"
        home_id, away_id = m.get("home_team_id"), m.get("away_team_id")
        lcode = m.get("league_code") or m.get("league_id") or ""
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
        if not h2h and m.get("fixture_id") and get_h2h:
            try:
                h2h = get_h2h(m["fixture_id"], limit=5, use_mock=False)
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
                    h2h_parts.append(f"{hg}-{ag}")
            part.append(f"  H2H (resultados): {'; '.join(h2h_parts)}")

        parts.append("\n".join(part))
    return "\n\n".join(parts) if parts else None


def build_proposal_extra_context(home_id: int, away_id: int, home_name: str, away_name: str) -> str:
    from db import get_connection

    lines = ["**Datos adicionales (propuesta de mejora):**"]
    try:
        with get_connection() as conn:
            c = conn.cursor()
            for label, team_id, team_name in [("Local", home_id, home_name), ("Visitante", away_id, away_name)]:
                try:
                    c.execute(
                        """SELECT home_xg, away_xg, home_goals, away_goals, home_team_id, away_team_id
                             FROM historical_matches
                             WHERE (home_team_id = ? OR away_team_id = ?)
                             ORDER BY date DESC LIMIT 8""",
                        (team_id, team_id),
                    )
                    rows = c.fetchall()
                except Exception:
                    rows = []
                if rows:
                    xg_for, xg_ag, goals_for, goals_ag = [], [], [], []
                    for r in rows:
                        d = dict(r)
                        is_home = d.get("home_team_id") == team_id
                        hxg, axg = d.get("home_xg"), d.get("away_xg")
                        hg, ag = d.get("home_goals"), d.get("away_goals")
                        if hxg is not None and axg is not None:
                            xg_for.append(hxg if is_home else axg)
                            xg_ag.append(axg if is_home else hxg)
                        if hg is not None and ag is not None:
                            goals_for.append(hg if is_home else ag)
                            goals_ag.append(ag if is_home else hg)
                    if xg_for:
                        avg_xg_f = round(sum(xg_for) / len(xg_for), 2)
                        avg_xg_a = round(sum(xg_ag) / len(xg_ag), 2)
                        avg_gf = round(sum(goals_for) / len(goals_for), 2) if goals_for else None
                        avg_ga = round(sum(goals_ag) / len(goals_ag), 2) if goals_ag else None
                        s = f"  {label} ({team_name}): xG medio a favor {avg_xg_f}, xG medio en contra {avg_xg_a}"
                        if avg_gf is not None and avg_ga is not None:
                            s += f"; goles reales media {avg_gf} a favor, {avg_ga} en contra (últimos {len(rows)} partidos)."
                        else:
                            s += f" (últimos {len(rows)} partidos)."
                        lines.append(s)

                try:
                    c.execute(
                        """SELECT home_goals, away_goals, home_team_id, away_team_id
                             FROM historical_matches
                             WHERE (home_team_id = ? OR away_team_id = ?)
                             ORDER BY date DESC LIMIT 15""",
                        (team_id, team_id),
                    )
                    rows = c.fetchall()
                except Exception:
                    rows = []
                home_gf, away_gf = [], []
                for r in rows:
                    d = dict(r)
                    is_home = d.get("home_team_id") == team_id
                    gf = d["home_goals"] if is_home else d["away_goals"]
                    if is_home and len(home_gf) < 5:
                        home_gf.append(gf)
                    elif not is_home and len(away_gf) < 5:
                        away_gf.append(gf)
                    if len(home_gf) >= 5 and len(away_gf) >= 5:
                        break
                if home_gf or away_gf:
                    s = f"  Forma {label} ({team_name}):"
                    if home_gf:
                        s += f" en casa (últimos {len(home_gf)}): goles a favor {', '.join(map(str, home_gf))}."
                    if away_gf:
                        s += f" Fuera (últimos {len(away_gf)}): goles a favor {', '.join(map(str, away_gf))}."
                    lines.append(s)

                try:
                    c.execute(
                        """SELECT home_offsides, away_offsides, home_team_id, away_team_id
                             FROM historical_matches
                             WHERE (home_team_id = ? OR away_team_id = ?)
                               AND (home_offsides IS NOT NULL OR away_offsides IS NOT NULL)
                             ORDER BY date DESC LIMIT 8""",
                        (team_id, team_id),
                    )
                    rows = c.fetchall()
                except Exception:
                    rows = []
                offs = []
                for r in rows:
                    d = dict(r)
                    is_home = d.get("home_team_id") == team_id
                    o = d.get("home_offsides") if is_home else d.get("away_offsides")
                    if o is not None:
                        offs.append(int(o))
                if offs:
                    avg_off = round(sum(offs) / len(offs), 1)
                    lines.append(f"  Fueras de juego {label} ({team_name}): media {avg_off} por partido (últimos {len(offs)} con dato).")
    except Exception as e:
        lines.append(f"  (Error: {e})")
    return "\n".join(lines) if len(lines) > 1 else ""


def run_one(match_spec, results_list):
    from db import init_db
    init_db()

    match_data = build_match_data(match_spec)
    db_context = build_db_context(match_data)

    try:
        from enriched_context_v3 import build_enriched_context_for_matches
        enriched_context = build_enriched_context_for_matches(match_data) or ""
    except Exception:
        enriched_context = ""

    extra = build_proposal_extra_context(
        match_spec["home_team_id"], match_spec["away_team_id"],
        match_spec["home_team"], match_spec["away_team"],
    )
    if extra:
        enriched_context = (enriched_context + "\n\n" + extra).strip() if enriched_context else extra

    from grok_client import ask_grok_proposal_analysis_v3
    title = f"{match_spec['home_team']} vs {match_spec['away_team']}"
    log.info("Analizando: %s ...", title)
    try:
        result = ask_grok_proposal_analysis_v3(
            match_data,
            db_context=db_context,
            enriched_context=enriched_context or None,
        )
        analysis = result.get("analysis", "") or "(sin análisis)"
    except Exception as e:
        analysis = f"(Error: {e})"
    results_list.append({"title": title, "analysis": analysis})
    return analysis


def main():
    results = []
    for i, m in enumerate(MATCHES, 1):
        log.info("=== Partido %d/3 ===", i)
        run_one(m, results)

    # Imprimir todos los resultados
    print("\n\n")
    print("=" * 80)
    print("RESULTADOS PRUEBA PROPUESTA — TRES PARTIDOS CHAMPIONS LEAGUE")
    print("=" * 80)
    for r in results:
        print("\n")
        print("-" * 80)
        print("PARTIDO:", r["title"])
        print("-" * 80)
        print(r["analysis"])
        print()

    # Guardar en un solo doc
    out_path = ROOT / "docs" / "prueba_propuesta_tres_partidos.md"
    out_path.parent.mkdir(exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("# Prueba propuesta — Tres partidos Champions League\n\n")
        for r in results:
            f.write(f"## {r['title']}\n\n")
            f.write(r["analysis"])
            f.write("\n\n---\n\n")
    log.info("Guardado: %s", out_path)


if __name__ == "__main__":
    main()
