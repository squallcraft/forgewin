#!/usr/bin/env python3
"""
Prueba aislada: Inter Milan vs Bodo Glimt (Champions, hoy) con la PROPUESTA de mejora.
- Añade al contexto: xG histórico, forma en casa/fuera, offsides, (árbitro si hubiera).
- No modifica el código principal; solo este script y las llamadas a la API.
Ejecutar desde la raíz: python scripts/run_prueba_propuesta_inter_bodo.py
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

# Partido: Inter Milan vs Bodo Glimt — Champions League (hoy)
FIXTURE_ID_TEST = 999001
HOME_TEAM = "Inter Milan"
AWAY_TEAM = "Bodo Glimt"
LEAGUE_NAME = "Champions League"
LEAGUE_CODE = "CL"
MATCH_DATE = "2026-02-18"

PROB_HOME, PROB_DRAW, PROB_AWAY = 0.72, 0.18, 0.10
XG, CS_HOME, CS_AWAY = 2.8, 0.55, 0.25
BTTS, OVER25 = 0.45, 0.65

INTER_TEAM_ID_BD = 505
BODO_TEAM_ID_BD = 327


def build_match_data():
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


def build_db_context(match_data_list):
    """Contexto forma + H2H (igual que run_v3_test_inter_bodo)."""
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


def build_proposal_extra_context(home_id: int, away_id: int, home_name: str, away_name: str, league_id: str) -> str:
    """
    Bloque extra de la propuesta: xG histórico, forma casa/fuera, offsides.
    Consultas directas a historical_matches sin tocar db.py.
    """
    from db import get_connection

    lines = ["**Datos adicionales (propuesta de mejora):**"]
    try:
        with get_connection() as conn:
            c = conn.cursor()
            # Columnas que pueden no existir en todas las BDs
            cols_xg = "home_xg, away_xg, home_goals, away_goals"
            cols_off = "home_offsides, away_offsides"
            for label, team_id, team_name in [("Local", home_id, home_name), ("Visitante", away_id, away_name)]:
                # xG histórico (últimos partidos donde participa el equipo)
                try:
                    c.execute(
                        f"""SELECT home_xg, away_xg, home_goals, away_goals, home_team_id, away_team_id
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

                # Forma en casa vs fuera (últimos 5 en casa, últimos 5 fuera)
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

                # Offsides (media)
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
        lines.append(f"  (Error obteniendo datos propuesta: {e})")
    return "\n".join(lines) if len(lines) > 1 else ""


def main():
    from db import init_db
    init_db()

    match_data = build_match_data()
    db_context = build_db_context(match_data)

    try:
        from enriched_context_v3 import build_enriched_context_for_matches
        enriched_context = build_enriched_context_for_matches(match_data) or ""
    except Exception as e:
        log.warning("enriched_context no disponible: %s", e)
        enriched_context = ""

    # Bloque de la propuesta (xG histórico, forma casa/fuera, offsides)
    extra = build_proposal_extra_context(
        INTER_TEAM_ID_BD, BODO_TEAM_ID_BD,
        HOME_TEAM, AWAY_TEAM,
        LEAGUE_CODE,
    )
    if extra:
        enriched_context = (enriched_context + "\n\n" + extra).strip() if enriched_context else extra
        log.info("Contexto propuesta añadido (xG, forma casa/fuera, offsides).")

    # Un solo modelo para la prueba (Grok); si quieres ambos, descomenta Gemini
    from grok_client import ask_grok_proposal_analysis_v3
    log.info("Llamando a Alfred (Grok) V3 con contexto propuesta...")
    try:
        result = ask_grok_proposal_analysis_v3(
            match_data,
            db_context=db_context,
            enriched_context=enriched_context or None,
        )
        analysis = result.get("analysis", "")
        print("\n" + "=" * 70)
        print("ANÁLISIS V3 — Inter Milan vs Bodo Glimt (Champions League, hoy)")
        print("Con propuesta: xG histórico, forma casa/fuera, offsides")
        print("=" * 70 + "\n")
        print(analysis or "(sin análisis)")
        print("\n" + "=" * 70)
        # Guardar en docs para que lo veas
        out_path = ROOT / "docs" / "prueba_propuesta_inter_bodo_grok.md"
        out_path.parent.mkdir(exist_ok=True)
        out_path.write_text(
            "# Prueba propuesta — Inter vs Bodo (Alfred V3)\n\n" + (analysis or "(sin análisis)"),
            encoding="utf-8",
        )
        log.info("Guardado: %s", out_path)
    except Exception as e:
        log.exception("Error Grok V3")
        print("Error:", e)

    # Opcional: Gemini
    # from gemini_client import ask_gemini_proposal_analysis_v3
    # log.info("Llamando a Reginald (Gemini) V3...")
    # result_m = ask_gemini_proposal_analysis_v3(match_data, db_context=db_context, enriched_context=enriched_context or None)
    # print("\n--- Reginald ---\n", result_m.get("analysis", ""))


if __name__ == "__main__":
    main()
