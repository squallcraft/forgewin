"""
ForgeWin - Partidos del día (top 10 ligas), análisis Poisson, propuestas Alfred/Reginald, BD y usuarios.
"""

import logging
import os
import traceback

import streamlit as st
from streamlit.components.v1 import html as st_components_html

# Logging a consola (visible al ejecutar: streamlit run app.py)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)
import random
import pandas as pd
from collections import defaultdict
from datetime import datetime, date, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

# Mensajes de espera que rotan durante el análisis (hacen más amena la espera)
WAITING_MESSAGES = [
    "El algoritmo está analizando los partidos seleccionados…",
    "Alfred está revisando forma reciente y resultados entre equipos…",
    "Reginald está preparando su análisis…",
    "Calculando probabilidades (Poisson y cuotas de mercado)…",
    "Revisando datos históricos y enfrentamientos directos…",
    "Un momento, estamos armando tu propuesta…",
    "Alfred y Reginald están trabajando en paralelo…",
    "Esto puede tardar un poco: estamos procesando varios partidos…",
    "Analizando estadísticas de cada encuentro…",
    "Preparando la propuesta general 1+2…",
    "Comparando opiniones de Alfred y Reginald…",
    "Casi listo: generando la recomendación final…",
    "Alfred está generando la tabla de métricas por partido…",
    "Reginald está leyendo el análisis de Alfred…",
    "Integrando forma reciente, H2H y contexto de liga…",
]


def _random_waiting_message() -> str:
    """Devuelve un mensaje aleatorio para mostrar durante la espera del análisis."""
    return random.choice(WAITING_MESSAGES)

from config import LEAGUES, TOP_10_LEAGUE_CODES, PLANS_BENEFITS, get_league_emblem_url, get_league_code_from_name, get_league_id, get_league_name, get_canonical_league_name, FALLBACK_LOGO_URL
from db import (
    init_db,
    save_error_report,
    get_error_reports,
    get_accuracy_stats,
    create_proposal,
    get_proposal,
    get_proposals_for_history,
    set_proposal_pdf,
    set_proposal_email_sent,
    evaluate_proposals,
    get_proposal_outcomes,
    get_matches_by_fixture_ids,
    get_matches_by_date,
    get_matches_by_local_date,
    get_pending_discrepancies,
    resolve_discrepancy,
    normalize_team_name,
    get_pending_team_aliases,
    resolve_team_alias,
    create_user_session,
    get_session_user,
    delete_user_session,
)
from auth import (
    verify_password,
    get_user_by_username,
    ensure_admin_exists,
    create_user,
    list_users,
    set_grok_enabled,
    set_user_tier,
    add_credits,
    deduct_credits,
    get_user_by_id,
)
from scraper import get_todays_matches, get_upcoming_matches
from analyzer import analyze_specific_league, analyze_matches, top_teams_avg_goals, run_query
from data_fetcher import get_h2h
from api_sports_fetcher import get_h2h_api_sports, get_team_id_by_name, FALLBACK_FIXTURE_ID_MIN
try:
    from historical_analyzer import get_recent_form as get_recent_form_historical, get_head_to_head as get_h2h_historical
except ImportError:
    get_recent_form_historical = None
    get_h2h_historical = None
from concurrent.futures import ThreadPoolExecutor
from grok_client import ask_grok_proposal_analysis, ask_grok_proposal_analysis_v3, ask_grok_chat
from gemini_client import ask_gemini_proposal_analysis, ask_gemini_proposal_analysis_v3, ask_gemini_custom
from config import GEMINI_API_KEY, GROK_API_KEY
from odds import get_match_odds
from sync_data import fetch_and_store_matches, fetch_finished_results
from pdf_report import generate_proposal_pdf, generate_proposal_pdf_v2, generate_proposal_pdf_league_combined
from send_email import send_proposal_email
try:
    from enriched_context_v3 import build_enriched_context_for_matches
except ImportError:
    build_enriched_context_for_matches = None

# ── Persistencia de sesión vía token en URL query param ───────────────────────
# st.query_params persiste entre reloads y sobrevive la redirección de MP
# cuando incluimos ?t=TOKEN en el back_url.
_SESSION_PARAM = "t"


def _normalize_and_dedupe_matches(matches: list) -> list:
    """
    Homologa nombres de equipos y liga, y deduplica partidos.
    - Normaliza home/away con entity_aliases (normalize_team_name).
    - Unifica nombre de liga con get_canonical_league_name (evita LaLiga vs Primera Division).
    - Deduplica por (fecha, local, visitante, liga); mantiene la primera fila de cada grupo.
    """
    if not matches:
        return []
    out = []
    seen = set()
    for m in matches:
        lcode = m.get("league_code") or m.get("league_id") or get_league_code_from_name(m.get("league_name") or "")
        league_canonical = get_canonical_league_name(m.get("league_name") or m.get("league") or lcode or "")
        home_raw = m.get("home_team") or m.get("home") or ""
        away_raw = m.get("away_team") or m.get("away") or ""
        home_norm = normalize_team_name(home_raw, lcode) if home_raw else ""
        away_norm = normalize_team_name(away_raw, lcode) if away_raw else ""
        date_val = m.get("date") or m.get("match_date_utc") or ""
        date_str = str(date_val)[:10] if date_val else ""
        key = (date_str, home_norm, away_norm, lcode or league_canonical)
        if key in seen:
            continue
        seen.add(key)
        row = dict(m)
        row["home_team"] = home_norm or home_raw
        row["away_team"] = away_norm or away_raw
        row["league_name"] = league_canonical
        if row.get("home") is not None or home_raw:
            row["home"] = home_norm or home_raw
        if row.get("away") is not None or away_raw:
            row["away"] = away_norm or away_raw
        if row.get("league") is not None or m.get("league_name") or m.get("league"):
            row["league"] = league_canonical
        out.append(row)
    return out


def _run_proposals_parallel(match_data, db_context=None):
    """Ejecuta propuestas de Alfred y Reginald en paralelo; devuelve (result_grok, result_gemini)."""
    with ThreadPoolExecutor(max_workers=2) as ex:
        f_g = ex.submit(ask_grok_proposal_analysis, match_data, db_context)
        f_m = ex.submit(ask_gemini_proposal_analysis, match_data, db_context)
        return f_g.result(), f_m.result()


def _run_proposals_parallel_v3(match_data, db_context=None, enriched_context=None):
    """Ejecuta análisis V3 (enriquecido: asedio, fricción, HT/FT) en paralelo; devuelve (result_grok, result_gemini)."""
    with ThreadPoolExecutor(max_workers=2) as ex:
        f_g = ex.submit(ask_grok_proposal_analysis_v3, match_data, db_context, enriched_context)
        f_m = ex.submit(ask_gemini_proposal_analysis_v3, match_data, db_context, enriched_context)
        return f_g.result(), f_m.result()


def _can_plus(user: dict) -> bool:
    """MVP: todos los usuarios autenticados tienen acceso completo (PDF, historial, ligas combinadas)."""
    return bool(user)


def _can_v2(user: dict) -> bool:
    """True si el usuario tiene acceso al modo V2 (solo Usuario++ o admin)."""
    if not user:
        return False
    return user.get("role") == "admin" or (user.get("tier") or "base") == "plus_plus"


def _can_run_analysis(user: dict, n_matches: int = 1) -> bool:
    """True si puede ejecutar análisis (tiene créditos >= n_matches o es admin). 1 crédito = 1 partido."""
    if not user:
        return False
    if user.get("role") == "admin":
        return True
    balance = int(user.get("credits_balance") or 0)
    return balance >= n_matches


def _has_any_credit(user: dict) -> bool:
    """True si tiene al menos 1 crédito o es admin."""
    return _can_run_analysis(user, 1)


def _effective_ids_to_analyze(selected_ids: list, user: dict) -> list:
    """Si el usuario seleccionó más partidos que sus créditos, devuelve muestra aleatoria hasta el límite."""
    if not user or not selected_ids:
        return selected_ids
    if user.get("role") == "admin":
        return selected_ids
    balance = int(user.get("credits_balance") or 0)
    n = min(len(selected_ids), balance)
    if n >= len(selected_ids):
        return selected_ids
    return random.sample(selected_ids, n)


def _render_buy_credits_ui(num_credits: int, key_prefix: str) -> None:
    """Muestra botón para comprar créditos a 2.000 CLP c/u (Mercado Pago). Mínimo 10 créditos."""
    user = st.session_state.get("current_user")
    if not user or user.get("role") == "admin":
        return
    try:
        from payment_controller import is_configured, create_preference_for_credits, PRICE_PER_CREDIT_CLP, MIN_CREDITS_PURCHASE
        from referrals import validate_referral_code, REFERRAL_PRICE_CLP, NORMAL_PACK_PRICE_CLP, user_has_used_referral
        if not is_configured():
            st.caption("Pagos no configurados. Contacta al administrador.")
            return
    except ImportError:
        return
    # Aplicar mínimo de compra
    num_credits = max(num_credits, MIN_CREDITS_PURCHASE)
    user_email = user.get("email") or ""
    if not user_email:
        user_email = st.text_input("Tu email (para Mercado Pago)", key=f"{key_prefix}_email", placeholder="correo@ejemplo.com")

    # Campo de código de referido (solo si no ha usado uno antes)
    referral_code = ""
    referral_valid = False
    if not user_has_used_referral(user["id"]):
        ref_default = st.session_state.get("pending_referral_code", "")
        referral_input = st.text_input(
            "¿Tienes un código de referido? (opcional)",
            value=ref_default,
            key=f"{key_prefix}_ref_code",
            placeholder="Ej: TOMAS2024",
            max_chars=12,
        )
        if referral_input:
            ok_ref, msg_ref = validate_referral_code(referral_input.upper().strip(), user["id"])
            if ok_ref:
                st.success(msg_ref)
                referral_code = referral_input.upper().strip()
                referral_valid = True
            else:
                st.warning(f"Código no válido: {msg_ref}")

    if user_email:
        # Precio con o sin descuento
        if referral_valid and num_credits == 10:
            total_clp = REFERRAL_PRICE_CLP
            price_label = f"${total_clp:,} CLP (30% desc. por referido)".replace(",", ".")
        else:
            total_clp = num_credits * PRICE_PER_CREDIT_CLP
            price_label = f"${total_clp:,} CLP (IVA incl.)".replace(",", ".")
        label = f"Comprar {num_credits} créditos — {price_label}"
        if st.button(label, key=f"{key_prefix}_btn", type="primary"):
            _sess_tok = st.query_params.get(_SESSION_PARAM, "")
            _base = os.getenv("FORGEWIN_BASE_URL", "https://forgewin.cl")
            _back = f"{_base}?payment=pack&status=ok&t={_sess_tok}" if _sess_tok else None
            init_point, err = create_preference_for_credits(
                user["id"], num_credits, user_email,
                back_url=_back,
                referral_code=referral_code or None,
            )
            if init_point:
                st.link_button("💳 Ir a pagar con Mercado Pago", init_point, key=f"{key_prefix}_link")
            else:
                st.error(err)


def _norm_prob_display(val) -> Optional[float]:
    """Convierte probabilidad a 0-1 para formatear con .0%. Acepta 0-1 o 0-100 (LLM a veces devuelve 32 en vez de 0.32)."""
    if val is None:
        return None
    try:
        v = float(val)
        return v / 100.0 if v > 1.0 else v
    except (TypeError, ValueError):
        return None


def _format_h2h_string(h2h: list) -> str:
    """Formatea H2H para que la IA no invierta local/visitante. Incluye nombres: 'Qarabag 1-6 Newcastle'."""
    if not h2h:
        return ""
    parts = []
    for x in h2h:
        if not isinstance(x, dict):
            continue
        h_name = x.get("home_team_name")
        a_name = x.get("away_team_name")
        hg = x.get("home_goals", "?")
        ag = x.get("away_goals", "?")
        if h_name and a_name:
            parts.append(f"{h_name} {hg}-{ag} {a_name}")
        else:
            parts.append(f"{hg}-{ag} (local-visitante en ese partido)")
    return "; ".join(parts)


def _deduct_credits_and_refresh(amount: int) -> None:
    """Resta `amount` créditos al usuario actual (no admin) y actualiza session. 1 crédito = 1 partido."""
    if amount <= 0:
        return
    user = st.session_state.get("current_user")
    if not user or user.get("role") == "admin":
        return
    uid = user.get("id")
    if not uid:
        return
    ok, _ = deduct_credits(uid, amount)
    if ok:
        u = get_user_by_id(uid)
        if u is not None:
            st.session_state.current_user["credits_balance"] = int(u.get("credits_balance") or 0)


def _trigger_auto_download_once(flag_key: str) -> None:
    """Inyecta script para hacer clic automático en el primer botón de descarga y limpia el flag."""
    if not st.session_state.get(flag_key):
        return
    st.session_state[flag_key] = False
    js = """
    <script>
    (function tryClick(attempt) {
        var doc = (window.parent && window.parent.document) ? window.parent.document : document;
        var links = doc.querySelectorAll('a[download], [data-testid="stDownloadButton"] a');
        if (links.length > 0) {
            links[links.length - 1].click();
        } else if (attempt < 8) {
            setTimeout(function() { tryClick(attempt + 1); }, 400);
        }
    })(0);
    </script>
    """
    st_components_html(js, height=0)


def _show_league_logo(league_code: str, width: int = 28) -> None:
    """Muestra logo de la liga; si falla, muestra logo de balón de fútbol."""
    try:
        st.image(get_league_emblem_url(league_code), width=width)
    except Exception:
        try:
            st.image(FALLBACK_LOGO_URL, width=width)
        except Exception:
            st.write("")

# Inicializar BD y admin por defecto
init_db()
ensure_admin_exists()

# Zona horaria Chile para mostrar fechas
CHILE_TZ = ZoneInfo("America/Santiago")
MESES = ("ene", "feb", "mar", "abr", "may", "jun", "jul", "ago", "sep", "oct", "nov", "dic")


def format_date_chile(date_str):
    """Convierte fecha ISO (UTC) a texto: día - mes - año y hora - min (Chile)."""
    if not date_str:
        return "—", "—"
    try:
        s = (date_str or "").strip().replace("Z", "+00:00")
        if "+" not in s and "T" in s:
            s = s + "+00:00"
        if "T" in s:
            dt_utc = datetime.fromisoformat(s)
        else:
            dt_utc = datetime.fromisoformat(s + "T00:00:00+00:00")
        if dt_utc.tzinfo is None:
            dt_utc = dt_utc.replace(tzinfo=ZoneInfo("UTC"))
        dt_chile = dt_utc.astimezone(CHILE_TZ)
        dia_mes_ano = f"{dt_chile.day} - {MESES[dt_chile.month - 1]} - {dt_chile.year}"
        hora_min = f"{dt_chile.hour} - {dt_chile.minute:02d}"
        return dia_mes_ano, hora_min
    except Exception:
        return (date_str[:10] if date_str else "—"), "—"

_favicon_path = os.path.join(os.path.dirname(__file__), "static", "favicon.png")
st.set_page_config(
    page_title="ForgeWin - Estadísticas y análisis de partidos",
    page_icon=_favicon_path if os.path.isfile(_favicon_path) else "⚽",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Tema ForgeWin: fondo oscuro #1C1C1E, sidebar #121214, acento azul #6495ED, tipografía 14-16px
def _apply_forgewin_theme():
    st.markdown("""
    <style>
    :root {
      --fw-bg: #1C1C1E;
      --fw-sidebar: #121214;
      --fw-card: #2C2C2E;
      --fw-input: #3A3A3C;
      --fw-text: #E0E0E0;
      --fw-muted: #A0A0A0;
      --fw-accent: #6495ED;
      --fw-accent-hover: #7B68EE;
      --fw-border: #3d3d4d;
    }
    .main .block-container { padding-top: 1.25rem; max-width: 1400px; }
    [data-testid="stSidebar"] { background: var(--fw-sidebar); }
    [data-testid="stSidebar"] .stMarkdown, [data-testid="stSidebar"] label { color: var(--fw-text) !important; font-size: 15px !important; }
    h1, h2, h3 { color: var(--fw-text) !important; border-bottom: 1px solid var(--fw-border); padding-bottom: 0.35rem; font-size: 1.15rem !important; }
    .stButton > button,
    .stButton > button *,
    .stButton > button span,
    .stButton > button:hover,
    .stButton > button:hover *,
    .stButton > button:focus,
    .stButton > button:focus *,
    .stButton > button:active,
    .stButton > button:active * { color: #FFFFFF !important; }
    .stButton > button { background-color: var(--fw-accent) !important; border: none; border-radius: 10px; font-weight: 600; font-size: 15px !important; }
    .stButton > button:hover { background-color: var(--fw-accent-hover) !important; }
    [data-testid="stMetricValue"] { color: var(--fw-accent) !important; font-weight: 700; font-size: 1.2rem !important; }
    [data-testid="stMetricLabel"] { color: var(--fw-muted) !important; font-size: 13px !important; }
    [data-testid="stExpander"] { background-color: var(--fw-card); border: 1px solid var(--fw-border); border-radius: 10px; }
    .stTextInput input, .stNumberInput input { background-color: var(--fw-input) !important; color: var(--fw-text) !important; border: 1px solid var(--fw-border); border-radius: 8px; font-size: 15px !important; }
    p, .stCaption { color: var(--fw-muted) !important; font-size: 15px !important; }
    hr { border-color: var(--fw-border) !important; }
    [data-testid="stDataFrame"] { border-radius: 10px; overflow: hidden; border: 1px solid var(--fw-border); }
    .forgewin-header, .forgewin-header *, .forgewin-header span, .forgewin-header p, .forgewin-header a { color: #FFFFFF !important; }
    .grok-thinking { display: inline-flex; align-items: center; gap: 4px; color: var(--fw-muted); font-size: 15px; }
    .grok-thinking-dots span { animation: grok-bounce 1.4s ease-in-out infinite both; }
    .grok-thinking-dots span:nth-child(1) { animation-delay: 0s; }
    .grok-thinking-dots span:nth-child(2) { animation-delay: 0.2s; }
    .grok-thinking-dots span:nth-child(3) { animation-delay: 0.4s; }
    @keyframes grok-bounce { 0%, 80%, 100% { transform: scale(0.6); opacity: 0.5; } 40% { transform: scale(1); opacity: 1; } }
    </style>
    """, unsafe_allow_html=True)

_apply_forgewin_theme()

# ---------- Login ----------
def _ensure_session_state():
    if "current_user" not in st.session_state:
        st.session_state.current_user = None
    if "grok_chat_history" not in st.session_state:
        st.session_state.grok_chat_history = []
    if "grok_selected_match" not in st.session_state:
        st.session_state.grok_selected_match = None
    if "grok_selected_row" not in st.session_state:
        st.session_state.grok_selected_row = None  # fila con probs para el recuadro de estadísticas
    if "grok_default_response" not in st.session_state:
        st.session_state.grok_default_response = None
    if "todays_matches" not in st.session_state:
        st.session_state.todays_matches = []
    if "selected_fixture_ids" not in st.session_state:
        st.session_state.selected_fixture_ids = []
    if "admin_verify_chat_history" not in st.session_state:
        st.session_state.admin_verify_chat_history = []
    if "admin_verify_pending_images" not in st.session_state:
        st.session_state.admin_verify_pending_images = []

_ensure_session_state()

# ── Restaurar sesión desde token en URL (persiste en reload y redirect MP) ────
if st.session_state.current_user is None:
    try:
        _tok = st.query_params.get(_SESSION_PARAM, "")
        if _tok:
            _u_tok = get_session_user(_tok)
            if _u_tok:
                st.session_state.current_user = {
                    "id": _u_tok["id"],
                    "username": _u_tok["username"],
                    "email": _u_tok.get("email") or "",
                    "role": _u_tok.get("role", "user"),
                    "grok_enabled": bool(_u_tok.get("grok_enabled")),
                    "tier": _u_tok.get("tier") or "base",
                    "credits_balance": int(_u_tok.get("credits_balance") or 0),
                }
            else:
                # Token inválido/expirado — limpiamos el param
                st.query_params.pop(_SESSION_PARAM, None)
    except Exception as _e:
        log.warning("Session token restore error: %s", _e)

# ── Pre-cargar código de referido desde URL (?ref=CODIGO) ─────────────────────
_ref_from_url = st.query_params.get("ref", "")
if _ref_from_url and "pending_referral_code" not in st.session_state:
    st.session_state["pending_referral_code"] = _ref_from_url.upper().strip()

# ── Procesar retorno de Mercado Pago (fallback si el webhook falló) ────────────
_mp_qp = st.query_params
if _mp_qp.get("status") == "approved" and _mp_qp.get("payment_id"):
    try:
        from payment_controller import process_payment_notification
        _mp_ok, _mp_msg = process_payment_notification(str(_mp_qp.get("payment_id")))
        if _mp_ok and st.session_state.current_user:
            _u_fresh = get_user_by_id(st.session_state.current_user["id"])
            if _u_fresh:
                st.session_state.current_user["credits_balance"] = int(_u_fresh.get("credits_balance") or 0)
            st.toast("✅ ¡Pago aprobado! Tus créditos han sido acreditados.", icon="✅")
    except Exception as _e:
        log.exception("Error procesando retorno MP: %s", _e)

# ---------- Página de inicio: solo login centrado cuando no hay sesión ----------
if not st.session_state.current_user:
    # Sidebar mínimo en la página de login
    with st.sidebar:
        st.markdown("**ForgeWin**")
        st.caption("Estadísticas y análisis de partidos")

    # CSS para centrar y dar estilo a la página de login
    st.markdown("""
    <style>
    .login-page-container { max-width: 420px; margin: 2rem auto; padding: 2rem; }
    .login-logo-text {
        font-size: 2.2rem; font-weight: 700; color: #1a73e8;
        text-align: center; letter-spacing: -0.02em; margin-bottom: 0.5rem;
    }
    .login-subtitle { color: #888; text-align: center; font-size: 0.95rem; margin-bottom: 2rem; }
    .login-form-block { background: rgba(44,44,46,0.6); padding: 1.75rem; border-radius: 12px; border: 1px solid #3d3d4d; }
    </style>
    """, unsafe_allow_html=True)

    # Logo: imagen si existe, si no texto ForgeWin
    forgewin_logo_path = os.path.join(os.path.dirname(__file__), "static", "forgewin_logo.png")
    col_logo1, col_logo2, col_logo3 = st.columns([1, 2, 1])
    with col_logo2:
        if os.path.isfile(forgewin_logo_path):
            st.image(forgewin_logo_path, use_container_width=True)
        else:
            st.markdown('<p class="login-logo-text">ForgeWin</p>', unsafe_allow_html=True)
            st.markdown('<p class="login-subtitle">Partidos · Propuestas · Indicadores</p>', unsafe_allow_html=True)

    # Pestañas: Iniciar sesión | Crear cuenta (autoregistro)
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        tab_login, tab_register = st.tabs(["Iniciar sesión", "Crear cuenta"])
        with tab_login:
            with st.form("login_form", clear_on_submit=False):
                st.markdown("**Iniciar sesión**")
                login_user = st.text_input("Usuario", key="login_username", placeholder="Tu usuario", label_visibility="visible")
                login_pass = st.text_input("Contraseña", type="password", key="login_pass", placeholder="Tu contraseña", label_visibility="visible")
                submit = st.form_submit_button("Entrar")
                if submit:
                    u = get_user_by_username(login_user.strip()) if login_user else None
                    if u and verify_password(login_pass, u.get("password_hash", "")):
                        st.session_state.current_user = {
                            "id": u["id"], "username": u["username"], "email": u.get("email") or "",
                            "role": u.get("role", "user"), "grok_enabled": bool(u.get("grok_enabled")),
                            "tier": u.get("tier") or "base", "credits_balance": int(u.get("credits_balance") or 0),
                        }
                        try:
                            _new_token = create_user_session(u["id"])
                            st.query_params[_SESSION_PARAM] = _new_token
                        except Exception:
                            pass
                        st.rerun()
                    elif (login_user or "").strip() or login_pass:
                        st.error("Usuario o contraseña incorrectos.")
        with tab_register:
            with st.form("register_form", clear_on_submit=False):
                st.markdown("**Crear cuenta**")
                st.caption("Perfil inicial: Usuario base. Podrás comprar créditos y subir de perfil después.")
                reg_user = st.text_input("Usuario", key="reg_username", placeholder="Mín. 3 caracteres", label_visibility="visible", max_chars=40)
                reg_email = st.text_input("Email (opcional)", key="reg_email", placeholder="ejemplo@correo.com", label_visibility="visible")
                reg_pass = st.text_input("Contraseña", type="password", key="reg_pass", placeholder="Mín. 8 caracteres", label_visibility="visible")
                reg_pass2 = st.text_input("Confirmar contraseña", type="password", key="reg_pass2", placeholder="Repite la contraseña", label_visibility="visible")
                submit_reg = st.form_submit_button("Crear cuenta")
                if submit_reg:
                    err = None
                    if not (reg_user or "").strip():
                        err = "El usuario es obligatorio."
                    elif len((reg_user or "").strip()) < 3:
                        err = "El usuario debe tener al menos 3 caracteres."
                    elif len((reg_pass or "")) < 8:
                        err = "La contraseña debe tener al menos 8 caracteres."
                    elif (reg_pass or "") != (reg_pass2 or ""):
                        err = "Las contraseñas no coinciden."
                    if err:
                        st.error(err)
                    else:
                        ok, msg = create_user(
                            (reg_user or "").strip(),
                            reg_pass,
                            email=(reg_email or "").strip(),
                            role="user",
                            grok_enabled=False,
                            tier="base",
                        )
                        if ok:
                            st.success("Cuenta creada. Inicia sesión en la pestaña «Iniciar sesión».")
                        else:
                            st.error(msg)

    st.stop()

# ---------- Usuario logueado: sidebar completo y contenido principal ----------
# Refrescar datos de usuario desde BD para tener tier/créditos/grok al día (p. ej. si admin cambió algo)
def _refresh_current_user_from_db():
    uid = (st.session_state.get("current_user") or {}).get("id")
    if not uid:
        return
    u = get_user_by_id(uid)
    if not u:
        return
    cu = st.session_state.current_user
    cu["tier"] = u.get("tier") or "base"
    cu["credits_balance"] = int(u.get("credits_balance") or 0)
    cu["grok_enabled"] = bool(u.get("grok_enabled"))
    cu["role"] = u.get("role") or "user"

_refresh_current_user_from_db()

with st.sidebar:
    u = st.session_state.current_user
    is_admin = u.get("role") == "admin"
    can_plus = _can_plus(u)
    can_v2 = _can_v2(u)

    # ── Brand ────────────────────────────────────────────────────────────────
    st.markdown(
        "<div style='padding:0.4rem 0 0.2rem 0'>"
        "<span style='font-size:1.3rem;font-weight:700;letter-spacing:-0.02em'>⚽ ForgeWin</span>"
        "</div>",
        unsafe_allow_html=True,
    )

    # ── User info ─────────────────────────────────────────────────────────────
    if is_admin:
        st.caption(f"👤 **{u.get('username')}** · Admin")
    else:
        credits = int(u.get("credits_balance") or 0)
        credit_label = f"💳 {credits} crédito{'s' if credits != 1 else ''}"
        st.caption(f"👤 **{u.get('username')}** · {credit_label}")


    if st.button("Cerrar sesión", key="logout_btn", use_container_width=True):
        try:
            _logout_token = st.query_params.get(_SESSION_PARAM, "")
            if _logout_token:
                delete_user_session(_logout_token)
            st.query_params.clear()
        except Exception:
            pass
        st.session_state.current_user = None
        st.session_state.selected_fixture_ids = []
        st.rerun()

    st.divider()

    # ── Mode options (resolved before rendering nav) ──────────────────────────
    if is_admin:
        # Admin ve todo
        mode_options = [
            "Conoce los planes",
            "Partidos del día (top 10 ligas)",
            "V2",
            "V3",
            "Historial de propuestas",
            "Buscar propuesta",
            "Mis Referidos",
            "Administración",
        ]
    else:
        # MVP: secciones para usuarios
        mode_options = [
            "Partidos del día (top 10 ligas)",
            "V3",
            "Historial de propuestas",
            "Conoce los planes",
            "Mis Referidos",
        ]

    # Handle pending mode requests (set by league buttons etc.)
    if st.session_state.pop("pending_mode_partidos_dia", False):
        st.session_state["mode_radio"] = "Partidos del día (top 10 ligas)"
    elif st.session_state.pop("pending_mode_buscar", False):
        if "Buscar propuesta" in mode_options:
            st.session_state["mode_radio"] = "Buscar propuesta"
    elif st.session_state.pop("pending_mode_historial", False):
        if "Historial de propuestas" in mode_options:
            st.session_state["mode_radio"] = "Historial de propuestas"

    _current_mode = st.session_state.get("mode_radio")
    if _current_mode not in mode_options:
        _current_mode = "Partidos del día (top 10 ligas)"
        st.session_state["mode_radio"] = _current_mode
    mode = _current_mode

    # Clear league/combined state when navigating away from Partidos del día
    if mode != "Partidos del día (top 10 ligas)":
        if st.session_state.get("view_league"):
            st.session_state.pop("view_league", None)
        if st.session_state.get("view_combined_leagues"):
            log.info("MODO: cambió a %s, limpiando view_combined_leagues", mode)
            st.session_state.pop("view_combined_leagues", None)

    def _nav_btn(label, target_mode):
        """Sidebar navigation button; bold when active."""
        is_active = (mode == target_mode and
                     not st.session_state.get("view_league") and
                     not st.session_state.get("view_combined_leagues"))
        btn_label = f"**{label}**" if is_active else label
        if st.button(btn_label, key=f"nav__{target_mode}", use_container_width=True):
            st.session_state.pop("view_league", None)
            st.session_state.pop("view_combined_leagues", None)
            st.session_state["mode_radio"] = target_mode
            st.rerun()

    # ── ANÁLISIS ──────────────────────────────────────────────────────────────
    st.caption("**ANÁLISIS**")
    _nav_btn("📅 Partidos de hoy", "Partidos del día (top 10 ligas)")
    _nav_btn("🔥 Dale con todo con esteroides", "V3")

    if is_admin:
        _nav_btn("🔁 V2 (clásico)", "V2")

    st.divider()
    st.caption("**MIS ANÁLISIS**")
    _nav_btn("📋 Historial", "Historial de propuestas")
    _nav_btn("🤝 Mis Referidos", "Mis Referidos")
    if is_admin:
        _nav_btn("🔎 Buscar por ID", "Buscar propuesta")

    if is_admin:
        st.divider()
        _nav_btn("⚙️ Administración", "Administración")

    st.divider()
    _nav_btn("💳 Créditos", "Conoce los planes")

    st.divider()

    # ── LIGAS ─────────────────────────────────────────────────────────────────
    st.caption("**LIGAS**")

    # Show basket count badge if user has cross-league selections
    _basket_count = len(st.session_state.get("league_selected_by_league", {}).get("combinada") or [])
    if _basket_count > 0:
        st.caption(f"🗂️ Lista combinada: **{_basket_count}** partido{'s' if _basket_count != 1 else ''} en cesta")

    use_mock = st.checkbox("Datos mock (sin API fútbol)", value=False)

    view_league = st.session_state.get("view_league")
    league_sel = view_league
    league_code = LEAGUES.get(league_sel) if league_sel else None

    for league_name in LEAGUES.keys():
        code = LEAGUES[league_name]
        logo_col, name_col = st.columns([0.2, 0.8])
        with logo_col:
            _show_league_logo(code, 28)
        with name_col:
            _is_active_league = (view_league == league_name)
            _lbl = f"**{league_name}**" if _is_active_league else league_name
            if st.button(_lbl, key=f"liga_btn_{code}", use_container_width=True):
                st.session_state["view_league"] = league_name
                st.session_state.pop("view_combined_leagues", None)
                st.session_state["pending_mode_partidos_dia"] = True
                st.rerun()

    st.divider()
    st.caption("**Lista combinada** (análisis cruzado por ligas)")
    if True:
        combined_default = st.session_state.get("view_combined_leagues") or []
        combined_selection = st.multiselect("Ligas para una sola lista", options=list(LEAGUES.keys()), default=combined_default, key="multiselect_combined_leagues", label_visibility="collapsed")
        if st.button("Ver lista combinada", key="btn_combined_leagues", use_container_width=True):
            log.info("LISTA_COMBINADA: usuario pulsó Ver lista combinada, combined_selection=%s", combined_selection)
            if combined_selection:
                st.session_state["view_combined_leagues"] = combined_selection
                st.session_state.pop("view_league", None)
                st.session_state["pending_mode_partidos_dia"] = True
                log.info("LISTA_COMBINADA: view_combined_leagues=%s, rerun", combined_selection)
                st.rerun()
            else:
                st.warning("Selecciona al menos una liga.")

    st.divider()

    # ── Comprar créditos ──────────────────────────────────────────────────────
    if u.get("role") != "admin":
        try:
            from payment_controller import is_configured, create_preference_for_pack, list_packs
            if is_configured():
                with st.expander("💳 Comprar créditos"):
                    user_email = u.get("email") or ""
                    if not user_email:
                        user_email = st.text_input("Tu email (para Mercado Pago)", placeholder="correo@ejemplo.com", key="mp_email")
                    if user_email:
                        st.caption("Packs de créditos")
                        for p in list_packs():
                            if st.button(f"{p['credits']} cr. - ${p['amount']:,}".replace(",", "."), key=f"pack_{p['id']}"):
                                init_point, err = create_preference_for_pack(u["id"], p["id"], user_email)
                                if init_point:
                                    st.link_button("Ir a pagar", init_point)
                                else:
                                    st.error(err)
            else:
                st.caption("Pagos no configurados (MERCADOPAGO_ACCESS_TOKEN)")
        except ImportError:
            pass

    # ── Reportar error ────────────────────────────────────────────────────────
    with st.expander("🐛 Reportar problema"):
        st.caption("Si algo falló, cuéntanos qué estabas haciendo y el mensaje de error.")
        report_context = st.text_input("¿Qué estabas haciendo?", key="err_ctx", placeholder="Ej: Análisis Pro, Generar PDF…")
        report_error_msg = st.text_area("Mensaje de error (si apareció)", key="err_msg", placeholder="Opcional.")
        report_comment = st.text_area("Comentario adicional (opcional)", key="err_comment")
        if st.button("Enviar reporte", key="btn_send_error_report"):
            try:
                uid = (st.session_state.get("current_user") or {}).get("id")
                username = (st.session_state.get("current_user") or {}).get("username") or ""
                current_mode_rep = st.session_state.get("mode_radio") or ""
                report_id = save_error_report(
                    user_id=uid,
                    username=username,
                    context=(report_context or "").strip(),
                    error_message=(report_error_msg or "").strip(),
                    comment=(report_comment or "").strip(),
                    mode_or_screen=current_mode_rep,
                )
                log.info("error_report: id=%s user=%s context=%s", report_id, username, (report_context or "")[:80])
                st.success("Gracias. Tu reporte se ha guardado.")
            except Exception as e:
                log.exception("Error guardando reporte de usuario: %s", e)
                st.error("No se pudo enviar el reporte. Intenta más tarde.")

# Barra superior ForgeWin: fondo gris oscuro, acento azul, tipografía 14-16px
st.markdown("""
<div class="forgewin-header" style="background: #2C2C2E; padding: 0.85rem 1.25rem; border-radius: 10px; margin-bottom: 1.25rem; border: 1px solid #3d3d4d;">
  <span style="font-size: 1.4rem; font-weight: 700; color: #FFFFFF !important; letter-spacing: -0.02em;">ForgeWin</span>
  <span style="color: #A0A0A0 !important; font-size: 15px; margin-left: 1rem;">Partidos · Propuestas · Indicadores</span>
</div>
""", unsafe_allow_html=True)

with st.expander("¿Cómo usar?"):
    st.markdown("""
    1. **Partidos del día:** partidos de hoy (o mañana). Marca hasta 10 partidos y pulsa **Consultar propuesta** para generar un análisis (Alfred o Reginald) y recomendación. Puedes descargar PDF.
    2. **Competencias (izquierda):** haz clic en una liga para ver sus próximos partidos con probabilidades.
    3. **Buscar propuesta:** introduce el ID de una propuesta para verla y comparar con el resultado real.
    """)


# Contenido principal (una columna, sin tabla de posiciones)
main_col = st.columns([1.0])[0]
with main_col:
    view_combined_leagues = st.session_state.get("view_combined_leagues") or []
    if view_combined_leagues and not _can_plus(st.session_state.current_user):
        log.info("LISTA_COMBINADA: usuario NO es plus (tier=%s), limpiando view_combined_leagues", st.session_state.current_user.get("tier"))
        st.session_state.pop("view_combined_leagues", None)
        view_combined_leagues = []
    # ---------- Vista lista combinada (varias ligas): navegación por liga + cesta persistente ----------
    if view_combined_leagues:
        log.info("LISTA_COMBINADA: mostrando vista combinada, ligas=%s", view_combined_leagues)
        if "league_selected_by_league" not in st.session_state:
            st.session_state["league_selected_by_league"] = {}
        if "combinada_match_dicts" not in st.session_state:
            st.session_state["combinada_match_dicts"] = {}
        MAX_LEAGUE_SELECT = 10

        # ── on_change callback: updates basket immediately so counter is correct ──
        def _on_combinada_checkbox_change(fid, match_dict, checkbox_key):
            """Called by on_change on each checkbox; keeps basket in sync before rerender."""
            basket_now = list(st.session_state["league_selected_by_league"].get("combinada") or [])
            match_dicts_now = dict(st.session_state.get("combinada_match_dicts") or {})
            is_now_checked = bool(st.session_state.get(checkbox_key))
            if is_now_checked:
                if fid not in basket_now and len(basket_now) < MAX_LEAGUE_SELECT:
                    basket_now.append(fid)
                    match_dicts_now[fid] = match_dict
            else:
                if fid in basket_now:
                    basket_now.remove(fid)
                    match_dicts_now.pop(fid, None)
            st.session_state["league_selected_by_league"]["combinada"] = basket_now
            st.session_state["combinada_match_dicts"] = match_dicts_now

        # Read basket AFTER any pending on_change callbacks have run
        basket = st.session_state["league_selected_by_league"].get("combinada") or []
        combinada_match_dicts = st.session_state.get("combinada_match_dicts") or {}
        n_badge = len(basket)

        st.subheader("Partidos – análisis cruzado (varias ligas)")
        # Placeholder: filled at the END of the combined view block so it shows the updated count
        _counter_placeholder = st.empty()
        _counter_placeholder.metric("Partidos en tu lista", f"{n_badge} de {MAX_LEAGUE_SELECT}", "")
        st.caption("Elige una liga, marca partidos y cambia de liga cuando quieras. La selección se mantiene.")
        if st.button("← Volver al inicio", key="back_combined_view"):
            st.session_state.pop("view_combined_leagues", None)
            st.session_state.pop("combinada_match_dicts", None)
            st.session_state.pop("combinada_matches_cache", None)
            st.rerun()

        # Selector de liga (solo las seleccionadas para combinada)
        if st.session_state.get("combinada_current_league") not in view_combined_leagues:
            st.session_state["combinada_current_league"] = view_combined_leagues[0]
        current_league_name = st.radio(
            "Ver partidos de:",
            options=view_combined_leagues,
            index=view_combined_leagues.index(st.session_state["combinada_current_league"]),
            key="combinada_current_league",
            horizontal=True,
        )

        # ---------- Lista completa de partidos seleccionados (arriba, siempre visible) ----------
        st.markdown("---")
        st.markdown("**Tus partidos seleccionados**")
        if not basket:
            st.caption("Aún no has seleccionado ninguno. Elige una liga abajo y marca los partidos que quieras incluir.")
        else:
            header = st.columns([0.22, 0.35, 0.12, 0.11, 0.2])
            header[0].caption("**Liga**")
            header[1].caption("**Partido**")
            header[2].caption("**Día**")
            header[3].caption("**Hora**")
            header[4].caption("**Quitar**")
            for fid in basket:
                m = combinada_match_dicts.get(fid)
                if m:
                    dia, hora = format_date_chile(str(m.get("date") or m.get("datetime") or ""))
                    liga_n = m.get("league_name") or "—"
                    row = st.columns([0.22, 0.35, 0.12, 0.11, 0.2])
                    row[0].write(liga_n)
                    row[1].write(f"{m.get('home_team') or '—'} vs {m.get('away_team') or '—'}")
                    row[2].write(dia)
                    row[3].write(hora)
                    with row[4]:
                        if st.button("Quitar", key=f"quitar_combinada_{fid}"):
                            new_basket = [x for x in basket if x != fid]
                            st.session_state["league_selected_by_league"]["combinada"] = new_basket
                            combinada_match_dicts.pop(fid, None)
                            st.session_state["combinada_match_dicts"] = combinada_match_dicts
                            st.rerun()
                else:
                    row = st.columns([0.7, 0.3])
                    row[0].caption(f"Partido {fid} (sin datos; quita y vuelve a seleccionar en su liga).")
                    with row[1]:
                        if st.button("Quitar", key=f"quitar_combinada_{fid}"):
                            new_basket = [x for x in basket if x != fid]
                            st.session_state["league_selected_by_league"]["combinada"] = new_basket
                            st.session_state["combinada_match_dicts"] = {k: v for k, v in combinada_match_dicts.items() if k != fid}
                            st.rerun()
        # Botón "Generar análisis" visible aquí (sin bajar) cuando hay partidos
        # basket is already in sync via on_change callbacks
        if basket:
            st.markdown("**¿Listo para analizar?**")
            league_can_run_top = GEMINI_API_KEY and _has_any_credit(st.session_state.current_user)
            if league_can_run_top:
                if st.button("🔥 Analizar seleccionados", key="btn_league_analysis_combined_top", type="primary"):
                    st.session_state["run_combined_analysis"] = True
                    st.rerun()
            else:
                st.caption("Configura GEMINI_API_KEY en .env para poder generar el análisis.")
        st.markdown("---")

        code_current = LEAGUES.get(current_league_name)
        if not code_current:
            log.warning("LISTA_COMBINADA: liga sin código: current_league_name=%s", current_league_name)
            st.warning("Liga sin código.")
        else:
            cache_key = f"combinada_{code_current}_{use_mock}"
            if "combinada_matches_cache" not in st.session_state:
                st.session_state["combinada_matches_cache"] = {}
            if cache_key not in st.session_state["combinada_matches_cache"]:
                with st.spinner(f"Cargando partidos de {current_league_name}..."):
                    try:
                        matches = get_upcoming_matches([code_current], days_ahead=7, use_mock=use_mock)
                        log.info("LISTA_COMBINADA: cargados %d partidos para %s (code=%s)", len(matches or []), current_league_name, code_current)
                        for m in matches:
                            m["league_name"] = m.get("league_name") or current_league_name
                            m["league_code"] = m.get("league_id") or code_current
                        matches = _normalize_and_dedupe_matches(matches)
                        st.session_state["combinada_matches_cache"][cache_key] = matches
                    except Exception as e:
                        log.exception("Error cargando partidos combinada %s: %s", code_current, e)
                        st.error("Error al cargar partidos.")
                        matches = []
            else:
                matches = st.session_state["combinada_matches_cache"][cache_key]
            current_league_fids = {m.get("fixture_id") for m in matches}
            checked_in_current = []
            current_league_selected_dict = {}
            if matches:
                st.markdown(f"**{current_league_name}** — marca los partidos que quieras incluir en el análisis.")
                all_current_league_fids = [m.get("fixture_id") for m in matches if m.get("fixture_id")]
                if st.session_state.pop("show_max_10_comb_select_all", None):
                    st.warning("Se pueden analizar como máximo 10 partidos en total. Se han seleccionado hasta 10.")
                if all_current_league_fids and st.button("Seleccionar todo (esta liga)", key=f"btn_select_all_comb_{code_current}"):
                    other_fids = [f for f in basket if f not in current_league_fids]
                    remaining = max(0, MAX_LEAGUE_SELECT - len(other_fids))
                    new_basket_sel = other_fids + all_current_league_fids[:remaining]
                    st.session_state["league_selected_by_league"]["combinada"] = new_basket_sel
                    if len(all_current_league_fids) > remaining:
                        st.session_state["show_max_10_comb_select_all"] = True
                    st.rerun()
                header_cols = st.columns([0.06, 0.32, 0.32, 0.15, 0.15])
                for i, lb in enumerate(["Incluir", "Local", "Visitante", "Día", "Hora"]):
                    header_cols[i].caption(lb)
                for idx, m in enumerate(matches):
                    fid = m.get("fixture_id")
                    dia, hora = format_date_chile(str(m.get("date") or m.get("datetime") or ""))
                    c0, c1, c2, c3, c4 = st.columns([0.06, 0.32, 0.32, 0.15, 0.15])
                    _cb_key = f"liga_sel_combinada_{code_current}_{fid}"
                    _match_snapshot = dict(m)
                    with c0:
                        # on_change updates the basket immediately before the next render,
                        # which fixes both the counter and the cross-league persistence.
                        st.checkbox(
                            "",
                            value=(fid in basket),
                            key=_cb_key,
                            label_visibility="collapsed",
                            on_change=_on_combinada_checkbox_change,
                            args=(fid, _match_snapshot, _cb_key),
                        )
                        # Collect current checked state for local basket rebuild
                        if st.session_state.get(_cb_key):
                            if fid not in checked_in_current:
                                checked_in_current.append(fid)
                                current_league_selected_dict[fid] = _match_snapshot
                    c1.write(m.get("home_team") or "—")
                    c2.write(m.get("away_team") or "—")
                    c3.write(dia)
                    c4.write(hora)
                # Basket is already kept up-to-date by on_change callbacks.
                # Re-read the authoritative basket from session state.
                new_basket = list(st.session_state["league_selected_by_league"].get("combinada") or [])
                combinada_match_dicts = dict(st.session_state.get("combinada_match_dicts") or {})
                log.info("LISTA_COMBINADA: liga=%s, basket=%s", current_league_name, len(new_basket))
            else:
                st.info(f"No hay partidos en **{current_league_name}** en los próximos 7 días.")
                new_basket = basket

            # Re-read authoritative basket (updated by on_change callbacks above)
            league_selected = st.session_state["league_selected_by_league"].get("combinada") or []
            n_league_sel = len(league_selected)
            combinada_match_dicts = st.session_state.get("combinada_match_dicts") or {}

            # Update the counter placeholder with the correct, current count
            _counter_placeholder.metric("Partidos en tu lista", f"{n_league_sel} de {MAX_LEAGUE_SELECT}", "")

            st.markdown("---")
            if n_league_sel >= MAX_LEAGUE_SELECT:
                st.caption("Has llegado al máximo (10). Desmarca alguno arriba en «Tus partidos seleccionados» o en la tabla de la liga para añadir otro.")

            st.markdown("---")
            st.subheader("Analizar partidos seleccionados")
            balance_comb = int(st.session_state.current_user.get("credits_balance") or 0) if st.session_state.current_user and st.session_state.current_user.get("role") != "admin" else 999
            st.caption("Cuando tengas hasta 10 partidos seleccionados, pulsa el botón para analizar con Alfred y Reginald.")
            if n_league_sel > 0 and balance_comb > 0 and n_league_sel > balance_comb and st.session_state.current_user and st.session_state.current_user.get("role") != "admin":
                faltantes_comb = n_league_sel - balance_comb
                st.warning(f"Tienes **{balance_comb} créditos**. Selecciona solo hasta {balance_comb} partidos, o el sistema analizará **{balance_comb} partidos elegidos aleatoriamente** de los {n_league_sel} seleccionados.")
                st.caption("O compra los créditos faltantes:")
                _render_buy_credits_ui(faltantes_comb, "buy_cr_comb")

            def _league_match_data(rows_list, fids):
                out = []
                for fid in fids:
                    r = next((x for x in rows_list if x.get("fixture_id") == fid), None)
                    if r:
                        out.append(dict(
                            home_team=r.get("home"), away_team=r.get("away"), fixture_id=fid,
                            home_team_id=r.get("home_team_id"), away_team_id=r.get("away_team_id"),
                            league_code=r.get("league_code"), league_name=r.get("league"), date=r.get("date"),
                        ))
                return out

            def _league_db_context(match_data_list):
                if not match_data_list:
                    return None
                cache_form = {}
                cache_h2h = {}
                cache_team_id = {}
                parts = []
                for m in match_data_list:
                    fid = m.get("fixture_id")
                    home = m.get("home_team") or "Local"
                    away = m.get("away_team") or "Visitante"
                    home_id = m.get("home_team_id")
                    away_id = m.get("away_team_id")
                    lcode = m.get("league_code") or ""
                    part = [f"{home} vs {away}:"]
                    try:
                        use_fallback = fid is not None and int(fid) >= FALLBACK_FIXTURE_ID_MIN
                    except (TypeError, ValueError):
                        use_fallback = False
                    try:
                        if use_fallback:
                            as_home_id, as_away_id = home_id, away_id
                        else:
                            if home not in cache_team_id:
                                cache_team_id[home] = get_team_id_by_name(home)
                            if away not in cache_team_id:
                                cache_team_id[away] = get_team_id_by_name(away)
                            as_home_id, as_away_id = cache_team_id[home], cache_team_id[away]
                        # Forma: solo tabla historical_matches (master_table_checked → historical_matches)
                        form_h = None
                        if get_recent_form_historical and (lcode or "").strip():
                            try:
                                form_h = get_recent_form_historical(team_id=as_home_id, team_name=home, league_id=lcode, last_n=5, use_master_checked=True)
                            except Exception:
                                pass
                        if form_h:
                            part.append(f"  Forma local: {', '.join([str(f.get('goals_for', '?')) for f in form_h])}")
                        form_a = None
                        if get_recent_form_historical and (lcode or "").strip():
                            try:
                                form_a = get_recent_form_historical(team_id=as_away_id, team_name=away, league_id=lcode, last_n=5, use_master_checked=True)
                            except Exception:
                                pass
                        if form_a:
                            part.append(f"  Forma visitante: {', '.join([str(f.get('goals_for', '?')) for f in form_a])}")
                        h2h = []
                        if get_h2h_historical and (lcode or "").strip():
                            try:
                                h2h = get_h2h_historical(home_id=as_home_id, away_id=as_away_id, home_name=home, away_name=away, league_id=lcode, last_n=5, use_master_checked=True)
                            except Exception:
                                pass
                        if not h2h and as_home_id and as_away_id:
                            h2h_key = (min(as_home_id, as_away_id), max(as_home_id, as_away_id))
                            if h2h_key not in cache_h2h:
                                cache_h2h[h2h_key] = get_h2h_api_sports(as_home_id, as_away_id, limit=5)
                            h2h = cache_h2h[h2h_key]
                        if not h2h and fid and not use_fallback:
                            h2h = get_h2h(fid, limit=5, use_mock=False)
                        if h2h:
                            h2h_str = _format_h2h_string(h2h)
                            part.append(f"  H2H: {h2h_str}")
                    except Exception:
                        log.exception("_league_db_context: error forma/H2H para %s vs %s (fid=%s)", home, away, fid)
                    parts.append("\n".join(part))
                return "\n\n".join(parts) if parts else None

            league_can_run = n_league_sel > 0 and GEMINI_API_KEY and _has_any_credit(st.session_state.current_user)
            selected_match_dicts = [combinada_match_dicts[fid] for fid in league_selected if fid in combinada_match_dicts]
            league_selected_valid = [fid for fid in league_selected if fid in combinada_match_dicts]
            run_analysis_trigger = st.session_state.pop("run_combined_analysis", False) or (league_can_run and st.button("Generar análisis (V2 + Opinión recomendada)", key="btn_league_analysis_combined", type="primary"))
            if run_analysis_trigger and league_can_run:
                import time
                if not selected_match_dicts:
                    st.error("No hay datos de los partidos seleccionados. Quita y vuelve a seleccionar en cada liga.")
                elif len(selected_match_dicts) != len(league_selected):
                    st.warning("Algunos partidos ya no tienen datos; se analizarán solo los que sí. Quítalos de «Ver partidos seleccionados» si quieres.")
                if selected_match_dicts:
                    ids_to_analyze_comb = _effective_ids_to_analyze(league_selected_valid, st.session_state.current_user)
                    selected_match_dicts = [combinada_match_dicts[fid] for fid in ids_to_analyze_comb]
                    with st.spinner("Calculando probabilidades para los partidos seleccionados..."):
                        rows = analyze_matches(selected_match_dicts, use_mock=use_mock)
                    match_data_league = _league_match_data(rows, ids_to_analyze_comb)
                    db_ctx_league = _league_db_context(match_data_league)
                    start_league = time.time()
                    activity_league = []
                    progress_box_league = st.container()
                    with progress_box_league:
                        st.markdown("""
                        <div style="background:#2C2C2E;border:1px solid #3d3d4d;border-radius:10px;padding:1.2rem 1.5rem;margin:0.5rem 0;">
                        <p style="margin:0 0 0.5rem 0;font-weight:600;color:#6495ED;font-size:16px;">Análisis por liga (combinado)</p>
                        <p style="margin:0 0 0.8rem 0;font-size:15px;color:#A0A0A0;">Alfred → Reginald → Consenso → Reginald V2</p>
                        </div>
                        """, unsafe_allow_html=True)
                        prog_league = st.progress(0)
                        step_ph = st.empty()
                        time_ph = st.empty()
                        log_ph = st.empty()
                    def _up(step, msg, log_line):
                        elapsed = int(time.time() - start_league)
                        activity_league.append(f"[{elapsed}s] {log_line}")
                        prog_league.progress((step + 1) / 4)
                        step_ph.caption(
                            f"**Paso {step + 1} de 4** — {msg}\n\n_{_random_waiting_message()}_"
                        )
                        time_ph.caption(f"⏱ **{elapsed} s**")
                        with log_ph.container():
                            for line in activity_league:
                                st.caption(line)
                    try:
                        _up(0, "Alfred y Reginald analizan en paralelo...", "Ambos.")
                        result_g_league, result_m_league = _run_proposals_parallel(match_data_league, db_ctx_league)
                        v2_grok = result_g_league.get("analysis") or ""
                        v2_stats = result_g_league.get("stats_by_fixture") or {}
                        gemini_full = result_m_league.get("analysis") or ""
                        _up(0, "Ambos listos.", "Ambos listos.")
                        _up(2, "Propuesta General 1+2 (consenso)...", "Consenso.")
                        from consensus import run_consensus_flow
                        consensus_league = run_consensus_flow(v2_grok, gemini_full, match_data_league)
                        _up(2, "Consenso listo.", "Consenso listo.")
                        table_lines_league = []
                        for fid in ids_to_analyze_comb:
                            r = next((x for x in rows if x.get("fixture_id") == fid), {})
                            s = v2_stats.get(fid) or {}
                            p1, px, p2 = _norm_prob_display(s.get("prob_home_win")), _norm_prob_display(s.get("prob_draw")), _norm_prob_display(s.get("prob_away_win"))
                            xg, vb = s.get("expected_goals"), s.get("value_bet") or "—"
                            v1 = f"{p1:.0%}" if p1 is not None else "—"
                            vx = f"{px:.0%}" if px is not None else "—"
                            v2 = f"{p2:.0%}" if p2 is not None else "—"
                            xg_str = f"{xg:.1f}" if xg is not None else "—"
                            table_lines_league.append(
                                f"- {r.get('home') or 'Local'} vs {r.get('away') or 'Visitante'}: V.1={v1} X={vx} V.2={v2} xG={xg_str} Value={vb}"
                            )
                        user_v2 = "**Tabla Alfred:**\n\n" + "\n".join(table_lines_league) + "\n\n---\n\n**Análisis Alfred:**\n\n" + (v2_grok or "")
                        sys_v2 = "Eres Reginald, experto en fútbol y análisis. Analiza la tabla y el análisis de Alfred y da tu opinión en markdown. Responde solo en markdown."
                        _up(3, "Reginald V2 (análisis sobre Alfred)...", "Reginald V2.")
                        v2_reginald = ask_gemini_custom(sys_v2, user_v2)
                        _up(3, "Listo.", "Listo.")
                        fixture_to_match_league = {r.get("fixture_id"): dict(r, home_team=r.get("home"), away_team=r.get("away")) for r in rows if r.get("fixture_id") in ids_to_analyze_comb}
                        st.session_state["league_analysis"] = {
                            "league_code": "combinada",
                            "match_ids": list(ids_to_analyze_comb),
                            "v2_grok_analysis": v2_grok,
                            "v2_grok_stats": v2_stats,
                            "v2_gemini_analysis": v2_reginald or "",
                            "consensus": consensus_league,
                            "fixture_to_match": fixture_to_match_league,
                        }
                        prog_league.progress(1.0)
                        step_ph.caption("✅ **Completado**")
                        _deduct_credits_and_refresh(len(ids_to_analyze_comb))
                        st.success("Análisis generado. Revisa las secciones abajo y genera el PDF.")
                        st.rerun()
                    except Exception as e_league:
                        log.exception("Análisis liga combinada: %s", e_league)
                        activity_league.append(f"[{int(time.time()-start_league)}s] Error: {e_league}")
                        with log_ph.container():
                            for line in activity_league:
                                st.caption(line)
                        st.error(str(e_league))
                        with st.expander("Detalle"):
                            st.code(traceback.format_exc())

                la = st.session_state.get("league_analysis") or {}
                la_match_ids = set(la.get("match_ids") or [])
                show_combinada_results = la.get("league_code") == "combinada" and la_match_ids and (la_match_ids <= set(league_selected) or la.get("match_ids") == league_selected)
                if show_combinada_results:
                    st.markdown("---")
                    st.subheader("ForgeWin V2 (lista combinada)")
                    ft_league = la.get("fixture_to_match") or {}
                    v2_st = la.get("v2_grok_stats") or {}
                    if v2_st and ft_league:
                        st.markdown("**Datos por partido (Alfred)**")
                        for fid in la.get("match_ids", []):
                            m = ft_league.get(fid) or {}
                            s = v2_st.get(fid) or {}
                            p1, px, p2 = _norm_prob_display(s.get("prob_home_win")), _norm_prob_display(s.get("prob_draw")), _norm_prob_display(s.get("prob_away_win"))
                            xg, vb = s.get("expected_goals"), s.get("value_bet") or "—"
                            v1 = f"{p1:.0%}" if p1 is not None else "—"
                            vx = f"{px:.0%}" if px is not None else "—"
                            v2 = f"{p2:.0%}" if p2 is not None else "—"
                            xg_str = f"{xg:.1f}" if xg is not None else "—"
                            st.caption(f"{m.get('home_team') or m.get('home')} vs {m.get('away_team') or m.get('away')}: V.1={v1} X={vx} V.2={v2} xG={xg_str} Value={vb}")
                    with st.expander("Análisis Alfred (V2)", expanded=False):
                        st.markdown(la.get("v2_grok_analysis") or "")
                    with st.expander("Análisis Reginald (V2)", expanded=True):
                        st.markdown(la.get("v2_gemini_analysis") or "")
                    st.subheader("ForgeWin - Opinión recomendada")
                    with st.expander("Análisis y recomendación final", expanded=True):
                        st.markdown((la.get("consensus") or {}).get("analysis") or "")
                    full_id_league = f"combinada_{len(la.get('match_ids', []))}"
                    if _can_plus(st.session_state.current_user):
                        if st.button("Generar y descargar PDF", key="btn_league_pdf_combined"):
                            with st.spinner("Generando PDF..."):
                                path_league = generate_proposal_pdf_league_combined(
                                    full_id_league,
                                    la["fixture_to_match"],
                                    la["match_ids"],
                                    la["v2_grok_stats"],
                                    la["v2_grok_analysis"],
                                    la["v2_gemini_analysis"],
                                    la.get("consensus") or {},
                                )
                                with open(path_league, "rb") as f:
                                    pdf_bytes_league = f.read()
                            st.session_state["league_pdf_bytes"] = pdf_bytes_league
                            st.session_state["league_pdf_filename"] = f"forgewin_liga_{full_id_league}.pdf"
                            st.session_state["pdf_just_generated_league_combined"] = True
                            st.rerun()
                        if st.session_state.get("league_pdf_bytes") and st.session_state.get("league_pdf_filename") and la.get("league_code") == "combinada":
                            st.success("PDF listo. Si no se descargó automáticamente, haz clic en **Descargar nuevamente**.")
                            _trigger_auto_download_once("pdf_just_generated_league_combined")
                            st.download_button("Descargar nuevamente", data=st.session_state["league_pdf_bytes"], file_name=st.session_state["league_pdf_filename"], mime="application/pdf", key="dl_league_pdf_combined")
                    else:
                        st.caption("La descarga de PDF está disponible en perfil **Usuario+**.")
                elif n_league_sel > 0 and league_can_run:
                    st.info("Selecciona partidos arriba y pulsa **Generar análisis (V2 + Opinión recomendada)**.")

    # ---------- Vista por liga (clic en competencia) ----------
    elif view_league and league_code:
        # EL, CL, CLI: ventana ampliada (60 días) por defecto, partidos más espaciados
        is_cup_league = league_code in ("EL", "CL", "CLI")
        use_extended_days = st.session_state.get("league_upcoming_days") == league_code or is_cup_league
        days_ahead = 60 if is_cup_league else (30 if use_extended_days else 7)
        st.subheader(f"Próximos partidos – {league_sel}")
        if st.button("← Volver al inicio", key="back_league_view"):
            st.session_state.pop("view_league", None)
            st.session_state.pop("league_upcoming_days", None)
            st.session_state.pop("league_matches_cache", None)
            # No borrar league_selected_by_league: la selección de cada liga se conserva
            st.rerun()
        if use_extended_days and not is_cup_league:
            if st.button("← Volver a próximos 7 días", key="back_league_7d"):
                st.session_state["league_upcoming_days"] = None
                st.rerun()
            st.caption("Mostrando próximos 30 días.")
        elif is_cup_league:
            st.caption("Mostrando próximos 60 días (competiciones con partidos espaciados).")
        _show_league_logo(league_code, 50)
        cache_key_league = f"league_matches_{league_code}_{days_ahead}_{use_mock}"
        if "league_matches_cache" not in st.session_state:
            st.session_state["league_matches_cache"] = {}
        if cache_key_league not in st.session_state["league_matches_cache"]:
            with st.spinner("Cargando listado de partidos..."):
                try:
                    matches = get_upcoming_matches([league_code], days_ahead=days_ahead, use_mock=use_mock)
                    st.session_state["league_matches_cache"][cache_key_league] = matches
                except Exception as e:
                    log.exception("Error cargando partidos liga=%s: %s", league_code, e)
                    st.error("Error al cargar partidos. Revisa la consola donde ejecutas Streamlit o el detalle abajo.")
                    with st.expander("Ver detalle del error (log)", expanded=True):
                        st.code(traceback.format_exc(), language="text")
                    st.stop()
        else:
            matches = st.session_state["league_matches_cache"][cache_key_league]
        if not matches:
            st.info("**No hay partidos para mostrar.**")
            log.info("Partidos vacíos para liga=%s", league_code)
            if league_code == "EL":
                st.warning("**Europa League** podría requerir un plan de API de pago en [football-data.org](https://www.football-data.org/coverage). El plan gratuito incluye Champions League pero no Europa League. Si tienes API key de otro proveedor, no aplica.")
            elif not is_cup_league:
                if st.button("¿Quieres ver los próximos partidos disponibles? (buscar en 30 días)", key=f"upcoming_liga_{league_code}"):
                    st.session_state["league_upcoming_days"] = league_code
                    st.rerun()
        else:
            if "league_selected_by_league" not in st.session_state:
                st.session_state["league_selected_by_league"] = {}
            current_fids = {m.get("fixture_id") for m in matches}
            # Solo leer/actualizar la liga actual; las demás ligas conservan su selección al cambiar de liga
            league_selected = st.session_state["league_selected_by_league"].get(league_code) or []
            league_selected = [f for f in league_selected if f in current_fids]
            st.session_state["league_selected_by_league"][league_code] = league_selected
            MAX_LEAGUE_SELECT = 10
            st.caption("Listado de partidos. Marca los que quieras analizar (máx. 10). El contador y el botón están bajo la lista.")
            all_fids_league = [m.get("fixture_id") for m in matches if m.get("fixture_id")]
            if st.session_state.pop("show_max_10_league_select_all", None):
                st.warning("Se pueden analizar como máximo 10 partidos. Se han seleccionado los primeros 10.")
            if all_fids_league and st.button("Seleccionar todo", key=f"btn_select_all_liga_{league_code}"):
                st.session_state["league_selected_by_league"][league_code] = all_fids_league[:MAX_LEAGUE_SELECT]
                if len(all_fids_league) > MAX_LEAGUE_SELECT:
                    st.session_state["show_max_10_league_select_all"] = True
                st.rerun()
            checked_league = []
            header_cols = st.columns([0.06, 0.30, 0.30, 0.17, 0.17])
            for i, lb in enumerate(["Incluir", "Local", "Visitante", "Día", "Hora"]):
                header_cols[i].caption(lb)
            for idx, m in enumerate(matches):
                fid = m.get("fixture_id")
                dia, hora = format_date_chile(str(m.get("date") or m.get("datetime") or ""))
                c0, c1, c2, c3, c4 = st.columns([0.06, 0.30, 0.30, 0.17, 0.17])
                with c0:
                    # Clave estable por fixture_id (sin idx) para que el estado no se desincronice si cambia el orden
                    incluir = st.checkbox("", value=(fid in league_selected), key=f"liga_sel_{league_code}_{fid}", label_visibility="collapsed")
                    if incluir and fid not in checked_league:
                        checked_league.append(fid)
                c1.write(m.get("home_team") or "—")
                c2.write(m.get("away_team") or "—")
                c3.write(dia)
                c4.write(hora)
            st.session_state["league_selected_by_league"][league_code] = checked_league[:MAX_LEAGUE_SELECT]
            league_selected = checked_league[:MAX_LEAGUE_SELECT]
            n_league_sel = len(league_selected)
            # Contador y botón justo bajo la lista para que reflejen la selección en esta misma ejecución
            sc1, sc2, sc3 = st.columns([0.5, 0.25, 0.25])
            with sc2:
                st.metric("Partidos seleccionados", f"{n_league_sel} de {MAX_LEAGUE_SELECT}", "")
            with sc3:
                league_can_run_here = n_league_sel > 0 and GEMINI_API_KEY and _has_any_credit(st.session_state.current_user)
                if league_can_run_here and st.button("Generar análisis", key="btn_league_analysis_top", type="primary"):
                    st.session_state["run_single_league_analysis"] = True
                    st.rerun()
            st.markdown("---")
            st.subheader("Generar análisis (V2 + Opinión recomendada)")
            balance_liga = int(st.session_state.current_user.get("credits_balance") or 0) if st.session_state.current_user and st.session_state.current_user.get("role") != "admin" else 999
            if n_league_sel > 0 and balance_liga > 0 and n_league_sel > balance_liga and st.session_state.current_user and st.session_state.current_user.get("role") != "admin":
                faltantes_liga = n_league_sel - balance_liga
                st.warning(f"Tienes **{balance_liga} créditos**. Selecciona solo hasta {balance_liga} partidos, o el sistema analizará **{balance_liga} partidos elegidos aleatoriamente** de los {n_league_sel} seleccionados.")
                st.caption("O compra los créditos faltantes:")
                _render_buy_credits_ui(faltantes_liga, "buy_cr_liga")

            def _league_match_data(rows_list, fids):
                out = []
                for fid in fids:
                    r = next((x for x in rows_list if x.get("fixture_id") == fid), None)
                    if r:
                        out.append(dict(
                            home_team=r.get("home"), away_team=r.get("away"), fixture_id=fid,
                            home_team_id=r.get("home_team_id"), away_team_id=r.get("away_team_id"),
                            league_code=r.get("league_code"), league_name=r.get("league"), date=r.get("date"),
                        ))
                return out

            def _league_db_context(match_data_list):
                if not match_data_list:
                    return None
                cache_form = {}
                cache_h2h = {}
                cache_team_id = {}
                parts = []
                for m in match_data_list:
                    fid = m.get("fixture_id")
                    home = m.get("home_team") or "Local"
                    away = m.get("away_team") or "Visitante"
                    home_id = m.get("home_team_id")
                    away_id = m.get("away_team_id")
                    lcode = m.get("league_code") or ""
                    part = [f"{home} vs {away}:"]
                    try:
                        use_fallback = fid is not None and int(fid) >= FALLBACK_FIXTURE_ID_MIN
                    except (TypeError, ValueError):
                        use_fallback = False
                    try:
                        if use_fallback:
                            as_home_id, as_away_id = home_id, away_id
                        else:
                            if home not in cache_team_id:
                                cache_team_id[home] = get_team_id_by_name(home)
                            if away not in cache_team_id:
                                cache_team_id[away] = get_team_id_by_name(away)
                            as_home_id, as_away_id = cache_team_id[home], cache_team_id[away]
                        # Forma: solo tabla historical_matches (master_table_checked → historical_matches)
                        form_h = None
                        if get_recent_form_historical and (lcode or "").strip():
                            try:
                                form_h = get_recent_form_historical(team_id=as_home_id, team_name=home, league_id=lcode, last_n=5, use_master_checked=True)
                            except Exception:
                                pass
                        if form_h:
                            part.append(f"  Forma local: {', '.join([str(f.get('goals_for', '?')) for f in form_h])}")
                        form_a = None
                        if get_recent_form_historical and (lcode or "").strip():
                            try:
                                form_a = get_recent_form_historical(team_id=as_away_id, team_name=away, league_id=lcode, last_n=5, use_master_checked=True)
                            except Exception:
                                pass
                        if form_a:
                            part.append(f"  Forma visitante: {', '.join([str(f.get('goals_for', '?')) for f in form_a])}")
                        h2h = []
                        if get_h2h_historical and (lcode or "").strip():
                            try:
                                h2h = get_h2h_historical(home_id=as_home_id, away_id=as_away_id, home_name=home, away_name=away, league_id=lcode, last_n=5, use_master_checked=True)
                            except Exception:
                                pass
                        if not h2h and as_home_id and as_away_id:
                            h2h_key = (min(as_home_id, as_away_id), max(as_home_id, as_away_id))
                            if h2h_key not in cache_h2h:
                                cache_h2h[h2h_key] = get_h2h_api_sports(as_home_id, as_away_id, limit=5)
                            h2h = cache_h2h[h2h_key]
                        if not h2h and fid and not use_fallback:
                            h2h = get_h2h(fid, limit=5, use_mock=False)
                        if h2h:
                            h2h_str = _format_h2h_string(h2h)
                            part.append(f"  H2H: {h2h_str}")
                    except Exception:
                        log.exception("_league_db_context: error forma/H2H para %s vs %s (fid=%s)", home, away, fid)
                    parts.append("\n".join(part))
                return "\n\n".join(parts) if parts else None

            # la is always initialised before run_single_trigger so it is accessible below
            la = st.session_state.get("league_analysis") or {}
            la_match_ids_set = set(la.get("match_ids") or [])

            league_can_run = n_league_sel > 0 and GEMINI_API_KEY and _has_any_credit(st.session_state.current_user)
            run_single_trigger = st.session_state.pop("run_single_league_analysis", False) or (league_can_run and st.button("Analizar seleccionados", key="btn_league_analysis", type="primary"))
            if run_single_trigger and league_can_run:
                import time
                ids_to_analyze_liga = _effective_ids_to_analyze(league_selected, st.session_state.current_user)
                selected_match_dicts = [m for m in matches if m.get("fixture_id") in ids_to_analyze_liga]
                with st.spinner("Calculando probabilidades para los partidos seleccionados..."):
                    rows = analyze_matches(selected_match_dicts, use_mock=use_mock)
                match_data_league = _league_match_data(rows, ids_to_analyze_liga)
                db_ctx_league = _league_db_context(match_data_league)
                start_league = time.time()
                activity_league = []
                progress_box_league = st.container()
                with progress_box_league:
                    st.markdown("""
                    <div style="background:#2C2C2E;border:1px solid #3d3d4d;border-radius:10px;padding:1.2rem 1.5rem;margin:0.5rem 0;">
                    <p style="margin:0 0 0.5rem 0;font-weight:600;color:#6495ED;font-size:16px;">Análisis por liga</p>
                    <p style="margin:0 0 0.8rem 0;font-size:15px;color:#A0A0A0;">Alfred → Reginald → Consenso → Reginald V2</p>
                    </div>
                    """, unsafe_allow_html=True)
                    prog_league = st.progress(0)
                    step_ph = st.empty()
                    time_ph = st.empty()
                    log_ph = st.empty()

                def _up(step, msg, log_line):
                    elapsed = int(time.time() - start_league)
                    activity_league.append(f"[{elapsed}s] {log_line}")
                    prog_league.progress((step + 1) / 4)
                    step_ph.caption(
                        f"**Paso {step + 1} de 4** — {msg}\n\n_{_random_waiting_message()}_"
                    )
                    time_ph.caption(f"⏱ **{elapsed} s**")
                    with log_ph.container():
                        for line in activity_league:
                            st.caption(line)

                try:
                    _up(0, "Alfred y Reginald analizan en paralelo...", "Ambos.")
                    result_g_league, result_m_league = _run_proposals_parallel(match_data_league, db_ctx_league)
                    v2_grok = result_g_league.get("analysis") or ""
                    v2_stats = result_g_league.get("stats_by_fixture") or {}
                    gemini_full = result_m_league.get("analysis") or ""
                    _up(0, "Ambos listos.", "Ambos listos.")
                    _up(2, "Propuesta General 1+2 (consenso)...", "Consenso.")
                    from consensus import run_consensus_flow
                    consensus_league = run_consensus_flow(v2_grok, gemini_full, match_data_league)
                    _up(2, "Consenso listo.", "Consenso listo.")
                    table_lines_league = []
                    for fid in ids_to_analyze_liga:
                        r = next((x for x in rows if x.get("fixture_id") == fid), {})
                        s = v2_stats.get(fid) or {}
                        p1 = _norm_prob_display(s.get("prob_home_win"))
                        px = _norm_prob_display(s.get("prob_draw"))
                        p2 = _norm_prob_display(s.get("prob_away_win"))
                        xg = s.get("expected_goals")
                        vb = s.get("value_bet") or "—"
                        v1 = f"{p1:.0%}" if p1 is not None else "—"
                        vx = f"{px:.0%}" if px is not None else "—"
                        v2 = f"{p2:.0%}" if p2 is not None else "—"
                        xg_str = f"{xg:.1f}" if xg is not None else "—"
                        table_lines_league.append(
                            f"- {r.get('home') or 'Local'} vs {r.get('away') or 'Visitante'}: V.1={v1} X={vx} V.2={v2} xG={xg_str} Value={vb}"
                        )
                    user_v2 = "**Tabla Alfred:**\n\n" + "\n".join(table_lines_league) + "\n\n---\n\n**Análisis Alfred:**\n\n" + (v2_grok or "")
                    sys_v2 = "Eres Reginald, experto en fútbol y análisis. Analiza la tabla y el análisis de Alfred y da tu opinión en markdown. Responde solo en markdown."
                    _up(3, "Reginald V2 (análisis sobre Alfred)...", "Reginald V2.")
                    v2_reginald = ask_gemini_custom(sys_v2, user_v2)
                    _up(3, "Listo.", "Listo.")
                    fixture_to_match_league = {r.get("fixture_id"): dict(r, home_team=r.get("home"), away_team=r.get("away")) for r in rows if r.get("fixture_id") in ids_to_analyze_liga}
                    st.session_state["league_analysis"] = {
                        "league_code": league_code,
                        "match_ids": list(ids_to_analyze_liga),
                        "v2_grok_analysis": v2_grok,
                        "v2_grok_stats": v2_stats,
                        "v2_gemini_analysis": v2_reginald or "",
                        "consensus": consensus_league,
                        "fixture_to_match": fixture_to_match_league,
                    }
                    prog_league.progress(1.0)
                    step_ph.caption("✅ **Completado**")
                    _deduct_credits_and_refresh(len(ids_to_analyze_liga))
                    st.success("Análisis generado. Revisa las secciones abajo y genera el PDF.")
                    st.rerun()
                except Exception as e_league:
                    log.exception("Análisis liga individual: %s", e_league)
                    activity_league.append(f"[{int(time.time()-start_league)}s] Error: {e_league}")
                    with log_ph.container():
                        for line in activity_league:
                            st.caption(line)
                    st.error(str(e_league))
                    with st.expander("Detalle"):
                        st.code(traceback.format_exc())

                la = st.session_state.get("league_analysis") or {}
                la_match_ids_set = set(la.get("match_ids") or [])
            # la / la_match_ids_set were initialised above and refreshed inside run_single_trigger if needed
            if la.get("league_code") == league_code and la_match_ids_set and la_match_ids_set <= set(league_selected) and n_league_sel > 0:
                st.markdown("---")
                st.subheader("ForgeWin V2")
                st.caption("Tabla Alfred + Análisis Alfred + Análisis Reginald")
                ft_league = la.get("fixture_to_match") or {}
                v2_st = la.get("v2_grok_stats") or {}
                if v2_st and ft_league:
                    st.markdown("**Datos por partido (Alfred)**")
                    for fid in la.get("match_ids", []):
                        m = ft_league.get(fid) or {}
                        s = v2_st.get(fid) or {}
                        p1, px, p2 = _norm_prob_display(s.get("prob_home_win")), _norm_prob_display(s.get("prob_draw")), _norm_prob_display(s.get("prob_away_win"))
                        xg, vb = s.get("expected_goals"), s.get("value_bet") or "—"
                        v1 = f"{p1:.0%}" if p1 is not None else "—"
                        vx = f"{px:.0%}" if px is not None else "—"
                        v2 = f"{p2:.0%}" if p2 is not None else "—"
                        xg_str = f"{xg:.1f}" if xg is not None else "—"
                        st.caption(f"{m.get('home_team') or m.get('home')} vs {m.get('away_team') or m.get('away')}: V.1={v1} X={vx} V.2={v2} xG={xg_str} Value={vb}")
                with st.expander("Análisis Alfred (V2)", expanded=False):
                    st.markdown(la.get("v2_grok_analysis") or "")
                with st.expander("Análisis Reginald (V2)", expanded=True):
                    st.markdown(la.get("v2_gemini_analysis") or "")

                st.subheader("ForgeWin - Opinión recomendada")
                st.caption("Propuesta General 1+2 (recomendación final)")
                with st.expander("Análisis y recomendación final", expanded=True):
                    st.markdown((la.get("consensus") or {}).get("analysis") or "")

                full_id_league = f"{league_code}_{len(la.get('match_ids', []))}"
                if _can_plus(st.session_state.current_user):
                    if st.button("Generar y descargar PDF", key="btn_league_pdf"):
                        with st.spinner("Generando PDF..."):
                            path_league = generate_proposal_pdf_league_combined(
                                full_id_league,
                                la["fixture_to_match"],
                                la["match_ids"],
                                la["v2_grok_stats"],
                                la["v2_grok_analysis"],
                                la["v2_gemini_analysis"],
                                la.get("consensus") or {},
                            )
                            with open(path_league, "rb") as f:
                                pdf_bytes_league = f.read()
                        st.session_state["league_pdf_bytes"] = pdf_bytes_league
                        st.session_state["league_pdf_filename"] = f"forgewin_liga_{full_id_league}.pdf"
                        st.session_state["pdf_just_generated_league"] = True
                        st.rerun()
                    if st.session_state.get("league_pdf_bytes") and st.session_state.get("league_pdf_filename"):
                        st.success("PDF listo. Si no se descargó automáticamente, haz clic en **Descargar nuevamente**.")
                        _trigger_auto_download_once("pdf_just_generated_league")
                        st.download_button("Descargar nuevamente", data=st.session_state["league_pdf_bytes"], file_name=st.session_state["league_pdf_filename"], mime="application/pdf", key="dl_league_pdf")
                else:
                    st.caption("La descarga de PDF está disponible en perfil **Usuario+**.")
            elif n_league_sel > 0 and league_can_run:
                st.info("Selecciona partidos arriba y pulsa **Generar análisis (V2 + Opinión recomendada)**.")

    # ---------- Créditos (antes: Conoce los planes) ----------
    elif mode == "Conoce los planes":
        u_credits = st.session_state.current_user
        is_admin_credits = u_credits.get("role") == "admin"
        balance_credits = int(u_credits.get("credits_balance") or 0) if not is_admin_credits else None

        st.subheader("💳 Mis créditos")

        # Saldo actual
        if is_admin_credits:
            st.info("**Admin:** sin límite de créditos.")
        else:
            col_bal, col_info = st.columns([1, 2])
            with col_bal:
                st.metric("Saldo actual", f"{balance_credits} créditos")
            with col_info:
                st.caption("Cada crédito = 1 análisis completo (Alfred + Reginald + Propuesta Final + PDF).")
                st.caption("**Precio: $2.000 CLP por crédito** · IVA incluido · Mínimo 10 créditos.")

        st.markdown("---")
        st.markdown("### Comprar créditos")

        if not is_admin_credits:
            try:
                from payment_controller import is_configured, create_preference_for_credits, PRICE_PER_CREDIT_CLP, MIN_CREDITS_PURCHASE
                mp_ok = is_configured()
            except ImportError:
                mp_ok = False
                PRICE_PER_CREDIT_CLP = 2000
                MIN_CREDITS_PURCHASE = 10

            packs_display = [
                {"credits": 10,  "price": 20000,  "label": "10 análisis",  "badge": ""},
                {"credits": 25,  "price": 50000,  "label": "25 análisis",  "badge": "⭐ Más popular"},
                {"credits": 50,  "price": 100000, "label": "50 análisis",  "badge": "💎 Mejor valor"},
            ]

            pack_cols = st.columns(len(packs_display))
            user_email_credits = u_credits.get("email") or ""
            if not user_email_credits:
                user_email_credits = st.text_input(
                    "Tu email (para Mercado Pago)",
                    key="credits_email_main",
                    placeholder="correo@ejemplo.com",
                )

            for i, pack in enumerate(packs_display):
                with pack_cols[i]:
                    if pack["badge"]:
                        st.markdown(f"**{pack['badge']}**")
                    st.markdown(f"### {pack['label']}")
                    st.markdown(f"**${pack['price']:,} CLP**".replace(",", "."))
                    st.caption(f"${PRICE_PER_CREDIT_CLP:,} CLP/análisis · IVA incl.".replace(",", "."))
                    if mp_ok and user_email_credits:
                        if st.button(f"Comprar {pack['credits']} créditos", key=f"pack_btn_{pack['credits']}", use_container_width=True, type="primary"):
                            _stok = st.query_params.get(_SESSION_PARAM, "")
                            _base = os.getenv("FORGEWIN_BASE_URL", "https://forgewin.cl")
                            _bk = f"{_base}?payment=pack&status=ok&t={_stok}" if _stok else None
                            init_point, err = create_preference_for_credits(
                                u_credits["id"], pack["credits"], user_email_credits, back_url=_bk
                            )
                            if init_point:
                                st.session_state[f"pack_link_{pack['credits']}"] = init_point
                                st.rerun()
                            else:
                                st.error(err)
                        link_key = f"pack_link_{pack['credits']}"
                        if st.session_state.get(link_key):
                            st.link_button("💳 Pagar con Mercado Pago", st.session_state[link_key], use_container_width=True)
                    elif not mp_ok:
                        st.caption("Pagos no configurados.")
                    else:
                        st.caption("Ingresa tu email arriba para continuar.")

            st.markdown("---")
            st.markdown("#### ¿Cuántos créditos necesitas?")
            custom_n = st.number_input(
                "Cantidad personalizada (mín. 10)",
                min_value=MIN_CREDITS_PURCHASE,
                max_value=500,
                value=MIN_CREDITS_PURCHASE,
                step=5,
                key="credits_custom_n",
            )
            if mp_ok and user_email_credits:
                _render_buy_credits_ui(custom_n, "credits_custom")

        st.markdown("---")
        st.markdown("### ¿Qué incluye cada análisis?")
        for b in PLANS_BENEFITS[0]["benefits"]:
            st.markdown(f"✅ {b}")

    # ---------- Partidos del día (top 10 ligas) ----------
    elif mode == "Partidos del día (top 10 ligas)":
        show_upcoming = st.session_state.get("partidos_dia_show_upcoming", False)

        if show_upcoming:
            st.subheader("Próximos partidos – Ligas principales")
            if st.button("← Volver a solo partidos de hoy"):
                st.session_state["partidos_dia_show_upcoming"] = False
                st.rerun()
            upcoming = []
            display_rows = []
            with st.spinner("Cargando próximos partidos de todas las ligas..."):
                upcoming = get_upcoming_matches(TOP_10_LEAGUE_CODES, days_ahead=7, use_mock=use_mock)
                if upcoming:
                    upcoming = _normalize_and_dedupe_matches(upcoming)
                if not upcoming:
                    st.info("No hay próximos partidos disponibles.")
                    st.session_state["partidos_dia_show_upcoming"] = False
                else:
                    rows_with_probs = analyze_matches(
                        [{"fixture_id": m.get("fixture_id"), "home_team": m.get("home_team"), "away_team": m.get("away_team"), "home_team_id": m.get("home_team_id"), "away_team_id": m.get("away_team_id"), "league_id": m.get("league_id"), "league_name": m.get("league_name"), "date": m.get("date") or m.get("datetime")} for m in upcoming],
                        use_mock=use_mock,
                    )
                    fixture_to_row = {r.get("fixture_id"): r for r in rows_with_probs}
                    for m in upcoming:
                        r = fixture_to_row.get(m.get("fixture_id")) or {}
                        display_rows.append({
                            "fixture_id": m.get("fixture_id"),
                            "home": r.get("home") or r.get("home_team") or m.get("home_team"),
                            "away": r.get("away") or r.get("away_team") or m.get("away_team"),
                            "date": r.get("date") or m.get("date") or m.get("datetime"),
                            "league": r.get("league") or m.get("league_name"),
                            "league_code": r.get("league_code") or m.get("league_id"),
                            "prob_home_win": r.get("prob_home_win"), "prob_draw": r.get("prob_draw"), "prob_away_win": r.get("prob_away_win"),
                            "expected_goals": r.get("expected_goals"), "clean_sheet_home": r.get("clean_sheet_home"), "clean_sheet_away": r.get("clean_sheet_away"),
                            "prob_btts": r.get("prob_btts"), "prob_over25": r.get("prob_over25"),
                            "value_home": r.get("value_home"), "value_draw": r.get("value_draw"), "value_away": r.get("value_away"),
                            "implied_home": r.get("implied_home"), "implied_draw": r.get("implied_draw"), "implied_away": r.get("implied_away"),
                            "external_data": r.get("external_data"),
                        })
            if upcoming and display_rows:
                # Render same table as "else" block below (by_league, checkboxes, etc.)
                by_league = defaultdict(list)
                for row in display_rows:
                    by_league[row.get("league") or "Otros"].append(row)
                for _k in by_league:
                    by_league[_k].sort(key=lambda r: (str(r.get("date") or ""), r.get("fixture_id") or 0))
                ligas_orden = sorted(by_league.keys())
                MAX_SELECT = 10
                st.caption(f"Próximos 7 días. Selecciona hasta {MAX_SELECT} partidos para generar una propuesta.")
                checked_fids = []
                COL_DEFS = [
                    ("Sel.", "Marca para incluir en la propuesta (máx. 10)."),
                    ("Local", "Equipo local."),
                    ("Visitante", "Equipo visitante."),
                    ("Día", "Día del partido."),
                    ("Hora", "Hora del partido."),
                    ("Vic. local", "Prob. victoria local."),
                    ("Empate", "Prob. empate."),
                    ("Vic. visit.", "Prob. victoria visitante."),
                    ("Goles esp.", "Goles esperados."),
                    ("CS local", "Clean sheet local."),
                    ("CS visit.", "Clean sheet visitante."),
                    ("BTTS", "Ambos marcan."),
                    ("Over 2.5", "Más de 2.5 goles."),
                    ("Value", "Value bet (1/X/2)."),
                ]
                for liga_nombre in ligas_orden:
                    filas = by_league[liga_nombre]
                    primera = filas[0]
                    lcode = primera.get("league_code") or get_league_code_from_name(liga_nombre)
                    logo_col, name_col = st.columns([0.08, 0.92])
                    with logo_col:
                        if lcode:
                            _show_league_logo(lcode, 40)
                    with name_col:
                        st.markdown(f"**{liga_nombre}**")
                    header_cols = st.columns([0.04, 0.09, 0.09, 0.06, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.04, 0.04])
                    for idx, (nombre_col, _) in enumerate(COL_DEFS):
                        with header_cols[idx]:
                            st.caption(nombre_col)
                    for idx_row, row in enumerate(filas):
                        fid = row.get("fixture_id")
                        c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13 = st.columns([0.04, 0.09, 0.09, 0.06, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.04, 0.04])
                        with c0:
                            st.checkbox("", value=(fid in st.session_state.get("selected_fixture_ids", [])), key=f"sel_up_{fid}_{liga_nombre}", disabled=True)
                        c1.write(row.get("home", ""))
                        c2.write(row.get("away", ""))
                        dia_mes_ano, hora_min = format_date_chile(row.get("date"))
                        c3.write(dia_mes_ano if dia_mes_ano != "—" else "—")
                        c4.write(hora_min if dia_mes_ano != "—" else "—")
                        p1, px, p2 = _norm_prob_display(row.get("prob_home_win")), _norm_prob_display(row.get("prob_draw")), _norm_prob_display(row.get("prob_away_win"))
                        c5.write(f"{p1:.0%}" if p1 is not None else "—")
                        c6.write(f"{px:.0%}" if px is not None else "—")
                        c7.write(f"{p2:.0%}" if p2 is not None else "—")
                        c8.write(f"{row.get('expected_goals'):.1f}" if row.get("expected_goals") is not None else "—")
                        cs_h, cs_a = _norm_prob_display(row.get("clean_sheet_home")), _norm_prob_display(row.get("clean_sheet_away"))
                        btts, over25 = _norm_prob_display(row.get("prob_btts")), _norm_prob_display(row.get("prob_over25"))
                        c9.write(f"{cs_h:.0%}" if cs_h is not None else "—")
                        c10.write(f"{cs_a:.0%}" if cs_a is not None else "—")
                        c11.write(f"{btts:.0%}" if btts is not None else "—")
                        c12.write(f"{over25:.0%}" if over25 is not None else "—")
                        val_parts = []
                        if row.get("value_home"):
                            val_parts.append("1")
                        if row.get("value_draw"):
                            val_parts.append("X")
                        if row.get("value_away"):
                            val_parts.append("2")
                        c13.write(",".join(val_parts) if val_parts else "—")
                    st.markdown("---")
        else:
            from datetime import date, timedelta, timezone
            def _match_on_local_date(m, tgt):
                dt = m.get("datetime")
                if not dt:
                    return False
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.astimezone(CHILE_TZ).date() == tgt

            day_options = ["Hoy", "Mañana"]
            if "partidos_dia_day" not in st.session_state:
                st.session_state.partidos_dia_day = "Hoy"
            col_sel, col_btn = st.columns([0.35, 0.65])
            with col_sel:
                partidos_dia_day = st.radio("Ver partidos", day_options, index=day_options.index(st.session_state.partidos_dia_day) if st.session_state.partidos_dia_day in day_options else 0, key="partidos_dia_day_radio", horizontal=True)
                st.session_state.partidos_dia_day = partidos_dia_day
            is_tomorrow = partidos_dia_day == "Mañana"
            target_date = (date.today() + timedelta(days=1)) if is_tomorrow else date.today()
            target_date_iso = target_date.isoformat()
            day_label = "mañana" if is_tomorrow else "hoy"

            st.subheader(f"Partidos de {day_label} – Ligas principales")
            if not is_tomorrow and st.button("Actualizar resultados (partidos ya jugados)"):
                with st.spinner("Actualizando resultados desde API y evaluando propuestas..."):
                    fetch_finished_results(TOP_10_LEAGUE_CODES, days_back=2)
                    n = evaluate_proposals()
                st.success(f"Resultados actualizados. Propuestas evaluadas: {n}.")

            todays_db = get_matches_by_local_date(TOP_10_LEAGUE_CODES, target_date)
            if not todays_db:
                with st.spinner("Sincronizando partidos desde API..."):
                    fetch_and_store_matches(TOP_10_LEAGUE_CODES, days_ahead=2)
                todays_db = get_matches_by_local_date(TOP_10_LEAGUE_CODES, target_date)
            if not todays_db and not use_mock:
                if st.button(f"Forzar carga desde API ({day_label})"):
                    fetch_and_store_matches(TOP_10_LEAGUE_CODES, days_ahead=2)
                    st.rerun()
            if not todays_db:
                upcoming = get_upcoming_matches(TOP_10_LEAGUE_CODES, days_ahead=2, use_mock=use_mock)
                tomorrow_matches = [m for m in (upcoming or []) if _match_on_local_date(m, target_date)]
                for m in tomorrow_matches:
                    from db import upsert_match
                    mm = dict(m)
                    mm["match_date_utc"] = mm.get("date") or (mm.get("datetime").isoformat() if mm.get("datetime") else None)
                    mm["league_code"] = mm.get("league_id")
                    upsert_match(mm)
                todays_db = get_matches_by_local_date(TOP_10_LEAGUE_CODES, target_date) if not use_mock else []
            if not todays_db and use_mock:
                upcoming = get_upcoming_matches(TOP_10_LEAGUE_CODES, days_ahead=2, use_mock=True)
                todays = [m for m in (upcoming or []) if _match_on_local_date(m, target_date)]
                rows_with_probs = analyze_matches(
                    [{"fixture_id": m.get("fixture_id"), "home_team": m.get("home_team"), "away_team": m.get("away_team"), "home_team_id": m.get("home_team_id"), "away_team_id": m.get("away_team_id"), "league_id": m.get("league_id"), "league_name": m.get("league_name"), "date": m.get("date") or m.get("datetime")} for m in todays],
                    use_mock=use_mock,
                ) if todays else []
                fixture_to_row_mock = {r.get("fixture_id"): r for r in rows_with_probs}
                todays_db = []
                for m in todays:
                    r = fixture_to_row_mock.get(m.get("fixture_id")) or {}
                    todays_db.append({
                        "fixture_id": m.get("fixture_id"),
                        "home_team": r.get("home") or m.get("home_team"),
                        "away_team": r.get("away") or m.get("away_team"),
                        "date": r.get("date") or m.get("date"),
                        "league_name": r.get("league") or m.get("league_name"),
                        "league_code": r.get("league_code") or m.get("league_id"),
                        "prob_home_win": r.get("prob_home_win"),
                        "prob_draw": r.get("prob_draw"),
                        "prob_away_win": r.get("prob_away_win"),
                        "expected_goals": r.get("expected_goals"),
                        "clean_sheet_home": r.get("clean_sheet_home"),
                        "clean_sheet_away": r.get("clean_sheet_away"),
                        "prob_btts": r.get("prob_btts"),
                        "prob_over25": r.get("prob_over25"),
                    })

            todays = _normalize_and_dedupe_matches(todays_db)
            if not todays:
                st.info(f"**No hay partidos {day_label}.**")
                if st.button("¿Quieres ver los próximos partidos disponibles?"):
                    st.session_state["partidos_dia_show_upcoming"] = True
                    st.rerun()
            else:
                # Calcular/rellenar probs si faltan (desde analyzer)
                rows_with_probs = analyze_matches(
                    [{"fixture_id": m.get("fixture_id"), "home_team": m.get("home_team"), "away_team": m.get("away_team"), "home_team_id": m.get("home_team_id"), "away_team_id": m.get("away_team_id"), "league_id": m.get("league_code"), "league_name": m.get("league_name"), "date": m.get("date") or m.get("match_date_utc")} for m in todays],
                    use_mock=use_mock
                )
                fixture_to_row = {r.get("fixture_id"): r for r in rows_with_probs}
                display_rows = []
                for m in todays:
                    r = fixture_to_row.get(m.get("fixture_id")) or m
                    display_rows.append({
                        "fixture_id": m.get("fixture_id"),
                        "home": r.get("home") or r.get("home_team") or m.get("home_team"),
                        "away": r.get("away") or r.get("away_team") or m.get("away_team"),
                        "date": r.get("date") or m.get("date") or m.get("match_date_utc"),
                        "league": r.get("league") or m.get("league_name"),
                        "league_code": r.get("league_code") or m.get("league_id"),
                        "prob_home_win": r.get("prob_home_win"),
                        "prob_draw": r.get("prob_draw"),
                        "prob_away_win": r.get("prob_away_win"),
                        "expected_goals": r.get("expected_goals"),
                        "clean_sheet_home": r.get("clean_sheet_home"),
                        "clean_sheet_away": r.get("clean_sheet_away"),
                        "prob_btts": r.get("prob_btts"),
                        "prob_over25": r.get("prob_over25"),
                        "value_home": r.get("value_home"),
                        "value_draw": r.get("value_draw"),
                        "value_away": r.get("value_away"),
                        "implied_home": r.get("implied_home"),
                        "implied_draw": r.get("implied_draw"),
                        "implied_away": r.get("implied_away"),
                        "external_data": r.get("external_data"),
                    })
                fixture_to_match = {m.get("fixture_id"): m for m in todays}
                for r in display_rows:
                    from db import upsert_match
                    upsert_match({
                        "fixture_id": r.get("fixture_id"),
                        "home_team": r.get("home"), "away_team": r.get("away"),
                        "league_code": r.get("league_code"), "league_name": r.get("league"),
                        "match_date_utc": r.get("date"), "prob_home_win": r.get("prob_home_win"),
                        "prob_draw": r.get("prob_draw"), "prob_away_win": r.get("prob_away_win"),
                        "expected_goals": r.get("expected_goals"), "clean_sheet_home": r.get("clean_sheet_home"),
                        "clean_sheet_away": r.get("clean_sheet_away"), "prob_btts": r.get("prob_btts"),
                        "prob_over25": r.get("prob_over25"),
                    })

                MAX_SELECT = 10
                st.caption("Opción 1 (izq.): Alfred. Opción 2 (der.): Reginald. Propuesta General 1+2: consenso entre ambos.")

                # Si hay última propuesta con stats, usarlas para rellenar la tabla
                last_pid = st.session_state.get("last_proposal_id_grok") or st.session_state.get("last_proposal_id_gemini")
                last_prop = get_proposal(last_pid) if last_pid else None
                grok_stats = (last_prop.get("grok_stats") or {}) if last_prop else {}

                by_league = defaultdict(list)
                for row in display_rows:
                    by_league[row.get("league") or "Otros"].append(row)
                for _k in by_league:
                    by_league[_k].sort(key=lambda r: (str(r.get("date") or ""), r.get("fixture_id") or 0))
                ligas_orden = sorted(by_league.keys())
                checked_fids = []
                all_fids_display = [r.get("fixture_id") for r in display_rows if r.get("fixture_id")]
                if st.session_state.pop("show_max_10_select_all", None):
                    st.warning("Se pueden analizar como máximo 10 partidos. Se han seleccionado los primeros 10.")
                if all_fids_display and st.button("Seleccionar todo", key="btn_select_all_partidos"):
                    st.session_state.selected_fixture_ids = all_fids_display[:MAX_SELECT]
                    if len(all_fids_display) > MAX_SELECT:
                        st.session_state["show_max_10_select_all"] = True
                    st.rerun()
                COL_DEFS = [
                    ("Sel.", "Marca para incluir (máx. 10)."),
                    ("Local", "Equipo local."),
                    ("Visitante", "Visitante."),
                    ("Día", "Día del partido."),
                    ("Hora", "Hora del partido."),
                    ("Vic. local", "Prob. victoria local."),
                    ("Empate", "Prob. empate."),
                    ("Vic. visit.", "Prob. victoria visitante."),
                    ("Goles esp.", "Goles esperados."),
                    ("CS local", "CS local."),
                    ("CS visit.", "CS visit."),
                    ("BTTS", "Ambos marcan."),
                    ("Over 2.5", "Over 2.5."),
                    ("Value", "Value bet."),
                ]
                with st.expander("Partidos – selecciona hasta 10 (clic para ver/ocultar)", expanded=False):
                    for liga_nombre in ligas_orden:
                        filas = by_league[liga_nombre]
                        primera = filas[0]
                        lcode = primera.get("league_code") or get_league_code_from_name(liga_nombre)
                        logo_col, name_col = st.columns([0.06, 0.94])
                        with logo_col:
                            if lcode:
                                _show_league_logo(lcode, 28)
                        with name_col:
                            st.markdown(f"**{liga_nombre}**")
                        header_cols = st.columns([0.03, 0.08, 0.08, 0.06, 0.05, 0.045, 0.045, 0.045, 0.045, 0.045, 0.045, 0.045, 0.04, 0.035])
                        for idx, (nombre_col, descripcion) in enumerate(COL_DEFS):
                            with header_cols[idx]:
                                st.caption(nombre_col, help=descripcion)
                        for idx_row, row in enumerate(filas):
                            fid = row.get("fixture_id")
                            row_show = dict(row)
                            if fid in grok_stats:
                                row_show.update(grok_stats[fid])
                            c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13 = st.columns([0.03, 0.08, 0.08, 0.06, 0.05, 0.045, 0.045, 0.045, 0.045, 0.045, 0.045, 0.045, 0.04, 0.035])
                            with c0:
                                cb = st.checkbox("", value=(fid in st.session_state.selected_fixture_ids), key=f"sel_{fid}_{liga_nombre}", help="Incluir")
                                if cb and fid not in checked_fids:
                                    checked_fids.append(fid)
                            c1.write(row_show.get("home", ""))
                            c2.write(row_show.get("away", ""))
                            dia_mes_ano, hora_min = format_date_chile(row_show.get("date"))
                            c3.write(dia_mes_ano)
                            c4.write(hora_min)
                            p1, px, p2 = _norm_prob_display(row_show.get("prob_home_win")), _norm_prob_display(row_show.get("prob_draw")), _norm_prob_display(row_show.get("prob_away_win"))
                            xg, cs_h, cs_a = row_show.get("expected_goals"), _norm_prob_display(row_show.get("clean_sheet_home")), _norm_prob_display(row_show.get("clean_sheet_away"))
                            btts, over25 = _norm_prob_display(row_show.get("prob_btts")), _norm_prob_display(row_show.get("prob_over25"))
                            c5.write(f"{p1:.0%}" if p1 is not None else "—")
                            c6.write(f"{px:.0%}" if px is not None else "—")
                            c7.write(f"{p2:.0%}" if p2 is not None else "—")
                            c8.write(f"{xg:.1f}" if xg is not None else "—")
                            c9.write(f"{cs_h:.0%}" if cs_h is not None else "—")
                            c10.write(f"{cs_a:.0%}" if cs_a is not None else "—")
                            c11.write(f"{btts:.0%}" if btts is not None else "—")
                            c12.write(f"{over25:.0%}" if over25 is not None else "—")
                            val_parts = []
                            vb = row_show.get("value_bet")
                            if vb:
                                val_parts = [s.strip() for s in str(vb).replace(",", " ").split() if s.strip() in ("1", "X", "2")]
                            if not val_parts and (row_show.get("value_home") or row_show.get("value_draw") or row_show.get("value_away")):
                                if row_show.get("value_home"):
                                    val_parts.append("1")
                                if row_show.get("value_draw"):
                                    val_parts.append("X")
                                if row_show.get("value_away"):
                                    val_parts.append("2")
                            c13.write(",".join(val_parts) if val_parts else "—")
                        st.markdown("---")

                st.session_state.selected_fixture_ids = checked_fids[:MAX_SELECT]  # máx 10
                n_sel = len(st.session_state.selected_fixture_ids)

                def _get_match_data(fids=None):
                    ids = fids if fids is not None else st.session_state.selected_fixture_ids
                    raw = [fixture_to_row.get(fid) or next((r for r in display_rows if r.get("fixture_id") == fid), {}) for fid in ids]
                    return [dict(m, home_team=m.get("home") or m.get("home_team"), away_team=m.get("away") or m.get("away_team"), league_name=m.get("league"), league=m.get("league"), date=m.get("date"), fixture_id=m.get("fixture_id")) for m in raw]

                def _build_db_context(match_data_list):
                    """Construye contexto (forma reciente, H2H) para Alfred y Reginald desde nuestra tabla maestra.
                    Orden: master_table_checked → historical_matches; solo si no hay datos, fallback a API-Sports.
                    Cache por team_id y par (id1,id2) para no superar límite de API-Sports."""
                    if not match_data_list:
                        return None
                    cache_form = {}
                    cache_h2h = {}
                    cache_team_id = {}
                    parts = []
                    # Contexto para los agentes: siempre usar tabla historical/maestra (master_table_checked → historical_matches)
                    use_master = True
                    for m in match_data_list:
                        fid = m.get("fixture_id")
                        home = m.get("home_team") or m.get("home") or "Local"
                        away = m.get("away_team") or m.get("away") or "Visitante"
                        home_id = m.get("home_team_id")
                        away_id = m.get("away_team_id")
                        lcode = m.get("league_code") or m.get("league_id")
                        part = [f"{home} vs {away}:"]
                        try:
                            use_fallback = fid is not None and int(fid) >= FALLBACK_FIXTURE_ID_MIN
                        except (TypeError, ValueError):
                            use_fallback = False
                        try:
                            if use_fallback:
                                as_home_id, as_away_id = home_id, away_id
                            else:
                                if home not in cache_team_id:
                                    cache_team_id[home] = get_team_id_by_name(home)
                                if away not in cache_team_id:
                                    cache_team_id[away] = get_team_id_by_name(away)
                                as_home_id, as_away_id = cache_team_id[home], cache_team_id[away]
                            # Forma: solo tabla historical (master_table_checked → historical_matches); no usar API para últimos 5
                            form_h = None
                            if get_recent_form_historical and (lcode or "").strip():
                                try:
                                    form_h = get_recent_form_historical(team_id=as_home_id, team_name=home, league_id=lcode or "", last_n=5, use_master_checked=use_master)
                                except Exception:
                                    pass
                            if form_h:
                                goles = [str(f.get("goals_for", "?")) for f in form_h]
                                part.append(f"  Forma local (goles a favor últimos 5): {', '.join(goles)}")
                            form_a = None
                            if get_recent_form_historical and (lcode or "").strip():
                                try:
                                    form_a = get_recent_form_historical(team_id=as_away_id, team_name=away, league_id=lcode or "", last_n=5, use_master_checked=use_master)
                                except Exception:
                                    pass
                            if form_a:
                                goles = [str(f.get("goals_for", "?")) for f in form_a]
                                part.append(f"  Forma visitante (goles a favor últimos 5): {', '.join(goles)}")
                            h2h = []
                            if get_h2h_historical and (lcode or "").strip():
                                try:
                                    h2h = get_h2h_historical(home_id=as_home_id, away_id=as_away_id, home_name=home, away_name=away, league_id=lcode or "", last_n=5, use_master_checked=use_master)
                                except Exception:
                                    pass
                            if not h2h and as_home_id and as_away_id:
                                h2h_key = (min(as_home_id, as_away_id), max(as_home_id, as_away_id))
                                if h2h_key not in cache_h2h:
                                    cache_h2h[h2h_key] = get_h2h_api_sports(as_home_id, as_away_id, limit=5)
                                h2h = cache_h2h[h2h_key]
                            if not h2h and fid and not use_fallback:
                                h2h = get_h2h(fid, limit=5, use_mock=False)
                            if h2h:
                                h2h_str = _format_h2h_string(h2h)
                                part.append(f"  H2H (resultados): {h2h_str}")
                        except Exception:
                            log.exception("_build_db_context: error forma/H2H para %s vs %s (fid=%s)", home, away, fid)
                        parts.append("\n".join(part))
                    return "\n\n".join(parts) if parts else None

                # Un solo botón: Dale con todo! (Paso 1 Alfred, Paso 2 Reginald, Paso 3 Propuesta General 1+2)
                import time
                _COST_DALE = 2  # créditos por partido
                balance = int(st.session_state.current_user.get("credits_balance") or 0) if st.session_state.current_user and st.session_state.current_user.get("role") != "admin" else 999
                can_run_all = (
                    n_sel > 0
                    and GEMINI_API_KEY
                    and _has_any_credit(st.session_state.current_user)
                )
                _costo_total = n_sel * _COST_DALE
                _partidos_posibles = balance // _COST_DALE if _COST_DALE else balance
                if n_sel > 0 and balance > 0 and _costo_total > balance and st.session_state.current_user and st.session_state.current_user.get("role") != "admin":
                    faltantes = _costo_total - balance
                    st.warning(f"Tienes **{balance} créditos** ({_partidos_posibles} partidos a {_COST_DALE} cr/partido). Selecciona hasta {_partidos_posibles} partidos o compra más créditos.")
                    st.caption("O compra los créditos faltantes para analizar todos:")
                    _render_buy_credits_ui(faltantes, "buy_cr_dale_falt")
                if st.session_state.current_user and st.session_state.current_user.get("role") == "admin" and n_sel > 0:
                    st.caption("🧪 Modo admin: pruebas sin descontar créditos.")
                if can_run_all and st.button("🔥 Dale con todo!", type="primary", key="dale_con_todo"):
                    ids_to_analyze = _effective_ids_to_analyze(st.session_state.selected_fixture_ids, st.session_state.current_user)
                    match_data = _get_match_data(ids_to_analyze)
                    start_time = time.time()
                    total_steps = 3
                    activity_log = []

                    progress_box = st.container()
                    with progress_box:
                        st.markdown("""
                        <div style="
                            background: #2C2C2E;
                            border: 1px solid #3d3d4d;
                            border-radius: 10px;
                            padding: 1.2rem 1.5rem;
                            margin: 0.5rem 0;
                        ">
                        <p style="margin:0 0 0.5rem 0; font-weight: 600; color: #6495ED; font-size: 16px;">🔥 Dale con todo!</p>
                        <p style="margin:0 0 0.8rem 0; font-size: 15px; color: #A0A0A0;">Ejecutando análisis Alfred → Reginald → Propuesta General 1+2</p>
                        </div>
                        """, unsafe_allow_html=True)
                        progress_bar = st.progress(0)
                        step_placeholder = st.empty()
                        time_placeholder = st.empty()
                        log_placeholder = st.empty()

                    def _update_ui(step: int, message: str, log_line: str):
                        elapsed = int(time.time() - start_time)
                        activity_log.append(f"[{elapsed}s] {log_line}")
                        progress_bar.progress((step + 1) / total_steps)
                        step_placeholder.caption(
                            f"**Paso {step + 1} de {total_steps}** — {message}\n\n_{_random_waiting_message()}_"
                        )
                        time_placeholder.caption(f"⏱ Tiempo transcurrido: **{elapsed} s**")
                        with log_placeholder.container():
                            for line in activity_log:
                                st.caption(line)

                    try:
                        _update_ui(0, "Alfred y Reginald analizan en paralelo...", "Iniciando ambos.")
                        db_context = _build_db_context(match_data)
                        result_g, result_m = _run_proposals_parallel(match_data, db_context)
                        analysis_g = result_g.get("analysis", "")
                        stats_g = result_g.get("stats_by_fixture") or {}
                        analysis_m = result_m.get("analysis", "")
                        stats_m = result_m.get("stats_by_fixture") or {}
                        pid_g = create_proposal(st.session_state.current_user["id"], match_data, analysis_g, grok_stats=stats_g)
                        pdf_path_g = generate_proposal_pdf(pid_g, match_data, analysis_g)
                        set_proposal_pdf(pid_g, pdf_path_g)
                        st.session_state.last_proposal_id_grok = pid_g
                        num_g = (get_proposal(pid_g) or {}).get("proposal_number") or pid_g[:8]
                        pid_m = create_proposal(st.session_state.current_user["id"], match_data, analysis_m, grok_stats=stats_m)
                        pdf_path_m = generate_proposal_pdf(pid_m, match_data, analysis_m)
                        set_proposal_pdf(pid_m, pdf_path_m)
                        st.session_state.last_proposal_id_gemini = pid_m
                        num_m = (get_proposal(pid_m) or {}).get("proposal_number") or pid_m[:8]
                        _update_ui(0, "Alfred listo.", f"Alfred: Propuesta #{num_g}")
                        _update_ui(1, "Reginald listo.", f"Reginald: Propuesta #{num_m}")

                        # Flujo Propuesta General 1+2 (consenso)
                        time.sleep(1)
                        _update_ui(2, "Flujo Dale con todo finalizado. Iniciando Generar Propuesta General 1+2...", "Iniciando flujo Propuesta General 1+2.")
                        from consensus import run_consensus_flow
                        consensus_result = run_consensus_flow(analysis_g, analysis_m, match_data)
                        st.session_state["consensus_result"] = consensus_result
                        st.session_state["consensus_match_ids"] = list(ids_to_analyze)
                        _update_ui(2, "Propuesta General 1+2 listo.", "Propuesta General 1+2: recomendación final generada.")

                        elapsed_final = int(time.time() - start_time)
                        progress_bar.progress(1.0)
                        step_placeholder.caption(f"✅ **Completado** — Opción 1, 2 y 3 generadas.")
                        time_placeholder.caption(f"⏱ Tiempo total: **{elapsed_final} s**")
                        with log_placeholder.container():
                            for line in activity_log:
                                st.caption(line)
                            st.caption(f"[{elapsed_final}s] Listo.")
                        _deduct_credits_and_refresh(len(ids_to_analyze) * _COST_DALE)
                        st.success("Dale con todo completado. Propuesta General 1+2 lista.")
                        st.rerun()
                    except Exception as e:
                        import traceback
                        log.exception("Dale con todo: %s", e)
                        activity_log.append(f"[{int(time.time()-start_time)}s] Error: {e}")
                        with log_placeholder.container():
                            for line in activity_log:
                                st.caption(line)
                        st.error(f"Error: {e}")
                        with st.expander("Detalle del error"):
                            st.code(traceback.format_exc())

                elif n_sel > 0 and not can_run_all:
                    if not _has_any_credit(st.session_state.current_user):
                        st.info("No tienes créditos. Compra un pack para ejecutar análisis o los créditos que necesitas para este análisis:")
                        _render_buy_credits_ui(n_sel, "buy_cr_dale")
                    elif not GEMINI_API_KEY:
                        st.info("Configura la API key de Reginald (Gemini) en .env para usar «Dale con todo!».")

                def _render_mini_table(selected_fids, stats_by_fixture, fixture_to_match, col_ctx, title):
                    """Muestra recuadro de métricas por partido."""
                    if not selected_fids or not stats_by_fixture:
                        col_ctx.caption("Sin datos. Genera la propuesta para ver la tabla.")
                        return
                    col_ctx.markdown(f"**{title}**")
                    labels = ["Local", "Visit.", "Día", "Hora", "V.1", "X", "V.2", "xG", "CS L", "CS V", "BTTS", "O2.5", "Value"]
                    h = col_ctx.columns([0.12, 0.10, 0.08, 0.06, 0.05, 0.04, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.06])
                    for i, lb in enumerate(labels):
                        h[i].caption(lb)
                    for fid in selected_fids:
                        m = fixture_to_match.get(fid) or {}
                        s = stats_by_fixture.get(fid) or {}
                        dia, hora = format_date_chile(m.get("date") or "")
                        p1, px, p2 = _norm_prob_display(s.get("prob_home_win")), _norm_prob_display(s.get("prob_draw")), _norm_prob_display(s.get("prob_away_win"))
                        xg = s.get("expected_goals")
                        cs_h, cs_a = _norm_prob_display(s.get("clean_sheet_home")), _norm_prob_display(s.get("clean_sheet_away"))
                        btts, over25 = _norm_prob_display(s.get("prob_btts")), _norm_prob_display(s.get("prob_over25"))
                        vb = s.get("value_bet") or "—"
                        row_cols = col_ctx.columns([0.12, 0.10, 0.08, 0.06, 0.05, 0.04, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.06])
                        row_cols[0].write(m.get("home_team") or m.get("home") or "—")
                        row_cols[1].write(m.get("away_team") or m.get("away") or "—")
                        row_cols[2].write(dia)
                        row_cols[3].write(hora)
                        row_cols[4].write(f"{p1:.0%}" if p1 is not None else "—")
                        row_cols[5].write(f"{px:.0%}" if px is not None else "—")
                        row_cols[6].write(f"{p2:.0%}" if p2 is not None else "—")
                        row_cols[7].write(f"{xg:.1f}" if xg is not None else "—")
                        row_cols[8].write(f"{cs_h:.0%}" if cs_h is not None else "—")
                        row_cols[9].write(f"{cs_a:.0%}" if cs_a is not None else "—")
                        row_cols[10].write(f"{btts:.0%}" if btts is not None else "—")
                        row_cols[11].write(f"{over25:.0%}" if over25 is not None else "—")
                        row_cols[12].write(str(vb))

                st.markdown("---")
                st.subheader("Propuesta General 1+2")
                st.caption("Se genera automáticamente con «Dale con todo». Flujo: Reginald opina sobre Alfred → Alfred opina sobre Reginald → Alfred consultor senior → Reginald define la recomendación final.")

                pid_g = st.session_state.get("last_proposal_id_grok")
                pid_m = st.session_state.get("last_proposal_id_gemini")
                prop_g = get_proposal(pid_g) if pid_g else None
                prop_m = get_proposal(pid_m) if pid_m else None

                consensus = st.session_state.get("consensus_result")
                consensus_fids = st.session_state.get("consensus_match_ids") or []
                if consensus and consensus.get("analysis") is not None:
                    if consensus_fids:
                        _render_mini_table(consensus_fids, consensus.get("stats_by_fixture") or {}, fixture_to_match, st, "Datos por partido (Propuesta General 1+2)")
                    with st.expander("Análisis Propuesta General 1+2", expanded=True):
                        st.markdown(consensus.get("analysis") or "")
                    # Descargas solo para Usuario+ o Admin
                    if _can_plus(st.session_state.current_user):
                        st.markdown("**Descargas**")
                        from pdf_report import generate_proposal_pdf_final_recommendation
                        opinion_full_id = f"{pid_g or 'g'}_{pid_m or 'm'}" if (pid_g or pid_m) else "opinion_final"
                        if st.button("Opinión recomendada", key="gen_opinion_recomendada"):
                            full_path = generate_proposal_pdf_final_recommendation(
                                opinion_full_id,
                                fixture_to_match,
                                list(consensus_fids),
                                consensus,
                            )
                            st.session_state["opinion_recomendada_pdf_path"] = full_path
                            st.session_state["opinion_recomendada_pdf_id"] = opinion_full_id
                            st.session_state["pdf_just_generated_opinion"] = True
                            st.rerun()
                        if st.session_state.get("opinion_recomendada_pdf_path") and os.path.exists(st.session_state["opinion_recomendada_pdf_path"]):
                            st.success("PDF listo. Si no se descargó automáticamente, haz clic en **Descargar nuevamente**.")
                            _trigger_auto_download_once("pdf_just_generated_opinion")
                            with open(st.session_state["opinion_recomendada_pdf_path"], "rb") as f:
                                st.download_button("Descargar nuevamente", f, file_name=f"opinion_recomendada_{st.session_state.get('opinion_recomendada_pdf_id', 'final')}.pdf", mime="application/pdf", key="dl_opinion_recomendada")
                    else:
                        st.caption("La descarga de PDF está disponible en perfil **Usuario+**.")
                elif pid_g and pid_m and n_sel > 0:
                    st.info("Ejecuta «Dale con todo» para generar la Propuesta General 1+2 automáticamente.")

                # Propuesta unificada (Opción 1 + 2 + 3): descargar PDF solo para Usuario+ o Admin.
                # La propuesta general y el PDF siempre son por los partidos efectivamente analizados (consensus_match_ids).
                if pid_g and pid_m and n_sel > 0 and prop_g and prop_m:
                    st.markdown("**Propuesta generada (Opción 1 + 2 + Propuesta General 1+2)**")
                    if _can_plus(st.session_state.current_user):
                        from pdf_report import generate_proposal_pdf_three_options
                        full_id = f"{pid_g}_{pid_m}"
                        match_ids_for_pdf = st.session_state.get("consensus_match_ids") or st.session_state.selected_fixture_ids
                        consensus_for_pdf = st.session_state.get("consensus_result")
                        if st.button("Generar y descargar propuesta completa (Opción 1 + 2 + Propuesta General 1+2)", key="dl_full_proposal"):
                            full_path = generate_proposal_pdf_three_options(full_id, fixture_to_match, list(match_ids_for_pdf), prop_g, prop_m, consensus=consensus_for_pdf)
                            st.session_state["full_proposal_pdf_path"] = full_path
                            st.session_state["full_proposal_pdf_id"] = full_id
                            st.session_state["pdf_just_generated_full"] = True
                            st.rerun()
                        if st.session_state.get("full_proposal_pdf_path") and os.path.exists(st.session_state["full_proposal_pdf_path"]):
                            st.success("PDF listo. Si no se descargó automáticamente, haz clic en **Descargar nuevamente**.")
                            _trigger_auto_download_once("pdf_just_generated_full")
                            with open(st.session_state["full_proposal_pdf_path"], "rb") as f:
                                st.download_button("Descargar nuevamente", f, file_name=f"propuesta_1_2_3_{st.session_state.get('full_proposal_pdf_id', 'full')}.pdf", mime="application/pdf", key="dl_full_pdf")
                    else:
                        st.caption("La descarga de PDF está disponible en perfil **Usuario+**.")

    # ---------- V3 (copia Partidos hoy/mañana; botón "Dale con todo V2"; análisis = propuesta Gemini) ----------
    elif mode == "V3":
        from datetime import date as _date, timedelta as _timedelta, timezone as _timezone
        def _match_on_local_date_v3(m, tgt):
            dt = m.get("datetime")
            if not dt:
                return False
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=_timezone.utc)
            return dt.astimezone(CHILE_TZ).date() == tgt
        st.subheader("🔬 Análisis Pro – Partidos de hoy y mañana")
        st.caption("Análisis enriquecido con **Índice de Asedio** (tiros a puerta, córners), **Alerta de Fricción** (árbitro + faltas) y **Dinámica HT/FT** (resultado al descanso). Alfred y Reginald analizan en paralelo y se genera una Propuesta Final de consenso.")
        day_options_v3 = ["Hoy", "Mañana"]
        if "v3_partidos_dia_day" not in st.session_state:
            st.session_state["v3_partidos_dia_day"] = "Hoy"
        col_sel_v3, col_btn_v3 = st.columns([0.35, 0.65])
        with col_sel_v3:
            partidos_dia_day_v3 = st.radio("Ver partidos", day_options_v3, index=day_options_v3.index(st.session_state["v3_partidos_dia_day"]) if st.session_state["v3_partidos_dia_day"] in day_options_v3 else 0, key="v3_partidos_dia_day_radio", horizontal=True)
            st.session_state["v3_partidos_dia_day"] = partidos_dia_day_v3
        is_tomorrow_v3 = partidos_dia_day_v3 == "Mañana"
        target_date_v3 = (_date.today() + _timedelta(days=1)) if is_tomorrow_v3 else _date.today()
        day_label_v3 = "mañana" if is_tomorrow_v3 else "hoy"
        st.subheader(f"Partidos de {day_label_v3} – Ligas principales (V3)")
        todays_db_v3 = get_matches_by_local_date(TOP_10_LEAGUE_CODES, target_date_v3)
        if not todays_db_v3:
            with st.spinner("Sincronizando partidos desde API..."):
                fetch_and_store_matches(TOP_10_LEAGUE_CODES, days_ahead=2)
            todays_db_v3 = get_matches_by_local_date(TOP_10_LEAGUE_CODES, target_date_v3)
        if not todays_db_v3 and not use_mock:
            if st.button(f"Forzar carga desde API ({day_label_v3})", key="v3_force_fetch"):
                fetch_and_store_matches(TOP_10_LEAGUE_CODES, days_ahead=2)
                st.rerun()
        if not todays_db_v3:
            upcoming_v3 = get_upcoming_matches(TOP_10_LEAGUE_CODES, days_ahead=2, use_mock=use_mock)
            tomorrow_matches_v3 = [m for m in (upcoming_v3 or []) if _match_on_local_date_v3(m, target_date_v3)]
            for m in tomorrow_matches_v3:
                from db import upsert_match
                mm = dict(m)
                mm["match_date_utc"] = mm.get("date") or (mm.get("datetime").isoformat() if mm.get("datetime") else None)
                mm["league_code"] = mm.get("league_id")
                upsert_match(mm)
            todays_db_v3 = get_matches_by_local_date(TOP_10_LEAGUE_CODES, target_date_v3) if not use_mock else []
        if not todays_db_v3 and use_mock:
            upcoming_v3 = get_upcoming_matches(TOP_10_LEAGUE_CODES, days_ahead=2, use_mock=True)
            todays_mock_v3 = [m for m in (upcoming_v3 or []) if _match_on_local_date_v3(m, target_date_v3)]
            rows_with_probs_v3 = analyze_matches(
                [{"fixture_id": m.get("fixture_id"), "home_team": m.get("home_team"), "away_team": m.get("away_team"), "home_team_id": m.get("home_team_id"), "away_team_id": m.get("away_team_id"), "league_id": m.get("league_id"), "league_name": m.get("league_name"), "date": m.get("date") or m.get("datetime")} for m in todays_mock_v3],
                use_mock=use_mock,
            ) if todays_mock_v3 else []
            fixture_to_row_mock_v3 = {r.get("fixture_id"): r for r in rows_with_probs_v3}
            todays_db_v3 = []
            for m in todays_mock_v3:
                r = fixture_to_row_mock_v3.get(m.get("fixture_id")) or {}
                todays_db_v3.append({
                    "fixture_id": m.get("fixture_id"),
                    "home_team": r.get("home") or m.get("home_team"),
                    "away_team": r.get("away") or m.get("away_team"),
                    "date": r.get("date") or m.get("date"),
                    "league_name": r.get("league") or m.get("league_name"),
                    "league_code": r.get("league_code") or m.get("league_id"),
                    "prob_home_win": r.get("prob_home_win"),
                    "prob_draw": r.get("prob_draw"),
                    "prob_away_win": r.get("prob_away_win"),
                    "expected_goals": r.get("expected_goals"),
                    "clean_sheet_home": r.get("clean_sheet_home"),
                    "clean_sheet_away": r.get("clean_sheet_away"),
                    "prob_btts": r.get("prob_btts"),
                    "prob_over25": r.get("prob_over25"),
                })
        todays_v3 = _normalize_and_dedupe_matches(todays_db_v3)
        if not todays_v3:
            st.info(f"**No hay partidos {day_label_v3}.**")
        else:
            rows_with_probs_v3 = analyze_matches(
                [{"fixture_id": m.get("fixture_id"), "home_team": m.get("home_team"), "away_team": m.get("away_team"), "home_team_id": m.get("home_team_id"), "away_team_id": m.get("away_team_id"), "league_id": m.get("league_code"), "league_name": m.get("league_name"), "date": m.get("date") or m.get("match_date_utc")} for m in todays_v3],
                use_mock=use_mock,
            )
            fixture_to_row_v3 = {r.get("fixture_id"): r for r in rows_with_probs_v3}
            display_rows_v3 = []
            for m in todays_v3:
                r = fixture_to_row_v3.get(m.get("fixture_id")) or m
                display_rows_v3.append({
                    "fixture_id": m.get("fixture_id"),
                    "home": r.get("home") or r.get("home_team") or m.get("home_team"),
                    "away": r.get("away") or r.get("away_team") or m.get("away_team"),
                    "date": r.get("date") or m.get("date") or m.get("match_date_utc"),
                    "league": r.get("league") or m.get("league_name"),
                    "league_code": r.get("league_code") or m.get("league_id"),
                    "home_team_id": r.get("home_team_id") or m.get("home_team_id"),
                    "away_team_id": r.get("away_team_id") or m.get("away_team_id"),
                    "prob_home_win": r.get("prob_home_win"),
                    "prob_draw": r.get("prob_draw"),
                    "prob_away_win": r.get("prob_away_win"),
                    "expected_goals": r.get("expected_goals"),
                    "clean_sheet_home": r.get("clean_sheet_home"),
                    "clean_sheet_away": r.get("clean_sheet_away"),
                    "prob_btts": r.get("prob_btts"),
                    "prob_over25": r.get("prob_over25"),
                    "value_home": r.get("value_home"),
                    "value_draw": r.get("value_draw"),
                    "value_away": r.get("value_away"),
                    "implied_home": r.get("implied_home"),
                    "implied_draw": r.get("implied_draw"),
                    "implied_away": r.get("implied_away"),
                    "external_data": r.get("external_data"),
                })
            fixture_to_match_v3 = {m.get("fixture_id"): m for m in todays_v3}
            by_league_v3 = defaultdict(list)
            for row in display_rows_v3:
                by_league_v3[row.get("league") or "Otros"].append(row)
            for _k in by_league_v3:
                by_league_v3[_k].sort(key=lambda r: (str(r.get("date") or ""), r.get("fixture_id") or 0))
            ligas_orden_v3 = sorted(by_league_v3.keys())
            MAX_SELECT_V3 = 10
            if "v3_selected_fixture_ids" not in st.session_state:
                st.session_state["v3_selected_fixture_ids"] = []
            checked_fids_v3 = []
            COL_DEFS_V3 = [
                ("Sel.", "Marca para incluir (máx. 10)."),
                ("Local", "Equipo local."),
                ("Visitante", "Visitante."),
                ("Día", "Día del partido."),
                ("Hora", "Hora del partido."),
                ("Vic. local", "Prob. victoria local."),
                ("Empate", "Prob. empate."),
                ("Vic. visit.", "Prob. victoria visitante."),
                ("Goles esp.", "Goles esperados."),
                ("CS local", "CS local."),
                ("CS visit.", "CS visit."),
                ("BTTS", "Ambos marcan."),
                ("Over 2.5", "Over 2.5."),
                ("Value", "Value bet."),
            ]
            with st.expander("Partidos de hoy/mañana V3 – selecciona hasta 10", expanded=True):
                for liga_nombre_v3 in ligas_orden_v3:
                    filas_v3 = by_league_v3[liga_nombre_v3]
                    primera_v3 = filas_v3[0]
                    lcode_v3 = primera_v3.get("league_code") or get_league_code_from_name(liga_nombre_v3)
                    logo_col_v3, name_col_v3 = st.columns([0.06, 0.94])
                    with logo_col_v3:
                        if lcode_v3:
                            _show_league_logo(lcode_v3, 28)
                    with name_col_v3:
                        st.markdown(f"**{liga_nombre_v3}**")
                    header_cols_v3 = st.columns([0.03, 0.08, 0.08, 0.06, 0.05, 0.045, 0.045, 0.045, 0.045, 0.045, 0.045, 0.045, 0.04, 0.035])
                    for idx, (nombre_col, descripcion) in enumerate(COL_DEFS_V3):
                        with header_cols_v3[idx]:
                            st.caption(nombre_col, help=descripcion)
                    for idx_row, row in enumerate(filas_v3):
                        fid = row.get("fixture_id")
                        row_show_v3 = dict(row)
                        c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13 = st.columns([0.03, 0.08, 0.08, 0.06, 0.05, 0.045, 0.045, 0.045, 0.045, 0.045, 0.045, 0.045, 0.04, 0.035])
                        with c0:
                            cb = st.checkbox("", value=(fid in st.session_state["v3_selected_fixture_ids"]), key=f"v3_sel_{fid}_{liga_nombre_v3}", help="Incluir")
                            if cb and fid not in checked_fids_v3:
                                checked_fids_v3.append(fid)
                        c1.write(row_show_v3.get("home", ""))
                        c2.write(row_show_v3.get("away", ""))
                        dia_mes_ano, hora_min = format_date_chile(row_show_v3.get("date"))
                        c3.write(dia_mes_ano)
                        c4.write(hora_min)
                        p1, px, p2 = _norm_prob_display(row_show_v3.get("prob_home_win")), _norm_prob_display(row_show_v3.get("prob_draw")), _norm_prob_display(row_show_v3.get("prob_away_win"))
                        xg, cs_h, cs_a = row_show_v3.get("expected_goals"), _norm_prob_display(row_show_v3.get("clean_sheet_home")), _norm_prob_display(row_show_v3.get("clean_sheet_away"))
                        btts, over25 = _norm_prob_display(row_show_v3.get("prob_btts")), _norm_prob_display(row_show_v3.get("prob_over25"))
                        c5.write(f"{p1:.0%}" if p1 is not None else "—")
                        c6.write(f"{px:.0%}" if px is not None else "—")
                        c7.write(f"{p2:.0%}" if p2 is not None else "—")
                        c8.write(f"{xg:.1f}" if xg is not None else "—")
                        c9.write(f"{cs_h:.0%}" if cs_h is not None else "—")
                        c10.write(f"{cs_a:.0%}" if cs_a is not None else "—")
                        c11.write(f"{btts:.0%}" if btts is not None else "—")
                        c12.write(f"{over25:.0%}" if over25 is not None else "—")
                        val_parts = []
                        vb = row_show_v3.get("value_bet")
                        if vb:
                            val_parts = [s.strip() for s in str(vb).replace(",", " ").split() if s.strip() in ("1", "X", "2")]
                        if not val_parts and (row_show_v3.get("value_home") or row_show_v3.get("value_draw") or row_show_v3.get("value_away")):
                            if row_show_v3.get("value_home"):
                                val_parts.append("1")
                            if row_show_v3.get("value_draw"):
                                val_parts.append("X")
                            if row_show_v3.get("value_away"):
                                val_parts.append("2")
                        c13.write(",".join(val_parts) if val_parts else "—")
                    st.markdown("---")
            st.session_state["v3_selected_fixture_ids"] = checked_fids_v3[:MAX_SELECT_V3]
            n_sel_v3 = len(st.session_state["v3_selected_fixture_ids"])
            st.caption("Opción 1: Alfred. Opción 2: Reginald. Propuesta General 1+2: consenso. V3 usa solo el análisis enriquecido (asedio, fricción, HT/FT).")

            def _get_match_data_v3(fids=None):
                ids = fids if fids is not None else st.session_state["v3_selected_fixture_ids"]
                raw = [fixture_to_row_v3.get(fid) or next((r for r in display_rows_v3 if r.get("fixture_id") == fid), {}) for fid in ids]
                return [dict(m, home_team=m.get("home") or m.get("home_team"), away_team=m.get("away") or m.get("away_team"), league_name=m.get("league"), league=m.get("league"), date=m.get("date"), fixture_id=m.get("fixture_id")) for m in raw]

            def _build_db_context_v3(match_data_list):
                if not match_data_list:
                    return None
                cache_form = {}
                cache_h2h = {}
                cache_team_id = {}
                parts = []
                for m in match_data_list:
                    fid = m.get("fixture_id")
                    home = m.get("home_team") or m.get("home") or "Local"
                    away = m.get("away_team") or m.get("away") or "Visitante"
                    home_id = m.get("home_team_id")
                    away_id = m.get("away_team_id")
                    lcode = m.get("league_code") or m.get("league_id")
                    part = [f"{home} vs {away}:"]
                    try:
                        use_fallback = fid is not None and int(fid) >= FALLBACK_FIXTURE_ID_MIN
                    except (TypeError, ValueError):
                        use_fallback = False
                    try:
                        if use_fallback:
                            as_home_id, as_away_id = home_id, away_id
                        else:
                            if home not in cache_team_id:
                                cache_team_id[home] = get_team_id_by_name(home)
                            if away not in cache_team_id:
                                cache_team_id[away] = get_team_id_by_name(away)
                            as_home_id, as_away_id = cache_team_id[home], cache_team_id[away]
                        form_h = None
                        if get_recent_form_historical and (lcode or "").strip():
                            try:
                                form_h = get_recent_form_historical(team_id=as_home_id, team_name=home, league_id=lcode or "", last_n=5, use_master_checked=True)
                            except Exception:
                                pass
                        if form_h:
                            goles = [str(f.get("goals_for", "?")) for f in form_h]
                            part.append(f"  Forma local (goles a favor últimos 5): {', '.join(goles)}")
                        form_a = None
                        if get_recent_form_historical and (lcode or "").strip():
                            try:
                                form_a = get_recent_form_historical(team_id=as_away_id, team_name=away, league_id=lcode or "", last_n=5, use_master_checked=True)
                            except Exception:
                                pass
                        if form_a:
                            goles = [str(f.get("goals_for", "?")) for f in form_a]
                            part.append(f"  Forma visitante (goles a favor últimos 5): {', '.join(goles)}")
                        h2h = []
                        if get_h2h_historical and (lcode or "").strip():
                            try:
                                h2h = get_h2h_historical(home_id=as_home_id, away_id=as_away_id, home_name=home, away_name=away, league_id=lcode or "", last_n=5, use_master_checked=True)
                            except Exception:
                                pass
                        if not h2h and as_home_id and as_away_id:
                            h2h_key = (min(as_home_id, as_away_id), max(as_home_id, as_away_id))
                            if h2h_key not in cache_h2h:
                                cache_h2h[h2h_key] = get_h2h_api_sports(as_home_id, as_away_id, limit=5)
                            h2h = cache_h2h[h2h_key]
                        if not h2h and fid and not use_fallback:
                            h2h = get_h2h(fid, limit=5, use_mock=False)
                        if h2h:
                            h2h_str = _format_h2h_string(h2h)
                            part.append(f"  H2H (resultados): {h2h_str}")
                    except Exception:
                        log.exception("_build_db_context_v3: error forma/H2H para %s vs %s (fid=%s)", home, away, fid)
                    parts.append("\n".join(part))
                return "\n\n".join(parts) if parts else None

            import time as _time
            _COST_PRO = 3  # créditos por partido (Dale con Todo Pro)
            balance_v3 = int(st.session_state.current_user.get("credits_balance") or 0) if st.session_state.current_user and st.session_state.current_user.get("role") != "admin" else 999
            can_run_v3 = n_sel_v3 > 0 and GEMINI_API_KEY and _has_any_credit(st.session_state.current_user)
            _costo_total_v3 = n_sel_v3 * _COST_PRO
            _partidos_posibles_v3 = balance_v3 // _COST_PRO if _COST_PRO else balance_v3
            if n_sel_v3 > 0 and balance_v3 > 0 and _costo_total_v3 > balance_v3 and st.session_state.current_user and st.session_state.current_user.get("role") != "admin":
                st.warning(f"Tienes **{balance_v3} créditos** ({_partidos_posibles_v3} partidos a {_COST_PRO} cr/partido). Selecciona hasta {_partidos_posibles_v3} partidos o compra más créditos.")
                _render_buy_credits_ui(_costo_total_v3 - balance_v3, "buy_cr_v3")
            if st.session_state.current_user and st.session_state.current_user.get("role") == "admin" and n_sel_v3 > 0:
                st.caption("🧪 Modo admin: pruebas sin descontar créditos.")
            if can_run_v3 and st.button("🔥 Dale con Todo Pro", type="primary", key="dale_con_todo_v3"):
                ids_to_analyze_v3 = _effective_ids_to_analyze(st.session_state["v3_selected_fixture_ids"], st.session_state.current_user)
                match_data_v3 = _get_match_data_v3(ids_to_analyze_v3)
                start_time_v3 = _time.time()
                activity_log_v3 = []
                total_steps_v3 = 3
                progress_bar_v3 = st.progress(0)
                step_placeholder_v3 = st.empty()
                time_placeholder_v3 = st.empty()
                log_placeholder_v3 = st.empty()
                def _update_ui_v3(step, message, log_line):
                    elapsed = int(_time.time() - start_time_v3)
                    activity_log_v3.append(f"[{elapsed}s] {log_line}")
                    progress_bar_v3.progress((step + 1) / total_steps_v3)
                    step_placeholder_v3.caption(f"**Paso {step + 1} de {total_steps_v3}** — {message}")
                    time_placeholder_v3.caption(f"⏱ Tiempo: **{elapsed} s**")
                    with log_placeholder_v3.container():
                        for line in activity_log_v3:
                            st.caption(line)
                try:
                    _update_ui_v3(0, "Alfred y Reginald analizan en paralelo (V3 enriquecido)...", "Iniciando.")
                    db_context_v3 = _build_db_context_v3(match_data_v3)
                    enriched_context_v3 = build_enriched_context_for_matches(match_data_v3) if build_enriched_context_for_matches else ""
                    result_g_v3, result_m_v3 = _run_proposals_parallel_v3(match_data_v3, db_context_v3, enriched_context_v3)
                    analysis_g_v3 = result_g_v3.get("analysis", "")
                    stats_g_v3 = result_g_v3.get("stats_by_fixture") or {}
                    analysis_m_v3 = result_m_v3.get("analysis", "")
                    stats_m_v3 = result_m_v3.get("stats_by_fixture") or {}
                    pid_g_v3 = create_proposal(st.session_state.current_user["id"], match_data_v3, analysis_g_v3, grok_stats=stats_g_v3)
                    pdf_path_g_v3 = generate_proposal_pdf(pid_g_v3, match_data_v3, analysis_g_v3)
                    set_proposal_pdf(pid_g_v3, pdf_path_g_v3)
                    st.session_state["v3_last_proposal_id_grok"] = pid_g_v3
                    pid_m_v3 = create_proposal(st.session_state.current_user["id"], match_data_v3, analysis_m_v3, grok_stats=stats_m_v3)
                    pdf_path_m_v3 = generate_proposal_pdf(pid_m_v3, match_data_v3, analysis_m_v3)
                    set_proposal_pdf(pid_m_v3, pdf_path_m_v3)
                    st.session_state["v3_last_proposal_id_gemini"] = pid_m_v3
                    _update_ui_v3(0, "Alfred listo.", "Alfred listo.")
                    _update_ui_v3(1, "Reginald listo.", "Reginald listo.")
                    _time.sleep(1)
                    _update_ui_v3(2, "Generando Propuesta General 1+2 (V3)...", "Consenso.")
                    from consensus import run_consensus_flow
                    consensus_result_v3 = run_consensus_flow(analysis_g_v3, analysis_m_v3, match_data_v3)
                    st.session_state["v3_consensus_result"] = consensus_result_v3
                    st.session_state["v3_consensus_match_ids"] = list(ids_to_analyze_v3)
                    _update_ui_v3(2, "Propuesta General 1+2 listo.", "Listo.")
                    elapsed_final_v3 = int(_time.time() - start_time_v3)
                    progress_bar_v3.progress(1.0)
                    step_placeholder_v3.caption("✅ **Completado** — V3.")
                    time_placeholder_v3.caption(f"⏱ Tiempo total: **{elapsed_final_v3} s**")
                    with log_placeholder_v3.container():
                        for line in activity_log_v3:
                            st.caption(line)
                    _deduct_credits_and_refresh(len(ids_to_analyze_v3) * _COST_PRO)
                    st.success("Dale con Todo Pro completado. Propuesta General 1+2 lista.")
                    st.rerun()
                except Exception as e_v3:
                    import traceback
                    log.exception("Dale con todo V3: %s", e_v3)
                    activity_log_v3.append(f"[{int(_time.time()-start_time_v3)}s] Error: {e_v3}")
                    with log_placeholder_v3.container():
                        for line in activity_log_v3:
                            st.caption(line)
                    st.error(f"Error: {e_v3}")
                    with st.expander("Detalle del error"):
                        st.code(traceback.format_exc())
            elif n_sel_v3 > 0 and not can_run_v3:
                if not _has_any_credit(st.session_state.current_user):
                    st.info("No tienes créditos. Compra un pack para ejecutar análisis.")
                    _render_buy_credits_ui(n_sel_v3, "buy_cr_v3")
                elif not GEMINI_API_KEY:
                    st.info("Configura GEMINI_API_KEY en .env para usar el análisis Pro (V3).")

            def _render_mini_table_v3(selected_fids, stats_by_fixture, fixture_to_match, col_ctx, title):
                if not selected_fids or not stats_by_fixture:
                    col_ctx.caption("Sin datos. Genera la propuesta para ver la tabla.")
                    return
                col_ctx.markdown(f"**{title}**")
                labels = ["Local", "Visit.", "Día", "Hora", "V.1", "X", "V.2", "xG", "CS L", "CS V", "BTTS", "O2.5", "Value"]
                h = col_ctx.columns([0.12, 0.10, 0.08, 0.06, 0.05, 0.04, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.06])
                for i, lb in enumerate(labels):
                    h[i].caption(lb)
                for fid in selected_fids:
                    m = fixture_to_match.get(fid) or {}
                    s = stats_by_fixture.get(fid) or {}
                    dia, hora = format_date_chile(m.get("date") or "")
                    p1, px, p2 = _norm_prob_display(s.get("prob_home_win")), _norm_prob_display(s.get("prob_draw")), _norm_prob_display(s.get("prob_away_win"))
                    xg = s.get("expected_goals")
                    cs_h, cs_a = _norm_prob_display(s.get("clean_sheet_home")), _norm_prob_display(s.get("clean_sheet_away"))
                    btts, over25 = _norm_prob_display(s.get("prob_btts")), _norm_prob_display(s.get("prob_over25"))
                    vb = s.get("value_bet") or "—"
                    row_cols = col_ctx.columns([0.12, 0.10, 0.08, 0.06, 0.05, 0.04, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.06])
                    row_cols[0].write(m.get("home_team") or m.get("home") or "—")
                    row_cols[1].write(m.get("away_team") or m.get("away") or "—")
                    row_cols[2].write(dia)
                    row_cols[3].write(hora)
                    row_cols[4].write(f"{p1:.0%}" if p1 is not None else "—")
                    row_cols[5].write(f"{px:.0%}" if px is not None else "—")
                    row_cols[6].write(f"{p2:.0%}" if p2 is not None else "—")
                    row_cols[7].write(f"{xg:.1f}" if xg is not None else "—")
                    row_cols[8].write(f"{cs_h:.0%}" if cs_h is not None else "—")
                    row_cols[9].write(f"{cs_a:.0%}" if cs_a is not None else "—")
                    row_cols[10].write(f"{btts:.0%}" if btts is not None else "—")
                    row_cols[11].write(f"{over25:.0%}" if over25 is not None else "—")
                    row_cols[12].write(str(vb))

            st.markdown("---")
            st.subheader("Propuesta General 1+2 (Pro)")
            consensus_v3 = st.session_state.get("v3_consensus_result")
            consensus_fids_v3 = st.session_state.get("v3_consensus_match_ids") or []
            if consensus_v3 and consensus_v3.get("analysis") is not None:
                if consensus_fids_v3:
                    _render_mini_table_v3(consensus_fids_v3, consensus_v3.get("stats_by_fixture") or {}, fixture_to_match_v3, st, "Datos por partido (Propuesta General 1+2)")
                with st.expander("Análisis Propuesta General 1+2 (V3)", expanded=True):
                    st.markdown(consensus_v3.get("analysis") or "")
                if _can_plus(st.session_state.current_user):
                    from pdf_report import generate_proposal_pdf_final_recommendation
                    opinion_id_v3 = f"v3_{st.session_state.get('v3_last_proposal_id_grok', 'g')}_{st.session_state.get('v3_last_proposal_id_gemini', 'm')}"
                    if st.button("Opinión recomendada (V3)", key="gen_opinion_v3"):
                        full_path_v3 = generate_proposal_pdf_final_recommendation(opinion_id_v3, fixture_to_match_v3, list(consensus_fids_v3), consensus_v3)
                        st.session_state["v3_opinion_pdf_path"] = full_path_v3
                        st.session_state["pdf_just_generated_opinion_v3"] = True
                        st.rerun()
                    if st.session_state.get("v3_opinion_pdf_path") and os.path.exists(st.session_state.get("v3_opinion_pdf_path", "")):
                        st.success("PDF listo.")
                        with open(st.session_state["v3_opinion_pdf_path"], "rb") as f:
                            st.download_button("Descargar PDF opinión recomendada (V3)", f, file_name=f"opinion_recomendada_v3_{opinion_id_v3}.pdf", mime="application/pdf", key="dl_opinion_v3")
            elif n_sel_v3 > 0 and (st.session_state.get("v3_last_proposal_id_grok") or st.session_state.get("v3_last_proposal_id_gemini")):
                st.info("Ejecuta «Dale con todo V2» para generar la Propuesta General 1+2 en V3.")

    # ---------- V2 ----------
    elif mode == "V2":
        if not _can_v2(st.session_state.current_user):
            st.warning("El modo V2 está disponible solo en perfil **Usuario++**.")
            st.stop()
        st.subheader("V2")
        st.markdown("""
        <div style="
            background: linear-gradient(135deg, #1a237e 0%, #283593 50%, #3949ab 100%);
            border: 1px solid #5c6bc0;
            border-radius: 12px;
            padding: 1.25rem 1.5rem;
            margin-bottom: 1.5rem;
            color: #e8eaf6;
        ">
        <p style="margin: 0 0 0.6rem 0; font-size: 1.1rem; font-weight: 600; color: #c5cae9;">¿Qué es V2?</p>
        <p style="margin: 0 0 0.75rem 0; font-size: 0.95rem; line-height: 1.5;">
        V2 es nuestro <strong>análisis en dos pasos</strong>: primero <strong>Alfred</strong> genera una tabla de estadísticas por partido y su análisis; 
        después <strong>Reginald</strong> lee ese resultado y da su opinión encima — qué partidos le parecen más interesantes, dónde coincide o no con Alfred, y su recomendación.
        </p>
        <p style="margin: 0 0 0.5rem 0; font-size: 0.95rem; line-height: 1.5;">
        Es una opción <strong>premium</strong> porque combina dos modelos de IA en secuencia para que tengas un segundo criterio experto sobre el mismo análisis, 
        con más contexto y coherencia que dos opiniones paralelas.
        </p>
        <p style="margin: 0; font-size: 0.9rem; color: #9fa8da;">
        Selecciona hasta 10 partidos (hoy o mañana), pulsa <strong>Generar V2</strong> y en unos segundos tendrás la tabla Alfred + análisis Alfred + análisis Reginald.
        </p>
        </div>
        """, unsafe_allow_html=True)
        st.caption("Tabla de estadísticas (Alfred), análisis de partidos (Alfred), análisis de Reginald sobre ambos. El PDF descargable contiene todo lo anterior.")
        # Cargar partidos de hoy y mañana (mismo modelo que Partidos del día, fecha local Chile)
        today_date = date.today()
        tomorrow_date = today_date + timedelta(days=1)
        todays_v2 = get_matches_by_local_date(TOP_10_LEAGUE_CODES, today_date) + get_matches_by_local_date(TOP_10_LEAGUE_CODES, tomorrow_date)
        if not todays_v2:
            with st.spinner("Sincronizando partidos desde API..."):
                fetch_and_store_matches(TOP_10_LEAGUE_CODES, days_ahead=2)
            todays_v2 = get_matches_by_local_date(TOP_10_LEAGUE_CODES, today_date) + get_matches_by_local_date(TOP_10_LEAGUE_CODES, tomorrow_date)
        # Fallback con datos mock cuando no hay partidos en BD (ej. sin API key o admin en entorno sin datos)
        if not todays_v2 and use_mock:
            from datetime import timezone
            def _match_on_local_date_v2(m, tgt):
                dt = m.get("datetime")
                if not dt:
                    return False
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.astimezone(CHILE_TZ).date() == tgt
            upcoming_v2 = get_upcoming_matches(TOP_10_LEAGUE_CODES, days_ahead=2, use_mock=True)
            todays_v2 = [m for m in (upcoming_v2 or []) if _match_on_local_date_v2(m, today_date)] + [m for m in (upcoming_v2 or []) if _match_on_local_date_v2(m, tomorrow_date)]
        todays_v2 = _normalize_and_dedupe_matches(todays_v2)
        display_rows_v2_full = []
        fixture_to_match_v2_full = {}
        if todays_v2:
            rows_with_probs_v2 = analyze_matches(
                [{"fixture_id": m.get("fixture_id"), "home_team": m.get("home_team"), "away_team": m.get("away_team"), "home_team_id": m.get("home_team_id"), "away_team_id": m.get("away_team_id"), "league_id": m.get("league_code"), "league_name": m.get("league_name"), "date": m.get("date") or m.get("match_date_utc")} for m in todays_v2],
                use_mock=use_mock,
            )
            fixture_to_row_v2 = {r.get("fixture_id"): r for r in rows_with_probs_v2}
            for m in todays_v2:
                r = fixture_to_row_v2.get(m.get("fixture_id")) or m
                display_rows_v2_full.append({
                    "fixture_id": m.get("fixture_id"),
                    "home": r.get("home") or r.get("home_team") or m.get("home_team"),
                    "away": r.get("away") or r.get("away_team") or m.get("away_team"),
                    "date": r.get("date") or m.get("date") or m.get("match_date_utc"),
                    "league": r.get("league") or r.get("league_name"),
                    "league_code": r.get("league_code") or m.get("league_id"),
                    "prob_home_win": r.get("prob_home_win"),
                    "prob_draw": r.get("prob_draw"),
                    "prob_away_win": r.get("prob_away_win"),
                    "expected_goals": r.get("expected_goals"),
                    "clean_sheet_home": r.get("clean_sheet_home"),
                    "clean_sheet_away": r.get("clean_sheet_away"),
                    "prob_btts": r.get("prob_btts"),
                    "prob_over25": r.get("prob_over25"),
                    "value_home": r.get("value_home"),
                    "value_draw": r.get("value_draw"),
                    "value_away": r.get("value_away"),
                    "implied_home": r.get("implied_home"),
                    "implied_draw": r.get("implied_draw"),
                    "implied_away": r.get("implied_away"),
                    "external_data": r.get("external_data"),
                })
            fixture_to_match_v2_full = {m.get("fixture_id"): m for m in todays_v2}
            by_league_v2 = defaultdict(list)
            for row in display_rows_v2_full:
                by_league_v2[row.get("league") or "Otros"].append(row)
            for _k in by_league_v2:
                by_league_v2[_k].sort(key=lambda r: (str(r.get("date") or ""), r.get("fixture_id") or 0))
            ligas_orden_v2 = sorted(by_league_v2.keys())
            MAX_SELECT_V2 = 10
            checked_fids_v2 = []
            COL_DEFS_V2 = [
                ("Sel.", "Marca para incluir (máx. 10)."),
                ("Local", "Equipo local."),
                ("Visitante", "Visitante."),
                ("Día", "Día del partido."),
                ("Hora", "Hora del partido."),
                ("Vic. local", "Prob. victoria local."),
                ("Empate", "Prob. empate."),
                ("Vic. visit.", "Prob. victoria visitante."),
                ("Goles esp.", "Goles esperados."),
                ("CS local", "CS local."),
                ("CS visit.", "CS visit."),
                ("BTTS", "Ambos marcan."),
                ("Over 2.5", "Over 2.5."),
                ("Value", "Value bet."),
            ]
            last_pid_v2 = st.session_state.get("last_proposal_id_grok") or st.session_state.get("last_proposal_id_gemini")
            last_prop_v2 = get_proposal(last_pid_v2) if last_pid_v2 else None
            grok_stats_v2 = (last_prop_v2.get("grok_stats") or {}) if last_prop_v2 else {}
            all_fids_v2_display = [r.get("fixture_id") for r in display_rows_v2_full if r.get("fixture_id")]
            if st.session_state.pop("show_max_10_v2_select_all", None):
                st.warning("Se pueden analizar como máximo 10 partidos. Se han seleccionado los primeros 10.")
            if all_fids_v2_display and st.button("Seleccionar todo", key="btn_select_all_v2"):
                st.session_state.selected_fixture_ids = all_fids_v2_display[:MAX_SELECT_V2]
                if len(all_fids_v2_display) > MAX_SELECT_V2:
                    st.session_state["show_max_10_v2_select_all"] = True
                st.rerun()
            with st.expander("Partidos de hoy y mañana – selecciona hasta 10 (clic para ver/ocultar)", expanded=True):
                for liga_nombre in ligas_orden_v2:
                    filas = by_league_v2[liga_nombre]
                    primera = filas[0]
                    lcode = primera.get("league_code") or get_league_code_from_name(liga_nombre)
                    logo_col, name_col = st.columns([0.06, 0.94])
                    with logo_col:
                        if lcode:
                            _show_league_logo(lcode, 28)
                    with name_col:
                        st.markdown(f"**{liga_nombre}**")
                    header_cols = st.columns([0.03, 0.08, 0.08, 0.06, 0.05, 0.045, 0.045, 0.045, 0.045, 0.045, 0.045, 0.045, 0.04, 0.035])
                    for idx, (nombre_col, descripcion) in enumerate(COL_DEFS_V2):
                        with header_cols[idx]:
                            st.caption(nombre_col, help=descripcion)
                    for idx_row, row in enumerate(filas):
                        fid = row.get("fixture_id")
                        row_show = dict(row)
                        if fid in grok_stats_v2:
                            row_show.update(grok_stats_v2[fid])
                        c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13 = st.columns([0.03, 0.08, 0.08, 0.06, 0.05, 0.045, 0.045, 0.045, 0.045, 0.045, 0.045, 0.045, 0.04, 0.035])
                        with c0:
                            cb = st.checkbox("", value=(fid in st.session_state.selected_fixture_ids), key=f"v2_sel_{fid}_{liga_nombre}", help="Incluir")
                            if cb and fid not in checked_fids_v2:
                                checked_fids_v2.append(fid)
                        c1.write(row_show.get("home", ""))
                        c2.write(row_show.get("away", ""))
                        dia_mes_ano, hora_min = format_date_chile(row_show.get("date"))
                        c3.write(dia_mes_ano)
                        c4.write(hora_min)
                        p1, px, p2 = _norm_prob_display(row_show.get("prob_home_win")), _norm_prob_display(row_show.get("prob_draw")), _norm_prob_display(row_show.get("prob_away_win"))
                        xg, cs_h, cs_a = row_show.get("expected_goals"), _norm_prob_display(row_show.get("clean_sheet_home")), _norm_prob_display(row_show.get("clean_sheet_away"))
                        btts, over25 = _norm_prob_display(row_show.get("prob_btts")), _norm_prob_display(row_show.get("prob_over25"))
                        c5.write(f"{p1:.0%}" if p1 is not None else "—")
                        c6.write(f"{px:.0%}" if px is not None else "—")
                        c7.write(f"{p2:.0%}" if p2 is not None else "—")
                        c8.write(f"{xg:.1f}" if xg is not None else "—")
                        c9.write(f"{cs_h:.0%}" if cs_h is not None else "—")
                        c10.write(f"{cs_a:.0%}" if cs_a is not None else "—")
                        c11.write(f"{btts:.0%}" if btts is not None else "—")
                        c12.write(f"{over25:.0%}" if over25 is not None else "—")
                        val_parts = []
                        vb = row_show.get("value_bet")
                        if vb:
                            val_parts = [s.strip() for s in str(vb).replace(",", " ").split() if s.strip() in ("1", "X", "2")]
                        if not val_parts and (row_show.get("value_home") or row_show.get("value_draw") or row_show.get("value_away")):
                            if row_show.get("value_home"):
                                val_parts.append("1")
                            if row_show.get("value_draw"):
                                val_parts.append("X")
                            if row_show.get("value_away"):
                                val_parts.append("2")
                        c13.write(",".join(val_parts) if val_parts else "—")
                    st.markdown("---")
            st.session_state.selected_fixture_ids = checked_fids_v2[:MAX_SELECT_V2]
        else:
            st.info("No hay partidos de hoy ni mañana. Puedes sincronizar en **Partidos del día** y volver.")
            st.markdown("---")

        v2_fids = st.session_state.get("selected_fixture_ids") or []
        n_sel_v2 = len(v2_fids)
        balance_v2 = int(st.session_state.current_user.get("credits_balance") or 0) if st.session_state.current_user and st.session_state.current_user.get("role") != "admin" else 999
        if n_sel_v2 == 0 and display_rows_v2_full:
            st.info("Selecciona hasta 10 partidos en la lista de arriba y pulsa **Generar V2**.")
        elif n_sel_v2 > 0 and fixture_to_match_v2_full:
            if n_sel_v2 > balance_v2 and balance_v2 > 0 and st.session_state.current_user and st.session_state.current_user.get("role") != "admin":
                faltantes_v2 = n_sel_v2 - balance_v2
                st.warning(f"Tienes **{balance_v2} créditos**. Selecciona solo hasta {balance_v2} partidos, o el sistema analizará **{balance_v2} partidos elegidos aleatoriamente** de los {n_sel_v2} seleccionados.")
                st.caption("O compra los créditos faltantes:")
                _render_buy_credits_ui(faltantes_v2, "buy_cr_v2")
            elif n_sel_v2 > 0 and balance_v2 <= 0 and st.session_state.current_user and st.session_state.current_user.get("role") != "admin":
                st.info("No tienes créditos. Compra los que necesitas para este análisis:")
                _render_buy_credits_ui(n_sel_v2, "buy_cr_v2_zero")
            fixture_to_match_v2 = {}
            for fid in v2_fids:
                m = fixture_to_match_v2_full.get(fid)
                if m:
                    fixture_to_match_v2[fid] = dict(m, home=m.get("home_team"), away=m.get("away_team"), date=m.get("date") or m.get("match_date_utc"), league=m.get("league_name") or m.get("league_code"))
            display_rows_v2 = []
            for fid in v2_fids:
                r = next((x for x in display_rows_v2_full if x.get("fixture_id") == fid), None)
                if r:
                    display_rows_v2.append(r)

            def _get_match_data_v2(fids=None):
                ids = fids if fids is not None else v2_fids
                raw = [fixture_to_match_v2.get(fid) or next((r for r in display_rows_v2 if r.get("fixture_id") == fid), {}) for fid in ids]
                return [dict(m, home_team=m.get("home") or m.get("home_team"), away_team=m.get("away") or m.get("away_team"), league_name=m.get("league"), league=m.get("league"), date=m.get("date"), fixture_id=m.get("fixture_id")) for m in raw]

            def _build_db_context_v2(match_data_list):
                """Contexto (forma, H2H) para V2 desde tabla maestra: master_table_checked → historical_matches; fallback API-Sports."""
                if not match_data_list:
                    return None
                cache_form = {}
                cache_h2h = {}
                cache_team_id = {}
                parts = []
                use_master = True
                for m in match_data_list:
                    fid = m.get("fixture_id")
                    home = m.get("home_team") or m.get("home") or "Local"
                    away = m.get("away_team") or m.get("away") or "Visitante"
                    home_id = m.get("home_team_id")
                    away_id = m.get("away_team_id")
                    lcode = m.get("league_code") or m.get("league_id")
                    part = [f"{home} vs {away}:"]
                    try:
                        use_fallback = fid is not None and int(fid) >= FALLBACK_FIXTURE_ID_MIN
                    except (TypeError, ValueError):
                        use_fallback = False
                    try:
                        if use_fallback:
                            as_home_id, as_away_id = home_id, away_id
                        else:
                            if home not in cache_team_id:
                                cache_team_id[home] = get_team_id_by_name(home)
                            if away not in cache_team_id:
                                cache_team_id[away] = get_team_id_by_name(away)
                            as_home_id, as_away_id = cache_team_id[home], cache_team_id[away]
                        # Forma: solo tabla historical (master_table_checked → historical_matches)
                        form_h = None
                        if get_recent_form_historical and (lcode or "").strip():
                            try:
                                form_h = get_recent_form_historical(team_id=as_home_id, team_name=home, league_id=lcode or "", last_n=5, use_master_checked=use_master)
                            except Exception:
                                pass
                        if form_h:
                            part.append(f"  Forma local (goles últimos 5): {', '.join([str(f.get('goals_for', '?')) for f in form_h])}")
                        form_a = None
                        if get_recent_form_historical and (lcode or "").strip():
                            try:
                                form_a = get_recent_form_historical(team_id=as_away_id, team_name=away, league_id=lcode or "", last_n=5, use_master_checked=use_master)
                            except Exception:
                                pass
                        if form_a:
                            part.append(f"  Forma visitante: {', '.join([str(f.get('goals_for', '?')) for f in form_a])}")
                        h2h = []
                        if get_h2h_historical and (lcode or "").strip():
                            try:
                                h2h = get_h2h_historical(home_id=as_home_id, away_id=as_away_id, home_name=home, away_name=away, league_id=lcode or "", last_n=5, use_master_checked=use_master)
                            except Exception:
                                pass
                        if not h2h and as_home_id and as_away_id:
                            h2h_key = (min(as_home_id, as_away_id), max(as_home_id, as_away_id))
                            if h2h_key not in cache_h2h:
                                cache_h2h[h2h_key] = get_h2h_api_sports(as_home_id, as_away_id, limit=5)
                            h2h = cache_h2h[h2h_key]
                        if not h2h and fid and not use_fallback:
                            h2h = get_h2h(fid, limit=5, use_mock=False)
                        if h2h:
                            h2h_str = _format_h2h_string(h2h)
                            part.append(f"  H2H: {h2h_str}")
                    except Exception:
                        log.exception("_build_db_context_v2: error forma/H2H para %s vs %s (fid=%s)", home, away, fid)
                    parts.append("\n".join(part))
                return "\n\n".join(parts) if parts else None

            def _render_mini_table_v2(selected_fids, stats_by_fixture, fixture_to_match, col_ctx, title):
                if not selected_fids or not stats_by_fixture:
                    col_ctx.caption("Sin datos. Genera la propuesta para ver la tabla.")
                    return
                col_ctx.markdown(f"**{title}**")
                labels = ["Local", "Visit.", "Día", "Hora", "V.1", "X", "V.2", "xG", "CS L", "CS V", "BTTS", "O2.5", "Value"]
                h = col_ctx.columns([0.12, 0.10, 0.08, 0.06, 0.05, 0.04, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.06])
                for i, lb in enumerate(labels):
                    h[i].caption(lb)
                for fid in selected_fids:
                    m = fixture_to_match.get(fid) or {}
                    s = stats_by_fixture.get(fid) or {}
                    dia, hora = format_date_chile(m.get("date") or "")
                    p1, px, p2 = _norm_prob_display(s.get("prob_home_win")), _norm_prob_display(s.get("prob_draw")), _norm_prob_display(s.get("prob_away_win"))
                    xg = s.get("expected_goals")
                    cs_h, cs_a = _norm_prob_display(s.get("clean_sheet_home")), _norm_prob_display(s.get("clean_sheet_away"))
                    btts, over25 = _norm_prob_display(s.get("prob_btts")), _norm_prob_display(s.get("prob_over25"))
                    vb = s.get("value_bet") or "—"
                    row_cols = col_ctx.columns([0.12, 0.10, 0.08, 0.06, 0.05, 0.04, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.06])
                    row_cols[0].write(m.get("home_team") or m.get("home") or "—")
                    row_cols[1].write(m.get("away_team") or m.get("away") or "—")
                    row_cols[2].write(dia)
                    row_cols[3].write(hora)
                    row_cols[4].write(f"{p1:.0%}" if p1 is not None else "—")
                    row_cols[5].write(f"{px:.0%}" if px is not None else "—")
                    row_cols[6].write(f"{p2:.0%}" if p2 is not None else "—")
                    row_cols[7].write(f"{xg:.1f}" if xg is not None else "—")
                    row_cols[8].write(f"{cs_h:.0%}" if cs_h is not None else "—")
                    row_cols[9].write(f"{cs_a:.0%}" if cs_a is not None else "—")
                    row_cols[10].write(f"{btts:.0%}" if btts is not None else "—")
                    row_cols[11].write(f"{over25:.0%}" if over25 is not None else "—")
                    row_cols[12].write(str(vb))

            v2_can_run = n_sel_v2 > 0 and GEMINI_API_KEY and _has_any_credit(st.session_state.current_user)
            if v2_can_run and st.button("Generar V2", key="btn_v2"):
                import time
                ids_to_analyze_v2 = _effective_ids_to_analyze(v2_fids, st.session_state.current_user)
                match_data = _get_match_data_v2(ids_to_analyze_v2)
                db_context = _build_db_context_v2(match_data)
                start_time_v2 = time.time()
                total_steps_v2 = 2
                activity_log_v2 = []

                progress_box_v2 = st.container()
                with progress_box_v2:
                    st.markdown("""
                    <div style="
                        background: #2C2C2E;
                        border: 1px solid #3d3d4d;
                        border-radius: 10px;
                        padding: 1.2rem 1.5rem;
                        margin: 0.5rem 0;
                    ">
                    <p style="margin:0 0 0.5rem 0; font-weight: 600; color: #6495ED; font-size: 16px;">V2</p>
                    <p style="margin:0 0 0.8rem 0; font-size: 15px; color: #A0A0A0;">Ejecutando: Alfred (tabla + análisis) → Reginald (análisis sobre Alfred)</p>
                    </div>
                    """, unsafe_allow_html=True)
                    progress_bar_v2 = st.progress(0)
                    step_placeholder_v2 = st.empty()
                    time_placeholder_v2 = st.empty()
                    log_placeholder_v2 = st.empty()

                def _update_ui_v2(step: int, message: str, log_line: str):
                    elapsed = int(time.time() - start_time_v2)
                    activity_log_v2.append(f"[{elapsed}s] {log_line}")
                    progress_bar_v2.progress((step + 1) / total_steps_v2)
                    step_placeholder_v2.caption(
                        f"**Paso {step + 1} de {total_steps_v2}** — {message}\n\n_{_random_waiting_message()}_"
                    )
                    time_placeholder_v2.caption(f"⏱ Tiempo transcurrido: **{elapsed} s**")
                    with log_placeholder_v2.container():
                        for line in activity_log_v2:
                            st.caption(line)

                try:
                    _update_ui_v2(0, "Alfred analiza partidos y estadísticas...", "Iniciando Alfred.")
                    result_g = ask_grok_proposal_analysis(match_data, db_context=db_context)
                    v2_grok_analysis = result_g.get("analysis") or ""
                    v2_grok_stats = result_g.get("stats_by_fixture") or {}
                    # Grok debe estar siempre disponible en backend; si falta API key o falla, informar claro
                    grok_ok = not (
                        not v2_grok_stats and v2_grok_analysis.strip().startswith("No está configurada")
                    )
                    if not grok_ok:
                        st.error("**Error al generar análisis (Alfred).** La API de Alfred no está configurada en el servidor. Compruebe XAI_API_KEY o GROK_API_KEY en .env.")
                        _update_ui_v2(0, "Alfred no disponible.", "Error: API key no configurada.")
                    else:
                        st.session_state["v2_grok_analysis"] = v2_grok_analysis
                        st.session_state["v2_grok_stats"] = v2_grok_stats
                        st.session_state["v2_match_ids"] = list(ids_to_analyze_v2)
                        _update_ui_v2(0, "Alfred listo.", "Alfred: tabla y análisis completados.")

                        table_lines = []
                        for fid in ids_to_analyze_v2:
                            m = fixture_to_match_v2.get(fid) or {}
                            s = v2_grok_stats.get(fid) or {}
                            p1, px, p2 = _norm_prob_display(s.get("prob_home_win")), _norm_prob_display(s.get("prob_draw")), _norm_prob_display(s.get("prob_away_win"))
                            xg = s.get("expected_goals")
                            vb = s.get("value_bet") or "—"
                            table_lines.append(
                                f"- {m.get('home_team') or m.get('home') or 'Local'} vs {m.get('away_team') or m.get('away') or 'Visitante'}: "
                                f"V.1={f'{p1:.0%}' if p1 is not None else '—'} X={f'{px:.0%}' if px is not None else '—'} "
                                f"V.2={f'{p2:.0%}' if p2 is not None else '—'} xG={f'{xg:.1f}' if xg is not None else '—'} Value={vb}"
                            )
                        table_text = "\n".join(table_lines)
                        user_content = (
                            "**Tabla de estadísticas por partido (generada por Alfred):**\n\n" + table_text
                            + "\n\n---\n\n**Análisis de los partidos por Alfred:**\n\n" + (v2_grok_analysis or "(sin texto)")
                        )
                        system_v2 = (
                            "Eres Reginald, experto en análisis de fútbol. Te paso la tabla de estadísticas por partido "
                            "generada por Alfred y su análisis de los partidos. Analiza la tabla y el análisis de Alfred "
                            "y da tu opinión/síntesis en markdown: qué partidos te parecen más interesantes, coincidencias "
                            "o diferencias con Alfred, y tu recomendación. Responde solo en markdown, sin JSON."
                        )
                        _update_ui_v2(1, "Reginald analiza tabla y análisis de Alfred...", "Iniciando Reginald.")
                        v2_gemini_analysis = ask_gemini_custom(system_v2, user_content)
                        st.session_state["v2_gemini_analysis"] = v2_gemini_analysis or ""
                        _update_ui_v2(1, "Reginald listo.", "Reginald: análisis V2 completado.")

                        elapsed_final_v2 = int(time.time() - start_time_v2)
                        progress_bar_v2.progress(1.0)
                        step_placeholder_v2.caption("✅ **Completado** — V2 generado.")
                        time_placeholder_v2.caption(f"⏱ Tiempo total: **{elapsed_final_v2} s**")
                        with log_placeholder_v2.container():
                            for line in activity_log_v2:
                                st.caption(line)
                            st.caption(f"[{elapsed_final_v2}s] Listo.")
                        _deduct_credits_and_refresh(len(ids_to_analyze_v2))
                        st.success("V2 generado.")
                        st.rerun()
                except Exception as e_v2:
                    log.exception("Generar V2: %s", e_v2)
                    activity_log_v2.append(f"[{int(time.time() - start_time_v2)}s] Error: {e_v2}")
                    with log_placeholder_v2.container():
                        for line in activity_log_v2:
                            st.caption(line)
                    st.error("**Error al generar análisis (Alfred).** Compruebe la configuración del servidor (XAI_API_KEY/GROK_API_KEY) o la conectividad.")
                    with st.expander("Detalle del error"):
                        st.code(traceback.format_exc())
        v2_match_ids = st.session_state.get("v2_match_ids") or []
        v2_grok_stats = st.session_state.get("v2_grok_stats") or {}
        v2_grok_analysis = st.session_state.get("v2_grok_analysis") or ""
        v2_gemini_analysis = st.session_state.get("v2_gemini_analysis") or ""
        if v2_match_ids and (v2_grok_analysis or v2_gemini_analysis):
            matches_v2_display = get_matches_by_fixture_ids(v2_match_ids)
            fixture_to_match_v2 = {m.get("fixture_id"): dict(m, home=m.get("home_team"), away=m.get("away_team"), date=m.get("match_date_utc") or m.get("date"), league=m.get("league_name") or m.get("league_code")) for m in matches_v2_display}
            if v2_grok_stats and fixture_to_match_v2:
                def _render_mini_table_v2_show(selected_fids, stats_by_fixture, fixture_to_match, col_ctx, title):
                    if not selected_fids or not stats_by_fixture:
                        return
                    col_ctx.markdown(f"**{title}**")
                    labels = ["Local", "Visit.", "Día", "Hora", "V.1", "X", "V.2", "xG", "CS L", "CS V", "BTTS", "O2.5", "Value"]
                    h = col_ctx.columns([0.12, 0.10, 0.08, 0.06, 0.05, 0.04, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.06])
                    for i, lb in enumerate(labels):
                        h[i].caption(lb)
                    for fid in selected_fids:
                        m = fixture_to_match.get(fid) or {}
                        s = stats_by_fixture.get(fid) or {}
                        dia, hora = format_date_chile(m.get("date") or "")
                        p1, px, p2 = _norm_prob_display(s.get("prob_home_win")), _norm_prob_display(s.get("prob_draw")), _norm_prob_display(s.get("prob_away_win"))
                        xg = s.get("expected_goals")
                        cs_h, cs_a = _norm_prob_display(s.get("clean_sheet_home")), _norm_prob_display(s.get("clean_sheet_away"))
                        btts, over25 = _norm_prob_display(s.get("prob_btts")), _norm_prob_display(s.get("prob_over25"))
                        vb = s.get("value_bet") or "—"
                        row_cols = col_ctx.columns([0.12, 0.10, 0.08, 0.06, 0.05, 0.04, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.06])
                        row_cols[0].write(m.get("home_team") or m.get("home") or "—")
                        row_cols[1].write(m.get("away_team") or m.get("away") or "—")
                        row_cols[2].write(dia)
                        row_cols[3].write(hora)
                        row_cols[4].write(f"{p1:.0%}" if p1 is not None else "—")
                        row_cols[5].write(f"{px:.0%}" if px is not None else "—")
                        row_cols[6].write(f"{p2:.0%}" if p2 is not None else "—")
                        row_cols[7].write(f"{xg:.1f}" if xg is not None else "—")
                        row_cols[8].write(f"{cs_h:.0%}" if cs_h is not None else "—")
                        row_cols[9].write(f"{cs_a:.0%}" if cs_a is not None else "—")
                        row_cols[10].write(f"{btts:.0%}" if btts is not None else "—")
                        row_cols[11].write(f"{over25:.0%}" if over25 is not None else "—")
                        row_cols[12].write(str(vb))
                _render_mini_table_v2_show(v2_match_ids, v2_grok_stats, fixture_to_match_v2, st, "Datos por partido (V2 - Alfred)")
            if v2_grok_analysis:
                with st.expander("Análisis Alfred (V2)", expanded=False):
                    st.markdown(v2_grok_analysis)
            if v2_gemini_analysis:
                with st.expander("Análisis Reginald (V2)", expanded=True):
                    st.markdown(v2_gemini_analysis)
            full_id_v2 = f"{st.session_state.get('last_proposal_id_grok', 'g')}_{st.session_state.get('last_proposal_id_gemini', 'm')}" if (st.session_state.get("last_proposal_id_grok") or st.session_state.get("last_proposal_id_gemini")) else "v2"
            if st.button("Generar PDF V2", key="gen_pdf_v2"):
                ft = {m["fixture_id"]: dict(m, home=m.get("home_team"), away=m.get("away_team"), date=m.get("match_date_utc") or m.get("date")) for m in get_matches_by_fixture_ids(v2_match_ids)}
                path_v2 = generate_proposal_pdf_v2(full_id_v2, ft, v2_match_ids, v2_grok_stats, v2_grok_analysis, v2_gemini_analysis)
                st.session_state["v2_pdf_path"] = path_v2
                st.session_state["pdf_just_generated_v2"] = True
                st.rerun()
            if st.session_state.get("v2_pdf_path") and os.path.exists(st.session_state.get("v2_pdf_path", "")):
                st.success("PDF listo. Si no se descargó automáticamente, haz clic en **Descargar nuevamente**.")
                _trigger_auto_download_once("pdf_just_generated_v2")
                with open(st.session_state["v2_pdf_path"], "rb") as f:
                    st.download_button("Descargar nuevamente", f, file_name=f"forgewin_v2_{full_id_v2}.pdf", mime="application/pdf", key="dl_pdf_v2")

    # ---------- Historial de propuestas ----------
    elif mode == "Historial de propuestas":
        st.subheader("Historial de propuestas")
        st.caption("Listado por día. Identifica cada propuesta por su **ID numérico** (desde 1000).")
        from datetime import datetime as dt_parse
        with st.expander("Filtros", expanded=False):
            col_d1, col_d2 = st.columns(2)
            with col_d1:
                hist_date_from = st.date_input("Desde (fecha)", value=None, key="hist_date_from")
            with col_d2:
                hist_date_to = st.date_input("Hasta (fecha)", value=None, key="hist_date_to")
            hist_filter_id = st.text_input("Filtrar por ID (opcional)", key="hist_filter_id", placeholder="Ej: 1005 o 1000-1010")
        history_list = get_proposals_for_history()
        # Aplicar filtros
        if history_list:
            def _date_from_created(created):
                if not created:
                    return None
                s = (created.split("T")[0] if "T" in created else (created.split(" ")[0] if " " in created else created[:10]))
                try:
                    return dt_parse.strptime(s[:10], "%Y-%m-%d").date()
                except Exception:
                    return None
            if hist_date_from or hist_date_to or (hist_filter_id and str(hist_filter_id).strip()):
                filtered = []
                for p in history_list:
                    d = _date_from_created(p.get("created_at"))
                    if hist_date_from and d and d < hist_date_from:
                        continue
                    if hist_date_to and d and d > hist_date_to:
                        continue
                    sid = str(hist_filter_id or "").strip()
                    if sid:
                        num = p.get("proposal_number")
                        if num is None:
                            continue
                        if "-" in sid:
                            try:
                                low, high = sid.split("-", 1)
                                low, high = int(low.strip()), int(high.strip())
                                if not (low <= num <= high):
                                    continue
                            except ValueError:
                                if num != sid.strip():
                                    continue
                        else:
                            try:
                                if num != int(sid):
                                    continue
                            except ValueError:
                                if str(num) != sid:
                                    continue
                    filtered.append(p)
                history_list = filtered
        if not history_list:
            st.info("Aún no hay propuestas. Genera análisis en Partidos del día o V2.")
        else:
            from collections import defaultdict
            by_day = defaultdict(list)
            for p in history_list:
                created = p.get("created_at") or ""
                try:
                    if "T" in created:
                        day = created.split("T")[0]
                    else:
                        day = created.split(" ")[0] if " " in created else created[:10]
                except Exception:
                    day = created[:10] if len(created) >= 10 else "—"
                by_day[day].append(p)
            for day in sorted(by_day.keys(), reverse=True):
                st.markdown(f"**{day}**")
                rows = by_day[day]
                for r in rows:
                    num = r.get("proposal_number") or "—"
                    created = r.get("created_at") or ""
                    if "T" in created:
                        try:
                            t = created.split("T")[1][:5]
                        except Exception:
                            t = ""
                    else:
                        t = created.split(" ")[1][:5] if " " in created else ""
                    n_matches = r.get("match_count") or 0
                    acc = r.get("accuracy_pct")
                    acc_str = f" — Acierto: {acc:.0f}%" if acc is not None else ""
                    col1, col2 = st.columns([0.85, 0.15])
                    with col1:
                        st.caption(f"**ID {num}** · {t} · {n_matches} partidos{acc_str}")
                    with col2:
                        if st.button("Ver", key=f"hist_ver_{r.get('id')}"):
                            st.session_state["view_proposal_id"] = r["id"]
                            st.session_state["pending_mode_buscar"] = True
                            st.session_state["came_from_historial"] = True
                            st.rerun()
                st.markdown("---")

    # ---------- Buscar propuesta ----------
    elif mode == "Buscar propuesta":
        if not is_admin:
            st.warning("Esta sección está disponible solo para administradores.")
            st.stop()
        st.subheader("Buscar propuesta por código")
        st.caption("Introduce el **ID numérico** (ej. 1000, 1001) o el código antiguo para ver la propuesta.")
        search_id = st.text_input("ID o código de propuesta", placeholder="Ej: 1000 o 1001", key="search_proposal_id")
        if st.button("Buscar"):
            pid = (search_id or "").strip()
            if not pid:
                st.warning("Escribe un ID o código de propuesta.")
            else:
                prop = get_proposal(pid)
                if not prop:
                    st.error("No se encontró ninguna propuesta con ese ID o código.")
                else:
                    st.session_state["view_proposal_id"] = prop["id"]
                    st.rerun()

        if st.session_state.get("view_proposal_id"):
            pid = st.session_state["view_proposal_id"]
            prop = get_proposal(pid)
            if not prop:
                st.session_state["view_proposal_id"] = None
            else:
                st.markdown("---")
                if st.session_state.get("came_from_historial"):
                    if st.button("← Volver al historial", key="back_to_historial"):
                        st.session_state["view_proposal_id"] = None
                        st.session_state["came_from_historial"] = False
                        st.session_state["pending_mode_historial"] = True
                        st.rerun()
                    st.markdown("")
                disp_id = prop.get("proposal_number") if prop.get("proposal_number") is not None else (pid[:8] + "..." if len(pid) > 8 else pid)
                st.subheader(f"Propuesta **#{disp_id}**")
                # Fase 1: acierto por propuesta completa (evaluada cuando hay resultados)
                tips_total = prop.get("tips_total")
                tips_fulfilled = prop.get("tips_fulfilled")
                accuracy_pct = prop.get("accuracy_pct")
                if tips_total is not None and tips_total > 0 and accuracy_pct is not None:
                    st.metric("Acierto de esta propuesta", f"{accuracy_pct:.1f}%", f"{tips_fulfilled or 0} de {tips_total} partidos con resultado")
                grok_stats_view = prop.get("grok_stats") or {}
                all_fids = []
                for bets in prop.get("bets_by_risk", {}).values():
                    for b in bets:
                        fid = b.get("fixture_id")
                        if fid and fid not in all_fids:
                            all_fids.append(fid)
                matches_info = {m["fixture_id"]: m for m in get_matches_by_fixture_ids(all_fids)} if all_fids else {}
                if grok_stats_view and all_fids:
                    st.markdown("**Datos por partido (Alfred)**")
                    cols = st.columns([0.11, 0.11, 0.07, 0.06, 0.06, 0.05, 0.06, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05])
                    for idx, label in enumerate(["Local", "Visitante", "Día", "Hora", "Vic. local", "Empate", "Vic. visit.", "Goles esp.", "CS local", "CS visit.", "BTTS", "Over 2.5", "Value"]):
                        cols[idx].caption(label)
                    for fid in all_fids:
                        m = matches_info.get(fid) or {}
                        s = grok_stats_view.get(fid) or {}
                        home = m.get("home_team") or "—"
                        away = m.get("away_team") or "—"
                        date_str = m.get("match_date_utc") or m.get("date") or ""
                        dia, hora = format_date_chile(date_str)
                        p1, px, p2 = _norm_prob_display(s.get("prob_home_win")), _norm_prob_display(s.get("prob_draw")), _norm_prob_display(s.get("prob_away_win"))
                        xg, cs_h, cs_a = s.get("expected_goals"), _norm_prob_display(s.get("clean_sheet_home")), _norm_prob_display(s.get("clean_sheet_away"))
                        btts, over25 = _norm_prob_display(s.get("prob_btts")), _norm_prob_display(s.get("prob_over25"))
                        vb = s.get("value_bet") or "—"
                        row_cols = st.columns([0.11, 0.11, 0.07, 0.06, 0.06, 0.05, 0.06, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05])
                        row_cols[0].write(home)
                        row_cols[1].write(away)
                        row_cols[2].write(dia)
                        row_cols[3].write(hora)
                        row_cols[4].write(f"{p1:.0%}" if p1 is not None else "—")
                        row_cols[5].write(f"{px:.0%}" if px is not None else "—")
                        row_cols[6].write(f"{p2:.0%}" if p2 is not None else "—")
                        row_cols[7].write(f"{xg:.1f}" if xg is not None else "—")
                        row_cols[8].write(f"{cs_h:.0%}" if cs_h is not None else "—")
                        row_cols[9].write(f"{cs_a:.0%}" if cs_a is not None else "—")
                        row_cols[10].write(f"{btts:.0%}" if btts is not None else "—")
                        row_cols[11].write(f"{over25:.0%}" if over25 is not None else "—")
                        row_cols[12].write(str(vb) if vb != "—" else "—")
                    st.markdown("---")
                # Resultados evaluados (1X2 por partido)
                outcomes = get_proposal_outcomes(pid)
                outcomes_1x2 = [(fid, outcomes[(fid, "1x2")]) for fid in all_fids if (fid, "1x2") in outcomes]
                if outcomes_1x2:
                    with st.expander("Resultados por partido (1X2 vs recomendación)", expanded=False):
                        for fid, out in outcomes_1x2:
                            m = matches_info.get(fid) or {}
                            home = m.get("home_team") or "—"
                            away = m.get("away_team") or "—"
                            st.caption(f"**{home}** vs **{away}** — Resultado: {out.get('actual_1x2')} ({out.get('actual_home')}-{out.get('actual_away')}) — {'Acertó' if out.get('hit') else 'Falló'}")
                analysis = prop.get("grok_analysis") or ""
                if analysis.strip():
                    st.markdown("**Análisis y recomendación**")
                    st.markdown(analysis)
                if not analysis.strip():
                    # Propuestas antiguas: mostrar por nivel de riesgo y resultados
                    outcomes = get_proposal_outcomes(pid)
                    for key in ("poco", "moderado", "arriesgada"):
                        bets = prop.get("bets_by_risk", {}).get(key, [])
                        if not bets:
                            continue
                        st.markdown(f"### Nivel: {key}")
                        for b in bets:
                            fid = b.get("fixture_id")
                            details = b.get("details") or {}
                            pred_win = details.get("winner") or b.get("prediction") or "—"
                            m = matches_info.get(fid) or {}
                            home = m.get("home_team") or "Local"
                            away = m.get("away_team") or "Visitante"
                            outcome = outcomes.get((fid, key))
                            with st.container():
                                st.markdown(f"**{home}** vs **{away}** — Predicción: {pred_win}")
                                if outcome:
                                    st.caption(f"Resultado real: {outcome.get('actual_1x2')} ({outcome.get('actual_home')}-{outcome.get('actual_away')}) — {'Acertó' if outcome.get('hit') else 'Falló'}")
                                st.markdown("---")
                if prop.get("pdf_path") and os.path.exists(prop["pdf_path"]):
                    with open(prop["pdf_path"], "rb") as f:
                        fn = f"propuesta_{prop.get('proposal_number', pid)}.pdf"
                        st.download_button("Descargar nuevamente", f, file_name=fn, mime="application/pdf", key=f"dl_search_{pid}")
                if st.button("Cerrar vista", key="close_view_proposal"):
                    st.session_state["view_proposal_id"] = None
                    st.session_state.pop("came_from_historial", None)
                    st.rerun()

    # ---------- Mis Referidos ----------
    elif mode == "Mis Referidos":
        from referrals import (
            generate_referral_code, get_vlogger_dashboard,
            REFERRAL_PRICE_CLP, NORMAL_PACK_PRICE_CLP, REFERRAL_COMMISSION_CLP,
            get_all_pending_commissions, mark_commission_paid,
        )
        u_ref = st.session_state.current_user
        uid_ref = u_ref["id"]
        uname_ref = u_ref.get("username", "")

        st.title("🤝 Mis Referidos")
        st.caption("Comparte tu código, gana comisiones cuando tus referidos compran su primer pack.")
        st.divider()

        # ── Generar código ────────────────────────────────────────────────────
        dash = get_vlogger_dashboard(uid_ref)
        if not dash["has_code"]:
            st.info("Aún no tienes un código de referido. Genera uno para empezar a ganar comisiones.")
            if st.button("✨ Generar mi código de referido", type="primary"):
                ok_gen, result_gen = generate_referral_code(uid_ref, uname_ref)
                if ok_gen:
                    st.success(f"¡Código generado: **{result_gen}**!")
                    st.rerun()
                else:
                    st.error(result_gen)
        else:
            code_ref = dash["code"]
            link_ref = dash["referral_link"]

            # ── Panel principal ───────────────────────────────────────────────
            col_code, col_link = st.columns([1, 2])
            with col_code:
                st.markdown("**Tu código**")
                st.markdown(
                    f"<div style='background:#1c1c1e;border:2px solid #ff6b35;border-radius:10px;"
                    f"padding:1rem 1.5rem;font-size:1.8rem;font-weight:700;letter-spacing:0.1em;"
                    f"text-align:center;color:#ff6b35'>{code_ref}</div>",
                    unsafe_allow_html=True,
                )
            with col_link:
                st.markdown("**Enlace de referido**")
                st.code(link_ref, language=None)
                st.caption("Compártelo en tus redes y videos. Cuando alguien lo use, ganas comisión.")

            st.divider()

            # ── Métricas ──────────────────────────────────────────────────────
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Referidos", dash["referral_count"])
            c2.metric("Comisión total", f"${dash['total_commission']:,} CLP".replace(",", "."))
            c3.metric("Pendiente de pago", f"${dash['pending_commission']:,} CLP".replace(",", "."))
            c4.metric("Ya pagado", f"${dash['paid_commission']:,} CLP".replace(",", "."))

            st.divider()

            # ── Cómo funciona ─────────────────────────────────────────────────
            with st.expander("ℹ️ ¿Cómo funciona?", expanded=False):
                st.markdown(f"""
- Tu referido ingresa el código **{code_ref}** al comprar su primer pack de créditos.
- Paga **${REFERRAL_PRICE_CLP:,} CLP** en vez de ${NORMAL_PACK_PRICE_CLP:,} CLP (30% de descuento).
- Tú recibes **${REFERRAL_COMMISSION_CLP:,} CLP de comisión** (se registra como pendiente).
- Las comisiones pendientes se pagan por transferencia una vez al mes.
- Solo aplica en la **primera compra** del usuario referido.
                """.replace(",", "."))

            # ── Lista de referidos ────────────────────────────────────────────
            st.subheader("Historial de referidos")
            if dash["usages"]:
                for usage in dash["usages"]:
                    fecha = (usage.get("created_at") or "")[:10]
                    estado_color = "🟢" if usage["status"] == "paid" else "🟡"
                    st.markdown(
                        f"{estado_color} **{usage.get('referred_username', '—')}** — "
                        f"compró ${usage['purchase_amount']:,} CLP — "
                        f"comisión ${usage['commission_paid']:,} CLP — "
                        f"estado: **{usage['status']}** — {fecha}".replace(",", ".")
                    )
            else:
                st.info("Aún no tienes referidos. ¡Comparte tu enlace!")

        # ── Panel admin: comisiones pendientes ────────────────────────────────
        if u_ref.get("role") == "admin":
            st.divider()
            st.subheader("⚙️ Admin — Comisiones pendientes")
            pending_all = get_all_pending_commissions()
            if pending_all:
                for p in pending_all:
                    col_a, col_b = st.columns([4, 1])
                    col_a.markdown(
                        f"**{p['vlogger_username']}** ← referido por **{p['referred_username']}** — "
                        f"${p['commission_paid']:,} CLP — {(p.get('created_at') or '')[:10]}".replace(",", ".")
                    )
                    if col_b.button("Marcar pagada", key=f"pay_comm_{p['id']}"):
                        mark_commission_paid(p["id"])
                        st.rerun()
            else:
                st.success("No hay comisiones pendientes.")

    # ---------- Administración ----------
    elif mode == "Administración":
        if not st.session_state.current_user or st.session_state.current_user.get("role") != "admin":
            st.subheader("Administración")
            st.warning("Solo los administradores pueden acceder. Inicia sesión como admin (por defecto: admin / admin2026!).")
            st.stop()

        # ---------- Verificación de datos (discrepancias + chatbot) ----------
        st.subheader("Verificación de datos")
        discrep = get_pending_discrepancies()
        if discrep:
            st.markdown("**Discrepancias pendientes** (master_table vs API-Sports)")
            filt_league = st.selectbox(
                "Filtrar liga",
                options=["Todas"] + list(LEAGUES.keys()),
                key="disp_league_filter",
            )
            filt_season = st.selectbox(
                "Filtrar temporada",
                options=["Todas"] + sorted(set(d.get("season") for d in discrep if d.get("season")), reverse=True),
                format_func=lambda x: str(x) if x != "Todas" else "Todas",
                key="disp_season_filter",
            )
            to_show = discrep
            if filt_league and filt_league != "Todas":
                lid = get_league_id(filt_league) or get_league_code_from_name(filt_league)
                to_show = [d for d in to_show if d.get("league_id") == lid]
            if filt_season != "Todas" and filt_season is not None:
                to_show = [d for d in to_show if d.get("season") == filt_season]

            for d in to_show:
                etype = d.get("entity_type") or "standings_champion"
                field = d.get("field") or ""
                is_champion = etype == "standings_champion" and field == "champion"
                with st.expander(f"{get_league_name(d.get('league_id') or '')} {d.get('season')} — {field} (ID {d.get('entity_id', '')})"):
                    st.write(f"**master_table:** {d.get('value_source_a', '')}")
                    st.write(f"**API-Sports:** {d.get('value_source_b', '')}")
                    choice = st.radio(
                        "¿Cuál usar?",
                        ["master_table", "api_sports", "manual"],
                        format_func=lambda x: {
                            "master_table": "Usar valor de master_table",
                            "api_sports": "Usar valor de API-Sports",
                            "manual": "Editar manualmente",
                        }.get(x, x),
                        key=f"resolve_choice_{d['id']}",
                        horizontal=True,
                    )
                    manual_team = None
                    manual_pts = None
                    if choice == "manual":
                        if is_champion:
                            manual_team = st.text_input(
                                "Nombre del campeón (se normaliza con entity_aliases)",
                                key=f"manual_team_{d['id']}",
                            )
                            manual_pts = st.number_input("Puntos", min_value=0, max_value=150, value=90, key=f"manual_pts_{d['id']}")
                        else:
                            manual_team = st.text_input(
                                "Valor manual (nombres de equipo se normalizan)",
                                key=f"manual_team_{d['id']}",
                                placeholder="Ej: Man United, 2, H",
                            )
                    btn_label = "Resolver y aplicar" if is_champion else "Resolver"
                    if st.button(btn_label, key=f"resolve_btn_{d['id']}"):
                        manual_val = manual_team if choice == "manual" and manual_team else None
                        ok, msg = resolve_discrepancy(
                            d["id"],
                            choice=choice,
                            manual_value=manual_val,
                            manual_points=manual_pts if choice == "manual" else None,
                            resolved_by=st.session_state.current_user.get("id"),
                        )
                        if ok:
                            st.success(msg)
                            st.rerun()
                        else:
                            st.error(msg)
        else:
            st.caption("No hay discrepancias pendientes. Ejecuta `python run_detect_discrepancies.py` para detectar.")

        st.markdown("---")
        st.markdown("**Nombres de equipos a normalizar**")
        st.caption("Mismo equipo con dos nombres (ej. Blackburn Rovers FC / Blackburn). Elige el canónico; el otro se guarda como alias.")
        pending_aliases = get_pending_team_aliases()
        if not pending_aliases:
            st.caption("No hay pendientes. Ejecuta `python run_detect_team_name_duplicates.py` para detectar candidatos.")
        else:
            for pa in pending_aliases:
                pid = pa.get("id")
                lid = pa.get("league_id") or ""
                n1 = pa.get("name_variant_1") or ""
                n2 = pa.get("name_variant_2") or ""
                with st.expander(f"{get_league_name(lid) or lid} — «{n1}» / «{n2}»", expanded=True):
                    st.caption("Elige cuál nombre será el canónico (el otro se registrará como alias).")
                    canonical_choice = st.radio(
                        "Nombre canónico",
                        [n1, n2],
                        key=f"team_alias_canon_{pid}",
                        horizontal=True,
                    )
                    if st.button("Resolver y guardar alias", key=f"team_alias_btn_{pid}"):
                        ok, msg = resolve_team_alias(
                            pid,
                            canonical_choice,
                            resolved_by=st.session_state.current_user.get("id"),
                        )
                        if ok:
                            st.success(msg)
                            st.rerun()
                        else:
                            st.error(msg)
        st.markdown("---")
        st.markdown("**Chatbot Grok (verificación humana)**")
        if not GROK_API_KEY:
            st.caption("Configura XAI_API_KEY o GROK_API_KEY en .env para usar el chatbot.")
        verif_league = st.selectbox(
            "Liga (contexto)",
            options=["—"] + list(LEAGUES.keys()),
            key="admin_verify_league",
        )
        verif_season = st.number_input(
            "Temporada (año fin, ej. 2024)",
            min_value=2015,
            max_value=2030,
            value=2024,
            key="admin_verify_season",
        )
        match_context = None
        if verif_league and verif_league != "—":
            lid = get_league_id(verif_league) or get_league_code_from_name(verif_league)
            if lid:
                match_context = f"Contexto: liga {verif_league} ({lid}), temporada {verif_season} ({verif_season - 1}/{verif_season}). El admin está verificando datos de clasificación o campeón."
        for msg in st.session_state.admin_verify_chat_history:
            with st.chat_message(msg["role"]):
                st.markdown(msg["content"])
        uploaded = st.file_uploader(
            "Adjuntar imagen(es) para verificación (tabla oficial, clasificación, etc.)",
            type=["png", "jpg", "jpeg"],
            accept_multiple_files=True,
            key="admin_verify_img_upload",
        )
        if uploaded:
            mime_map = {"png": "image/png", "jpg": "image/jpeg", "jpeg": "image/jpeg"}
            st.session_state.admin_verify_pending_images = [
                {"bytes": f.read(), "mime": mime_map.get((f.name or "").split(".")[-1].lower()) or "image/jpeg"}
                for f in uploaded
            ]
        if prompt := st.chat_input("Pregunta a Grok sobre la verificación de esta liga/temporada", key="admin_verify_chat", disabled=not GROK_API_KEY):
            pending = st.session_state.get("admin_verify_pending_images") or []
            user_display = prompt
            if pending:
                user_display = f"📎 {len(pending)} imagen(es) adjunta(s)\n\n{prompt}" if prompt.strip() else f"📎 {len(pending)} imagen(es) adjunta(s)"
            st.session_state.admin_verify_chat_history.append({"role": "user", "content": user_display})
            st.session_state.admin_verify_pending_images = []
            with st.chat_message("assistant"):
                with st.spinner("Pensando..."):
                    try:
                        reply = ask_grok_chat(
                            prompt,
                            conversation_history=st.session_state.admin_verify_chat_history[:-1],
                            match_context=match_context,
                            image_parts=pending if pending else None,
                        )
                        st.markdown(reply)
                        st.session_state.admin_verify_chat_history.append({"role": "assistant", "content": reply})
                    except Exception as e:
                        err = str(e)
                        st.error(err)
                        st.session_state.admin_verify_chat_history.append({"role": "assistant", "content": f"Error: {err}"})
            st.rerun()
        if st.session_state.admin_verify_chat_history and st.button("Borrar chat", key="admin_verify_clear"):
            st.session_state.admin_verify_chat_history = []
            st.session_state.admin_verify_pending_images = []
            st.rerun()

        st.markdown("---")
        st.subheader("Gestión de usuarios")
        users = list_users()
        df = pd.DataFrame(users)
        cols = [c for c in ["id", "username", "email", "role", "tier", "credits_balance", "grok_enabled"] if c in df.columns]
        st.dataframe(df[cols] if cols else df, use_container_width=True)
        st.markdown("**Crear usuario**")
        new_user = st.text_input("Usuario", key="new_username")
        new_pass = st.text_input("Contraseña", type="password", key="new_pass")
        new_email = st.text_input("Email", key="new_email")
        new_tier = st.radio("Perfil", ["base", "plus"], key="new_tier", horizontal=True)
        new_grok = st.checkbox("Activar Alfred para este usuario", value=False, key="new_grok")
        if st.button("Crear usuario"):
            ok, msg = create_user(new_user, new_pass, new_email, role="user", grok_enabled=new_grok, tier=new_tier)
            if ok:
                st.success(msg)
                st.rerun()
            else:
                st.error(msg)
        st.markdown("---")
        st.markdown("**Asignar perfil (base / plus / plus++)**")
        for u in users:
            uid, uname, role, tier = u.get("id"), u.get("username"), u.get("role"), u.get("tier") or "base"
            if role == "admin":
                continue
            col1, col2, col3 = st.columns([2, 1.5, 1])
            with col1:
                st.write(f"**{uname}** — actual: {tier}")
            with col2:
                tier_opts = ["base", "plus", "plus_plus"]
                tier_idx = tier_opts.index(tier) if tier in tier_opts else 0
                new_tier_u = st.radio("Nivel de usuario", tier_opts, index=tier_idx, key=f"tier_{uid}", horizontal=True, label_visibility="collapsed")
            with col3:
                if st.button("Aplicar", key=f"apply_tier_{uid}"):
                    set_user_tier(uid, new_tier_u)
                    st.success(f"{uname} → {new_tier_u}")
                    st.rerun()
        st.markdown("---")
        st.markdown("**Reportes de error** (enviados por usuarios)")
        try:
            reports = get_error_reports(limit=50)
            if reports:
                df_err = pd.DataFrame(reports)
                st.dataframe(df_err, use_container_width=True, height=200)
            else:
                st.caption("Aún no hay reportes.")
        except Exception as e:
            log.exception("Error listando reportes: %s", e)
            st.caption("No se pudieron cargar los reportes.")
        st.markdown("---")
        st.markdown("**Añadir créditos (simular compra)** — El tipo de pack define el perfil del usuario.")
        non_admin = [u for u in users if u.get("role") != "admin"]
        if non_admin:
            buy_uid = st.selectbox("Usuario", options=[u["id"] for u in non_admin], format_func=lambda x: next((u["username"] for u in users if u.get("id") == x), str(x)), key="buy_credits_user")
            buy_amount = st.number_input("Créditos", min_value=1, max_value=500, value=50, key="buy_credits_amount")
            buy_pack = st.radio("Tipo de pack", ["base", "plus", "plus_plus"], key="buy_pack_tier", horizontal=True)
        if non_admin and st.button("Añadir créditos (y asignar perfil según pack)"):
            ok, msg = add_credits(buy_uid, buy_amount, buy_pack)
            if ok:
                st.success(msg)
                if st.session_state.current_user.get("id") == buy_uid:
                    u = get_user_by_id(buy_uid)
                    if u:
                        st.session_state.current_user["credits_balance"] = int(u.get("credits_balance") or 0)
                        st.session_state.current_user["tier"] = u.get("tier") or "base"
                st.rerun()
            else:
                st.error(msg)
        st.markdown("---")
        st.markdown("**Activar/desactivar Alfred**")
        for u in users:
            uid, uname, grok = u.get("id"), u.get("username"), u.get("grok_enabled")
            if u.get("role") == "admin":
                continue
            col1, col2 = st.columns([2, 1])
            with col1:
                st.write(f"{uname} — Alfred: {'activado' if grok else 'desactivado'}")
            with col2:
                if st.button("Cambiar", key=f"grok_{uid}"):
                    set_grok_enabled(uid, not grok)
                    st.rerun()

    else:
        st.write("Selecciona un modo en la barra lateral.")
