"""
Módulo de Configuración - ForgeWin: estadísticas y análisis de partidos.
Fuente principal de datos: API-Sports (API_FOOTBALL_KEY). Secundaria: football-data.org v4.
Compatible con Python 3.9 (typing Optional en lugar de X | None).
"""

import os
from pathlib import Path
from typing import Optional

try:
    from dotenv import load_dotenv
    _env_path = Path(__file__).resolve().parent / ".env"
    load_dotenv(_env_path)
except ImportError:
    pass

# football-data.org API v4 - fuente secundaria cuando API-Sports no tiene datos
BASE_URL = "https://api.football-data.org/v4/"
# Token en .env: FOOTBALL_DATA_ORG_TOKEN (o FOOTBALL_DATA_API_KEY)
API_KEY = os.getenv("FOOTBALL_DATA_ORG_TOKEN") or os.getenv("FOOTBALL_DATA_API_KEY") or ""

# Odds: The Odds API (opcional). En .env: ODDS_API_KEY o THE_ODDS_API_KEY
# API-Sports (fuente principal): API_FOOTBALL_KEY en .env (api-sports.io)

# Alfred (xAI) para razonamiento y chatbot. En .env: XAI_API_KEY o GROK_API_KEY
GROK_API_KEY = os.getenv("XAI_API_KEY") or os.getenv("GROK_API_KEY") or ""
GROK_BASE_URL = "https://api.x.ai/v1"
GROK_MODEL = os.getenv("GROK_MODEL") or "grok-4-latest"
# Modelo económico para propuestas y consenso (pasos 2 y 5). Paso crítico final lo hace Reginald (Pro).
GROK_MODEL_FAST = os.getenv("GROK_MODEL_FAST") or "grok-4-fast"

# Reginald (Google) para análisis de propuestas. En .env: GEMINI_API_KEY o GOOGLE_API_KEY
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY") or ""
# Primer análisis (Opción 2 Reginald) y pasos 1–5 del consenso: Flash (menos 429, misma calidad).
GEMINI_MODEL = os.getenv("GEMINI_MODEL") or "gemini-2.0-flash"
# Solo paso 6 del consenso (Propuesta General 1+2): Pro para síntesis final y JSON preciso.
GEMINI_MODEL_CONSENSUS = os.getenv("GEMINI_MODEL_CONSENSUS") or "gemini-2.5-pro"
GEMINI_BASE = os.getenv("GEMINI_BASE") or "https://generativelanguage.googleapis.com/v1beta"

# Rate limit: football-data.org es gratuita (pocas peticiones/min); API-Sports es de pago (api_sports_fetcher).
# Pausa antes de cada GET a football-data.org. Plan gratuito ~10 req/min → mínimo 6 s entre peticiones.
# Override en .env: FOOTBALL_DATA_ORG_REQUEST_DELAY (segundos).
_raw_fd_delay = (os.getenv("FOOTBALL_DATA_ORG_REQUEST_DELAY") or "").strip()
if _raw_fd_delay:
    try:
        REQUEST_DELAY_SECONDS = max(6.0, float(_raw_fd_delay))
    except (ValueError, TypeError):
        REQUEST_DELAY_SECONDS = 6.0
else:
    REQUEST_DELAY_SECONDS = 6.0

# Mercado Pago (pagos y suscripciones). En .env: MERCADOPAGO_ACCESS_TOKEN
MERCADOPAGO_ACCESS_TOKEN = os.getenv("MERCADOPAGO_ACCESS_TOKEN") or os.getenv("MP_ACCESS_TOKEN") or ""
# URL base de la app (para back_url). En producción: https://tu-dominio.com
MERCADOPAGO_BASE_URL = os.getenv("FORGEWIN_BASE_URL") or os.getenv("BASE_URL") or "http://localhost:8501"

# Top 7 equipos por liga (para datos de rendimiento histórico)
TOP_N_TEAMS_PER_LEAGUE = 7

# Ligas: nombre -> código de competición (football-data.org v4)
# Top 10 ligas mundiales + alternativas + Europa League + Copa Libertadores
LEAGUES = {
    "Premier League": "PL",
    "La Liga": "PD",
    "Serie A": "SA",
    "Bundesliga": "BL1",
    "Ligue 1": "FL1",
    "Eredivisie": "DED",
    "Primeira Liga": "PPL",
    "Champions League": "CL",
    "Europa League": "EL",
    "Copa Libertadores": "CLI",
    "League One": "EL1",
    "Championship": "ELC",
}

# Alias para búsqueda por nombre (código de liga)
LEAGUE_ALIASES = {
    "premier": "PL",
    "laliga": "PD",
    "la liga": "PD",
    "primera division": "PD",
    "primera división": "PD",
    "serie a": "SA",
    "bundesliga": "BL1",
    "ligue 1": "FL1",
    "ligue1": "FL1",
    "eredivisie": "DED",
    "primeira": "PPL",
    "primeira liga": "PPL",
    "league one": "EL1",
    "championship": "ELC",
    "champions": "CL",
    "europa": "EL",
    "europa league": "EL",
    "uefa europa": "EL",
    "libertadores": "CLI",
    "copa libertadores": "CLI",
}

# Nombres que devuelven APIs → nombre canónico a mostrar (clave de LEAGUES)
# Así "Primera Division" y "La Liga" no aparecen como dos ligas distintas.
LEAGUE_NAME_TO_CANONICAL = {
    "Primera Division": "La Liga",
    "Primera División": "La Liga",
    "LaLiga": "La Liga",
    "Spanish La Liga": "La Liga",
    "Serie A Italy": "Serie A",
    "Bundesliga Germany": "Bundesliga",
    "Ligue 1 France": "Ligue 1",
    "Primeira Liga Portugal": "Primeira Liga",
    "UEFA Champions League": "Champions League",
    "UEFA Europa League": "Europa League",
}

# Ligas para partidos del día (hoy y mañana): principales ligas + Champions, Europa League, Copa Libertadores
TOP_10_LEAGUE_CODES = ["PL", "PD", "SA", "BL1", "FL1", "DED", "PPL", "CL", "EL", "CLI", "ELC", "EL1"]

# Planes y beneficios (visible en "Conoce los planes")
PLANS_BENEFITS = [
    {
        "id": "mvp_10",
        "name": "Pack 10 análisis",
        "credits": 10,
        "price": "20.000",
        "price_note": "IVA incluido",
        "benefits": [
            "10 créditos de análisis",
            "Partidos de hoy (top 10 ligas)",
            "Revisión por ligas",
            "Análisis 'Dale con todo' (Alfred + Reginald + Propuesta Final)",
            "Historial de tus análisis",
            "Descarga de PDF",
        ],
    },
    {
        "id": "mvp_25",
        "name": "Pack 25 análisis",
        "credits": 25,
        "price": "50.000",
        "price_note": "IVA incluido",
        "benefits": [
            "25 créditos de análisis",
            "Todos los beneficios del pack anterior",
        ],
    },
    {
        "id": "mvp_50",
        "name": "Pack 50 análisis",
        "credits": 50,
        "price": "100.000",
        "price_note": "IVA incluido",
        "benefits": [
            "50 créditos de análisis",
            "Todos los beneficios del pack anterior",
        ],
    },
]

# Logos oficiales: football-data.org emblems (https://crests.football-data.org/{code}.png)
# Logos locales (static/leagues/) para ligas cuyas fuentes externas no cargan bien: DED, EL1
CRESTS_BASE = "https://crests.football-data.org"
_STATIC_LEAGUES_DIR = Path(__file__).resolve().parent / "static" / "leagues"
# Si no se encuentra el logo de liga o equipo, se usa este (balón de fútbol)
FALLBACK_LOGO_URL = "https://upload.wikimedia.org/wikipedia/commons/thumb/6/6e/Football_%28soccer_ball%29.svg/120px-Football_%28soccer_ball%29.svg.png"


def get_league_emblem_url(league_code: str) -> str:
    """URL o ruta local del logo de la competición. Prioriza static/leagues si existe."""
    if league_code:
        local_path = _STATIC_LEAGUES_DIR / f"{league_code}.png"
        if local_path.exists():
            return str(local_path)
    return f"{CRESTS_BASE}/{league_code}.png"


def get_league_id(name: str) -> Optional[str]:
    """Resuelve nombre o alias de liga al código. Case-insensitive."""
    key = name.strip().lower()
    if key in LEAGUE_ALIASES:
        return LEAGUE_ALIASES[key]
    for league_name, code in LEAGUES.items():
        if league_name.lower() == key:
            return code
    return None


def get_league_name(league_code: str) -> str:
    """Devuelve nombre de liga dado su código."""
    for name, code in LEAGUES.items():
        if code == league_code:
            return name
    return league_code


def get_league_code_from_name(league_name: str) -> Optional[str]:
    """Devuelve código de competición dado el nombre (p. ej. API devuelve 'UEFA Champions League')."""
    if not league_name:
        return None
    for name, code in LEAGUES.items():
        if name == league_name or code == league_name:
            return code
    key = (league_name or "").strip()
    if key.lower() in LEAGUE_ALIASES:
        return LEAGUE_ALIASES[key.lower()]
    return None


def get_canonical_league_name(league_name_or_code: str) -> str:
    """
    Devuelve el nombre canónico de liga para mostrar en la UI (una sola etiqueta por competición).
    Ej: 'Primera Division' -> 'La Liga', 'PD' -> 'La Liga'. Evita duplicados tipo LaLiga / Primera Division.
    """
    if not league_name_or_code or not str(league_name_or_code).strip():
        return league_name_or_code or ""
    raw = str(league_name_or_code).strip()
    if raw in LEAGUES:
        return raw
    if raw in LEAGUE_NAME_TO_CANONICAL:
        return LEAGUE_NAME_TO_CANONICAL[raw]
    code = get_league_code_from_name(raw)
    if code:
        return get_league_name(code)
    return raw
