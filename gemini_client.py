"""
Cliente Reginald (API Google/Gemini) para análisis de propuestas.
Mismo prompt y mismo formato de respuesta que Alfred (stats JSON + markdown).
Usa REST API: generativelanguage.googleapis.com/v1beta
"""

import logging
import time
from datetime import date
from typing import Any, Dict, List, Optional

import requests

from config import GEMINI_API_KEY, GEMINI_MODEL, GEMINI_MODEL_CONSENSUS, GEMINI_BASE
from grok_client import (
    PROPOSAL_SYSTEM_PROMPT,
    PROPOSAL_SYSTEM_PROMPT_V3,
    PROPOSAL_USER_INSTRUCTIONS,
    PROPOSAL_USER_INSTRUCTIONS_V3,
    _build_match_data_context,
    _parse_grok_proposal_response,
)

log = logging.getLogger(__name__)



def _call_gemini(
    system: str,
    user_content: str,
    timeout: int = 180,
    model: Optional[str] = None,
    temperature: Optional[float] = None,
) -> str:
    """Llama a la API de Reginald (generateContent). model/temperature opcionales para síntesis con Pro."""
    if not GEMINI_API_KEY:
        return ""
    model = model or GEMINI_MODEL
    url = f"{GEMINI_BASE}/models/{model}:generateContent"
    headers = {
        "x-goog-api-key": GEMINI_API_KEY,
        "Content-Type": "application/json",
    }
    gen_config = {
        "temperature": temperature if temperature is not None else 0.4,
        "maxOutputTokens": 8192,
    }
    if model == "gemini-2.5-pro" or "2.5-pro" in (model or ""):
        gen_config.setdefault("topP", 0.95)
    # Reducir consumo de tokens (TPM): 429 puede ser por límite de tokens, no solo RPM.
    # Docs: https://ai.google.dev/gemini-api/docs/thinking
    m = (model or "").lower()
    if "2.5-flash" in m:
        gen_config["thinkingConfig"] = {"thinkingBudget": 0}  # Desactivar thinking → menos TPM
    elif "2.5-pro" in m:
        gen_config["thinkingConfig"] = {"thinkingBudget": 128}  # Mínimo razonamiento para no saturar TPM
    elif "3-flash" in m or "gemini-3" in m:
        gen_config["thinkingConfig"] = {"thinkingLevel": "low"}
    payload = {
        "system_instruction": {"parts": [{"text": system}]},
        "contents": [{"role": "user", "parts": [{"text": user_content}]}],
        "safetySettings": [
            {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_NONE"},
            {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_NONE"},
            {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_NONE"},
            {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_NONE"},
        ],
        "generationConfig": gen_config,
    }
    # Exponential backoff ante 429 y errores retryables (1s, 2s, 4s, 8s, 16s, 32s; máx 120s)
    max_retries = 6
    for attempt in range(max_retries):
        r = requests.post(url, json=payload, headers=headers, timeout=timeout)
        if r.status_code == 429:
            try:
                err_body = r.json()
                log.warning("Reginald 429: %s", err_body)
            except Exception:
                log.warning("Reginald 429: %s", r.text[:500] if r.text else r.status_code)
            wait = min(2 ** attempt, 120)
            try:
                header_wait = int(r.headers.get("Retry-After", 0))
                if header_wait > 0:
                    wait = max(wait, min(header_wait, 120))
            except (ValueError, TypeError):
                pass
            if attempt < max_retries - 1:
                log.warning(
                    "Reginald 429. Reintento en %s s (intento %s/%s).",
                    wait, attempt + 1, max_retries,
                )
                time.sleep(wait)
                continue
            # Último intento y seguimos con 429: no lanzar, devolver mensaje para que la app lo muestre
            log.warning("Reginald 429 tras %s reintentos. Devuelve mensaje de error.", max_retries)
            return (
                "Error 429 (Reginald): Límite de la API de Google alcanzado (solicitudes o tokens por minuto). "
                "Espera 1-2 minutos y vuelve a pulsar «Dale con todo!». Si persiste, revisa cuotas en Google AI Studio."
            )
        if r.status_code != 200:
            log.warning("Reginald API response %s: %s", r.status_code, r.text)
        try:
            r.raise_for_status()
        except requests.RequestException as e:
            if attempt < max_retries - 1:
                wait = min(2 ** attempt, 120)
                log.warning("Reginald error. Reintento en %s s (intento %s/%s).", wait, attempt + 1, max_retries)
                time.sleep(wait)
                continue
            raise
        data = r.json()
        candidates = data.get("candidates") or []
        if not candidates:
            log.warning("Reginald response without candidates: %s", data)
            return ""
        parts = (candidates[0].get("content") or {}).get("parts") or []
        if not parts:
            return ""
        try:
            return (parts[0].get("text") or "").strip()
        except (KeyError, IndexError, TypeError) as e:
            log.exception("Reginald response parse error: %s", e)
            return ""


def ask_gemini_proposal_analysis(
    match_data: List[Dict[str, Any]],
    db_context: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Pide a Reginald un análisis completo de los partidos seleccionados.
    Mismo prompt e instrucciones que Alfred. Devuelve el mismo formato:
    {"analysis": str, "stats_by_fixture": {fixture_id: {...}}}.

    Nota: Si hay muchos partidos y 429 por TPM, en .env usa GEMINI_MODEL=gemini-1.5-flash
    o considera enviar en bloques de 3-5 partidos con pausa entre llamadas.
    """
    if not GEMINI_API_KEY:
        return {
            "analysis": "No está configurada la API key de Reginald. Añade GEMINI_API_KEY (o GOOGLE_API_KEY) en el archivo .env.",
            "stats_by_fixture": {},
        }
    today_iso = date.today().isoformat()
    data_block = _build_match_data_context(match_data)
    user_content = (
        f"Fecha actual: {today_iso}.\n\n"
        "Datos de los partidos:\n\n"
        + data_block
    )
    if db_context and db_context.strip():
        user_content += "\n\nDatos adicionales disponibles en la app (forma reciente, H2H, etc.):\n" + db_context.strip()
    user_content += "\n\n" + PROPOSAL_USER_INSTRUCTIONS
    user_content += "\n\nIMPORTANTE: Incluye el bloque GROK_STATS_JSON + JSON + END_GROK_STATS exactamente como se indica, para que se muestre la tabla de datos por partido."
    raw = _call_gemini(PROPOSAL_SYSTEM_PROMPT, user_content, timeout=180)
    if raw and raw.strip().startswith("Error 429"):
        return {"analysis": raw.strip(), "stats_by_fixture": {}}
    return _parse_grok_proposal_response(raw or "", match_data)


def ask_gemini_proposal_analysis_v3(
    match_data: List[Dict[str, Any]],
    db_context: Optional[str] = None,
    enriched_context: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Análisis V3 con contexto enriquecido (Índice de Asedio, Alerta de Fricción, Dinámica HT/FT).
    Usa PROPOSAL_SYSTEM_PROMPT_V3 y PROPOSAL_USER_INSTRUCTIONS_V3.
    """
    if not GEMINI_API_KEY:
        return {
            "analysis": "No está configurada la API key de Reginald. Añade GEMINI_API_KEY en el archivo .env.",
            "stats_by_fixture": {},
        }
    today_iso = date.today().isoformat()
    data_block = _build_match_data_context(match_data)
    user_content = (
        f"Fecha actual: {today_iso}.\n\n"
        "Datos de los partidos:\n\n"
        + data_block
    )
    if db_context and db_context.strip():
        user_content += "\n\nDatos adicionales (forma reciente, H2H):\n" + db_context.strip()
    if enriched_context and enriched_context.strip():
        user_content += "\n\nDatos de nuestra tabla histórica para Índice de Asedio, Alerta de Fricción y Dinámica HT/FT (usa estos datos con tu propio razonamiento para el análisis):\n\n" + enriched_context.strip()
    else:
        user_content += "\n\nNo se han proporcionado datos enriquecidos (Asedio, Fricción, HT/FT). No incluyas las secciones de Índice de Asedio, Alerta de Fricción ni Dinámica HT/FT en tu análisis."
    user_content += "\n\n" + PROPOSAL_USER_INSTRUCTIONS_V3
    user_content += "\n\nIMPORTANTE: Incluye el bloque GROK_STATS_JSON + JSON + END_GROK_STATS exactamente como se indica."
    raw = _call_gemini(PROPOSAL_SYSTEM_PROMPT_V3, user_content, timeout=180)
    if raw and raw.strip().startswith("Error 429"):
        return {"analysis": raw.strip(), "stats_by_fixture": {}}
    return _parse_grok_proposal_response(raw or "", match_data)


def ask_gemini_custom(
    system_prompt: str,
    user_content: str,
    timeout: int = 180,
    model: Optional[str] = None,
    temperature: Optional[float] = None,
) -> str:
    """Llama a Reginald con un system y un mensaje de usuario. model/temperature para usar Pro en síntesis."""
    return _call_gemini(
        system_prompt, user_content, timeout=timeout, model=model, temperature=temperature
    )
