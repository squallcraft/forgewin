"""
Flujo Propuesta General 1+2: Alfred y Reginald intercambian opiniones; la opinión final es Reginald analizando todas las opciones.

Paso 1: Reginald analiza el análisis de Alfred (Opción 1) → opinión (desacuerdos y por qué).
Paso 2: Alfred analiza el análisis de Reginald (Opción 2) → opinión (desacuerdos y por qué).
Paso 3-4: Cada uno recibe la opinión del otro (contexto para los siguientes pasos).
Paso 5: Alfred, con su análisis inicial y las críticas de Reginald, actúa como consultor senior: consenso, discrepancias y recomendación.
Paso 6: Reginald analiza TODAS las opciones anteriormente expuestas (Opción 1, Opción 2, opiniones cruzadas y recomendación de Alfred) y produce la Propuesta General 1+2 para el usuario (stats JSON + markdown). La opinión final del flujo es siempre Reginald.
"""

import logging
from typing import Any, Dict, List

from grok_client import ask_grok_custom, _parse_grok_proposal_response
from gemini_client import ask_gemini_custom
from config import GEMINI_MODEL_CONSENSUS

log = logging.getLogger(__name__)

SYSTEM_CONSENSUS = (
    "Eres un analista experto en fútbol. "
    "Respondes en español. Evalúas análisis de otros expertos con rigor y propones consensos cuando se te pide. "
    "No uses lenguaje de apuestas (apuesta segura, recomendar apostar, cuota, etc.). Expresa expectativas con probabilidad: «es muy probable que...», «es probable que...», «podría ocurrir que...»."
)

# Instrucción para que Reginald (opinión final del flujo) emita la Propuesta General 1+2 en el formato esperado (stats JSON + markdown)
GEMINI_FINAL_FORMAT_INSTRUCTION = """
Debes responder con el siguiente formato exacto:
1) Primero escribe exactamente esta línea: GROK_STATS_JSON
2) En la línea siguiente, un JSON válido: un array de objetos, UNO POR PARTIDO en el mismo orden indicado. Cada objeto debe tener "fixture_id" (número, no texto), "prob_home_win", "prob_draw", "prob_away_win", "expected_goals", "clean_sheet_home", "clean_sheet_away", "prob_btts", "prob_over25", "value_bet" ("1", "X" o "2"). Esos números son TU recomendación de consenso (integrando Alfred y tú).
3) Luego escribe exactamente: END_GROK_STATS
4) Después, tu análisis de consenso en markdown (resumen, discrepancias resueltas, conclusión por partido). No repitas el JSON aquí. En el texto no uses lenguaje de apuestas; usa «es muy probable que...», «es probable que...», «podría ocurrir que...».
"""


def run_consensus_flow(
    grok_analysis: str,
    gemini_analysis: str,
    match_data: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Ejecuta el flujo de 6 pasos y devuelve la Propuesta General 1+2 (consenso).
    Entrada: análisis completos de Alfred y Reginald (texto markdown).
    Salida: {"analysis": str, "stats_by_fixture": {fixture_id: {...}}, "gemini_opinion": str, "grok_opinion": str, "grok_final": str}
    """
    result = {
        "analysis": "",
        "stats_by_fixture": {},
        "gemini_opinion": "",
        "grok_opinion": "",
        "grok_final": "",
    }
    grok_analysis = (grok_analysis or "").strip()
    gemini_analysis = (gemini_analysis or "").strip()
    match_data = list(match_data) if match_data is not None else []
    if not grok_analysis or not gemini_analysis:
        result["analysis"] = (
            "Faltan los análisis de Alfred y/o Reginald. "
            "Ejecuta primero «Dale con todo!» para generar Opción 1 y Opción 2, o genera cada una por separado."
        )
        return result

    # Paso 1: Reginald opina sobre el análisis de Alfred (respuesta concisa para ahorrar tokens)
    prompt1 = (
        "Análisis de un colega (Alfred) sobre partidos de fútbol:\n\n---\n\n"
        f"{grok_analysis}\n\n---\n\n"
        "Responde en español: desacuerdos y puntos clave por partido en 1-2 frases cada uno; respuesta concisa."
    )
    try:
        out1 = ask_gemini_custom(SYSTEM_CONSENSUS, prompt1, timeout=120)
        result["gemini_opinion"] = (out1 or "").strip() or "(Reginald no devolvió opinión)"
    except Exception as e:
        log.exception("Consenso paso 1 (Reginald opina sobre Alfred): %s", e)
        result["analysis"] = f"Error en paso 1 (Reginald): {e}"
        result["gemini_opinion"] = f"(Error: {e})"
        return result

    # Paso 2: Alfred opina sobre el análisis de Reginald (respuesta concisa)
    prompt2 = (
        "Análisis de un colega (Reginald) sobre partidos de fútbol:\n\n---\n\n"
        f"{gemini_analysis}\n\n---\n\n"
        "Responde en español: desacuerdos y puntos clave por partido en 1-2 frases cada uno; respuesta concisa."
    )
    try:
        out2 = ask_grok_custom(SYSTEM_CONSENSUS, prompt2, timeout=120)
        result["grok_opinion"] = (out2 or "").strip() or "(Alfred no devolvió opinión)"
    except Exception as e:
        log.exception("Consenso paso 2 (Alfred opina sobre Reginald): %s", e)
        result["analysis"] = f"Error en paso 2 (Alfred): {e}"
        result["grok_opinion"] = f"(Error: {e})"
        return result

    # Paso 5: Alfred consultor senior (resumen ejecutivo, no repetir todo el análisis)
    prompt5 = (
        "Tu análisis inicial:\n\n---\n\n"
        f"{grok_analysis}\n\n---\n\n"
        "Críticas de Reginald:\n\n---\n\n"
        f"{result['gemini_opinion']}\n\n---\n\n"
        "Como consultor senior: resume consenso, discrepancias críticas y recomendación final. Resumen ejecutivo; no repitas todo el análisis. Una recomendación por partido. Español."
    )
    try:
        out5 = ask_grok_custom(SYSTEM_CONSENSUS, prompt5, timeout=180)
        result["grok_final"] = (out5 or "").strip() or "(Alfred consultor no devolvió recomendación)"
    except Exception as e:
        log.exception("Consenso paso 5 (Alfred consultor senior): %s", e)
        result["analysis"] = f"Error en paso 5 (Alfred): {e}"
        result["grok_final"] = f"(Error: {e})"
        return result

    # Paso 6: Reginald analiza TODAS las opciones expuestas y produce la opinión final (Propuesta General 1+2)
    gemini_opinion = result.get("gemini_opinion") or ""
    grok_opinion = result.get("grok_opinion") or ""
    grok_final = result.get("grok_final") or ""
    # Lista de fixture_id en orden para que el JSON use números, no nombres de equipos
    ordered_ids = [m.get("fixture_id") for m in match_data if m.get("fixture_id") is not None]
    ids_line = f"En el JSON, usa para cada partido (en el orden anterior) este fixture_id numérico: {ordered_ids}. No uses nombres de equipos como fixture_id.\n\n" if ordered_ids else ""
    prompt6 = (
        "Tienes ante ti TODAS las opciones y opiniones del flujo. Tu tarea es analizarlas y producir la opinión final (Propuesta General 1+2). "
        "Los resultados de la tabla (probabilidades, value_bet, etc.) deben ser TU análisis de consenso final, integrando las estadísticas mostradas por Alfred y por ti (Reginald).\n\n"
        + ids_line
        + "--- Opción 1 (análisis de Alfred) ---\n\n"
        f"{grok_analysis}\n\n"
        "--- Fin Opción 1 ---\n\n"
        "--- Opción 2 (tu análisis inicial) ---\n\n"
        f"{gemini_analysis}\n\n"
        "--- Fin Opción 2 ---\n\n"
        "--- Opinión de Reginald sobre la Opción 1 ---\n\n"
        f"{gemini_opinion}\n\n"
        "--- Fin ---\n\n"
        "--- Opinión de Alfred sobre la Opción 2 ---\n\n"
        f"{grok_opinion}\n\n"
        "--- Fin ---\n\n"
        "--- Recomendación final de Alfred (consultor senior, tras considerar tu crítica) ---\n\n"
        f"{grok_final}\n\n"
        "--- Fin ---\n\n"
        "Analiza todas las opciones anteriormente expuestas e integra lo mejor de cada una. "
        "Tú eres quien emite la opinión final: define la Propuesta General 1+2 para el usuario (consenso). "
        "Da una conclusión clara por partido (value_bet en el JSON) y un análisis de consenso en markdown sin lenguaje de apuestas."
        + GEMINI_FINAL_FORMAT_INSTRUCTION
    )
    try:
        # Paso 6: síntesis con Gemini 2.5 Pro (mejor razonamiento, JSON preciso). Temperature baja para consenso.
        raw_final = ask_gemini_custom(
            SYSTEM_CONSENSUS,
            prompt6,
            timeout=240,
            model=GEMINI_MODEL_CONSENSUS,
            temperature=0.1,
        )
    except Exception as e:
        log.exception("Consenso paso 6 (Reginald define Propuesta General 1+2): %s", e)
        result["analysis"] = f"Error en paso 6 (Reginald): {e}"
        return result

    if not (raw_final or "").strip():
        result["analysis"] = "Reginald no devolvió la Propuesta General 1+2. Reintenta más tarde."
        return result

    parsed = _parse_grok_proposal_response(raw_final, match_data)
    result["analysis"] = (parsed.get("analysis") or raw_final).strip()
    result["stats_by_fixture"] = parsed.get("stats_by_fixture") or {}
    return result
