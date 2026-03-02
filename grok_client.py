"""
Cliente Alfred (API xAI/Grok) para razonamiento y chatbot.
Usa API https://api.x.ai/v1 (responses o chat/completions).
Soporta visión (imágenes) en ask_grok_chat cuando se pasan image_parts.
"""

import base64
import json
import logging
import re
import time
from datetime import date
from typing import Any, Dict, List, Optional

import requests

from config import GROK_API_KEY, GROK_BASE_URL, GROK_MODEL, GROK_MODEL_FAST

log = logging.getLogger(__name__)


# Preguntas por defecto cuando el usuario selecciona un partido ("Pregúntale a Alfred")
DEFAULT_MATCH_QUESTIONS = [
    "¿Cuál es la tendencia de los últimos 5 partidos?",
    "¿Cómo rinden los equipos en condiciones similares?",
    "¿Quién falta y por qué? ¿Cuál es el valor de reemplazo del ausente?",
    "¿Hay jugadores regresando?",
    "¿Cómo interactúan los estilos de juego entre ellos?",
    "¿Quién domina las áreas críticas?",
    "¿Qué hay en juego?",
    "¿Existe el factor Cansancio vs. Rotación?",
    "¿Hay factores externos?",
    "¿Cuál es la forma real de cada equipo en los últimos 5–10 partidos?",
    "¿Cómo rinden como local vs visitante?",
    "¿Hay rachas específicas? (invictos en casa, no anotan en los primeros 30', conceden muchos goles en la segunda parte)",
    "¿Qué ha pasado en los últimos 5–8 enfrentamientos entre estos dos equipos? (patrón de dominio, goles, local/visitante)",
    "¿Qué árbitro pita el partido? (tarjetero, penales, expulsiones). ¿Cuántos partidos ha ganado cada equipo (local y visitante) con ese árbitro? Indica victorias con ese árbitro si tienes el dato; si no, dilo.",
    "¿Cuántos partidos ha ganado cada equipo sin su mejor jugador en comparación con los partidos en los que sí ha estado? Identifica al jugador clave de cada equipo y contrasta resultados (victorias/con resultados sin él vs con él). Si no tienes datos exactos, da una valoración cualitativa.",
    "¿Cuál es el xG y xGA reciente de cada equipo? (sobre-rendiendo o sub-rendiendo)",
]


def _call_grok(
    messages: List[Dict[str, str]],
    model: Optional[str] = None,
    timeout: int = 120,
) -> str:
    """
    Llama a la API xAI. Prueba /chat/completions (OpenAI-compatible) y si falla /responses.
    messages = [{"role": "user"|"system", "content": "..."}].
    Exponential backoff ante 429 y errores de conexión.
    """
    if not GROK_API_KEY:
        return "No está configurada la API key de Alfred. Añade XAI_API_KEY o GROK_API_KEY en el archivo .env"
    model = model or GROK_MODEL
    headers = {
        "Authorization": f"Bearer {GROK_API_KEY}",
        "Content-Type": "application/json",
    }
    url_chat = f"{GROK_BASE_URL.rstrip('/')}/chat/completions"
    url_resp = f"{GROK_BASE_URL.rstrip('/')}/responses"
    max_retries = 6
    last_error = None
    for attempt in range(max_retries):
        try:
            payload_chat = {"model": model, "messages": messages}
            r = requests.post(url_chat, headers=headers, json=payload_chat, timeout=timeout)
            if r.status_code == 200:
                data = r.json()
                choice = (data.get("choices") or [None])[0]
                if choice:
                    msg = (choice.get("message") or {}).get("content") or ""
                    return msg.strip()
            if r.status_code == 403:
                try:
                    if "credits" in r.text.lower() or "licenses" in r.text.lower():
                        return (
                            "Tu cuenta de xAI (Alfred) no tiene créditos asignados. "
                            "Entra en https://console.x.ai/ y en tu equipo (Team) compra créditos o activa una licencia "
                            "para poder usar la API. Luego vuelve a intentar."
                        )
                except Exception:
                    pass
                return f"Alfred rechazó la petición (403). Revisa que tu equipo tenga créditos en https://console.x.ai/"
            if r.status_code == 429:
                wait = min(2 ** attempt, 120)
                try:
                    header_wait = int(r.headers.get("Retry-After", 0))
                    if header_wait > 0:
                        wait = max(wait, min(header_wait, 120))
                except (ValueError, TypeError):
                    pass
                if attempt < max_retries - 1:
                    log.warning("Alfred 429. Reintento en %s s (intento %s/%s).", wait, attempt + 1, max_retries)
                    time.sleep(wait)
                    continue
                return (
                    "Error 429 (Alfred): Demasiadas peticiones. Espera 1-2 minutos y vuelve a intentar."
                )
            # Fallback: API Responses (xAI nativa) cuando chat/completions no es 200
            input_list = [{"role": m["role"], "content": m["content"]} for m in messages]
            payload_resp = {"model": model, "input": input_list}
            r2 = requests.post(url_resp, headers=headers, json=payload_resp, timeout=timeout)
            if r2.status_code != 200:
                last_error = f"Error Alfred ({r2.status_code}): {r2.text[:500]}"
                if r2.status_code == 429 and attempt < max_retries - 1:
                    wait_r2 = min(2 ** attempt, 120)
                    time.sleep(wait_r2)
                    continue
                return last_error
            data = r2.json()
            out = data.get("output") or []
            if out and isinstance(out[0], dict) and out[0].get("content"):
                return (out[0]["content"] or "").strip()
            return "Respuesta vacía de Alfred."
        except requests.RequestException as e:
            last_error = f"Error de conexión con Alfred: {e}"
            if attempt < max_retries - 1:
                wait = min(2 ** attempt, 120)
                log.warning("Alfred error. Reintento en %s s (intento %s/%s).", wait, attempt + 1, max_retries)
                time.sleep(wait)
                continue
    return last_error or "Error desconocido de Alfred."


def build_match_context(match: Dict[str, Any], extra: Optional[Dict[str, Any]] = None) -> str:
    """
    Construye un texto de contexto para Alfred sobre el partido.
    match: dict con home_team, away_team, date, league_name, fixture_id, etc.
    extra: opcional, stats adicionales (form_home, form_away, h2h, probs, odds).
    """
    home = match.get("home_team") or match.get("home") or "Local"
    away = match.get("away_team") or match.get("away") or "Visitante"
    league = match.get("league_name") or match.get("league") or ""
    date = match.get("date") or ""
    ctx = f"Partido: {home} vs {away}. Liga: {league}. Fecha: {date}.\n"
    if extra:
        if extra.get("form_home"):
            ctx += f"\nForma reciente local ({home}): {extra['form_home']}\n"
        if extra.get("form_away"):
            ctx += f"Forma reciente visitante ({away}): {extra['form_away']}\n"
        if extra.get("h2h"):
            ctx += f"Últimos H2H: {extra['h2h']}\n"
        if extra.get("probs"):
            ctx += f"Probabilidades (Poisson + cuotas de mercado): {extra['probs']}\n"
        if extra.get("odds"):
            o = extra["odds"]
            ctx += f"Cuotas de mercado (1-X-2): {o.get('home_win')} / {o.get('draw')} / {o.get('away_win')}\n"
    return ctx


def ask_grok_default_for_match(match: Dict[str, Any], extra: Optional[Dict[str, Any]] = None) -> str:
    """
    Envía a Alfred el contexto del partido y las preguntas por defecto.
    Devuelve la respuesta en texto.
    """
    context = build_match_context(match, extra)
    all_questions = "\n".join(f"- {q}" for q in DEFAULT_MATCH_QUESTIONS)
    user_content = (
        "Contexto del partido (usa estos datos y tu conocimiento para razonar):\n\n"
        + context
        + "\n\nResponde de forma clara y concisa a estas preguntas (puedes agrupar por temas). "
        "Incluye en tu respuesta: (1) cuántos partidos ha ganado cada equipo con el árbitro del partido, si conoces al árbitro; "
        "(2) cuántos partidos ha ganado cada equipo sin su mejor jugador frente a con su mejor jugador (identifica al jugador y da el contraste). "
        "Si no tienes un dato concreto, indícalo y da una valoración cualitativa.\n\n"
        + all_questions
    )
    messages = [
        {
            "role": "system",
            "content": (
                "Eres un analista experto en fútbol. Respondes en español. "
                "Debes calcular todas las estadísticas (probabilidades 1-X-2, goles esperados, BTTS, Over/Under, clean sheet) "
                "combinando fuentes históricas (forma, H2H, datos de la liga) y cuotas de mercado cuando se proporcionen. "
                "Usa el contexto dado (probs ya mezcladas Poisson + casas, cuotas, forma, H2H) y tu razonamiento para ofrecer análisis y cifras. "
                "Incluye siempre que puedas: victorias de cada equipo con ese árbitro, y rendimiento sin vs con su mejor jugador. "
                "Si no tienes un dato concreto, indícalo y da una valoración cualitativa. "
                "No uses lenguaje de apuestas (apuesta segura, recomendar apostar, etc.); expresa con probabilidad: «es muy probable que...», «es probable que...», «podría ocurrir que...»."
            ),
        },
        {"role": "user", "content": user_content},
    ]
    return _call_grok(messages)


# Instrucción de estilo: sin lenguaje de apuestas; expresar con probabilidad.
NO_APUESTAS_INSTRUCTION = "No uses lenguaje de apuestas (apuesta segura, recomendar apostar, cuota, etc.). Expresa expectativas con probabilidad: «es muy probable que...», «es probable que...», «podría ocurrir que...», «hay mayor probabilidad de que...». No sugieras apostar ni menciones apuestas en el texto."

# Prompt fijo para propuestas: análisis por partido + recomendación (mismo texto salvo que se actualice el sistema).
PROPOSAL_SYSTEM_PROMPT = """Eres un analista experto en fútbol. Respondes en español.
Analiza los partidos con el contexto que se te da. En el JSON de stats (prob_home_win, prob_draw, expected_goals, value_bet, etc.) da TU PROPIA estimación; no copies los números del contexto. Analiza TODOS los partidos enviados (cualquier fecha). Verifica plantillas/trasferencias antes de citar jugadores; si no puedes verificarlo, no afirmes el equipo o indícalo.
En cada recomendación por partido menciona solo los equipos de ESE partido (local y visitante); nunca uses el nombre de un equipo que pertenezca a otro partido de la lista (evita confusiones como citar "Forest" en un partido que no sea el de Nottingham Forest).
""" + NO_APUESTAS_INSTRUCTION

PROPOSAL_USER_INSTRUCTIONS = """
Usa el contexto proporcionado (datos de BD/APIs: partidos, probs 1-X-2, xG, clean sheet, BTTS, Over 2.5, y cuando estén disponibles: tarjetas amarillas/partido, disparos al arco/partido, offsides/partido). Incluye por partido: forma reciente (5 partidos), H2H (3), probs, xG, clean sheet, BTTS, Over 2.5; cuando se proporcionen: tarjetas (agresividad, ritmo del juego), disparos al arco (presión ofensiva) y offsides (línea defensiva alta/baja); si aplica: efecto Europa/FIFA, contexto de mercado (Biscotto), árbitro; si no hay dato, indícalo. En H2H cada resultado viene como "EquipoLocal goles-goles EquipoVisitante" (ej. "Qarabag 1-6 Newcastle" = Qarabag 1, Newcastle 6; ganó Newcastle). Interpreta siempre correctamente quién ganó según los goles. Responde en markdown por partido (nombres de equipos, sin IDs). Incorpora: Efecto Europa (jueves Europa + domingo liga), Fecha FIFA, contexto situacional, árbitro.

Formato obligatorio:
1) Línea exacta: GROK_STATS_JSON
2) Línea siguiente: JSON válido, array de objetos UNO POR PARTIDO en el mismo orden. Cada objeto: "fixture_id" (número), "prob_home_win", "prob_draw", "prob_away_win", "expected_goals", "clean_sheet_home", "clean_sheet_away", "prob_btts", "prob_over25" (0-1), "value_bet" ("1", "X" o "2"). Valores = TU estimación, no copies el contexto.
3) Línea exacta: END_GROK_STATS
4) Después: tu análisis en markdown (datos y conclusión por partido). En el texto no uses lenguaje de apuestas; usa «es muy probable que...», «es probable que...», «podría ocurrir que...».
"""

# V3: análisis enriquecido (Índice de Asedio, Alerta de Fricción, Dinámica HT/FT)
PROPOSAL_SYSTEM_PROMPT_V3 = """Eres un analista experto en fútbol. Respondes en español.
Analiza los partidos con el contexto que se te da.

Solo cuando en el mensaje te hayamos proporcionado un bloque titulado "Datos de nuestra tabla histórica para Índice de Asedio, Alerta de Fricción y Dinámica HT/FT", incluye en tu análisis las secciones para las que SÍ aparezcan datos en ese bloque: (1) Índice de Asedio, (2) Alerta de Fricción, (3) Dinámica HT/FT. Usa exclusivamente los datos que te pasamos y tu razonamiento; no inventes cifras ni indiques "no hay datos" ni "0.0" — si para alguna de esas tres no hay datos en el bloque, omite esa subsección. Si NO te hemos enviado ese bloque de datos enriquecidos, no incluyas ninguna de esas secciones en tu análisis.

En el JSON de stats (prob_home_win, prob_draw, expected_goals, value_bet, etc.) da TU PROPIA estimación; no copies los números del contexto.
Analiza TODOS los partidos enviados. En cada conclusión por partido menciona solo los equipos de ESE partido (local y visitante).
""" + NO_APUESTAS_INSTRUCTION

PROPOSAL_USER_INSTRUCTIONS_V3 = """
Usa el contexto proporcionado: datos de partidos, probs 1-X-2, xG, clean sheet, BTTS, Over 2.5, tarjetas/disparos al arco/offsides (cuando estén disponibles) y, si lo hemos enviado, el bloque de métricas enriquecidas (Asedio, Fricción, HT/FT). Cuando estén disponibles las métricas tácticas (tarjetas, disparos al arco, offsides), úsalas para enriquecer el análisis: las tarjetas reflejan agresividad y el posible impacto de árbitros; los disparos al arco miden la efectividad ofensiva; los offsides indican si el equipo usa línea defensiva alta.
En H2H cada resultado viene como "EquipoLocal goles-goles EquipoVisitante" (ej. "Bodo/Glimt 3-1 Inter" = ganó Bodo; "Qarabag 1-6 Newcastle" = ganó Newcastle). Interpreta siempre correctamente quién ganó según los goles.
Solo incluye en tu análisis las secciones de Índice de Asedio, Alerta de Fricción y Dinámica HT/FT cuando te hayamos enviado arriba el bloque "Datos de nuestra tabla histórica para Índice de Asedio..."; si ese bloque no está en el mensaje, no añadas esas secciones.
Por partido incluye: forma reciente, H2H, probs, xG; y si hay datos enriquecidos, análisis de Asedio, Fricción y HT/FT. Responde en markdown por partido (nombres de equipos, sin IDs). No uses lenguaje de apuestas; expresa con probabilidad: «es muy probable que...», «es probable que...», «podría ocurrir que...».

Formato obligatorio:
1) Línea exacta: GROK_STATS_JSON
2) Línea siguiente: JSON válido, array de objetos UNO POR PARTIDO en el mismo orden. Cada objeto: "fixture_id" (número), "prob_home_win", "prob_draw", "prob_away_win", "expected_goals", "clean_sheet_home", "clean_sheet_away", "prob_btts", "prob_over25" (0-1), "value_bet" ("1", "X" o "2"). Valores = TU estimación.
3) Línea exacta: END_GROK_STATS
4) Después: tu análisis en markdown (incluye Asedio/Fricción/HT/FT solo si te dimos ese bloque de datos; sin lenguaje de apuestas).
"""


def _parse_grok_proposal_response(raw: str, match_data: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Extrae de la respuesta de Alfred/Reginald el bloque JSON de stats y el markdown de análisis.
    Devuelve {"analysis": str, "stats_by_fixture": {fixture_id: {prob_home_win, ...}}}.
    Acepta formato con markers GROK_STATS_JSON/END_GROK_STATS o JSON en bloques ```json.
    Fallback: si no hay stats, usa probs de match_data para mostrar tabla de referencia.
    """
    raw = raw or ""
    stats_by_fixture: Dict[int, Dict[str, Any]] = {}
    analysis = raw

    valid_fids = set()
    for m in match_data or []:
        try:
            fid = m.get("fixture_id")
            if fid is not None:
                valid_fids.add(int(float(fid)) if not isinstance(fid, int) else int(fid))
        except (TypeError, ValueError):
            pass

    def _try_parse_array(data, raw_text: str) -> bool:
        """Parsea array de stats; devuelve True si se extrajeron stats. Solo acepta fixture_id presentes en match_data."""
        if not isinstance(data, list):
            return False
        for i, item in enumerate(data):
            if not isinstance(item, dict):
                continue
            # Necesitamos al menos prob_home_win o fixture_id para considerar el objeto válido
            if "prob_home_win" not in item and "fixture_id" not in item:
                continue
            fid = item.get("fixture_id")
            if fid is None or not (
                isinstance(fid, (int, float))
                or (isinstance(fid, str) and str(fid).strip().isdigit())
            ):
                fid = match_data[i].get("fixture_id") if i < len(match_data) else None
            if fid is not None:
                try:
                    key = int(float(fid)) if not isinstance(fid, int) else int(fid)
                except (TypeError, ValueError):
                    raw_key = match_data[i].get("fixture_id") if i < len(match_data) else None
                    try:
                        key = int(float(raw_key)) if raw_key is not None else None
                    except (TypeError, ValueError):
                        key = None
                if key is not None and (not valid_fids or key in valid_fids):
                    stats_by_fixture[key] = {
                        "prob_home_win": item.get("prob_home_win"),
                        "prob_draw": item.get("prob_draw"),
                        "prob_away_win": item.get("prob_away_win"),
                        "expected_goals": item.get("expected_goals"),
                        "clean_sheet_home": item.get("clean_sheet_home"),
                        "clean_sheet_away": item.get("clean_sheet_away"),
                        "prob_btts": item.get("prob_btts"),
                        "prob_over25": item.get("prob_over25"),
                        "value_bet": item.get("value_bet"),
                    }
        return len(stats_by_fixture) > 0

    # 1) Formato canónico: GROK_STATS_JSON ... END_GROK_STATS
    start_marker = "GROK_STATS_JSON"
    end_marker = "END_GROK_STATS"
    if start_marker in raw and end_marker in raw:
        try:
            start_i = raw.index(start_marker) + len(start_marker)
            end_i = raw.index(end_marker)
            json_block = raw[start_i:end_i].strip()
            if "```" in json_block:
                m = re.search(r"```(?:json)?\s*([\s\S]*?)```", json_block)
                if m:
                    json_block = m.group(1).strip()
            data = json.loads(json_block)
            if _try_parse_array(data, raw):
                analysis = raw[end_i + len(end_marker) :].strip()
                return {"analysis": analysis, "stats_by_fixture": stats_by_fixture}
        except (json.JSONDecodeError, ValueError, KeyError):
            pass
        stats_by_fixture.clear()

    # 2) Fallback: buscar bloque ```json con array de objetos (Gemini a veces omite los markers)
    for m in re.finditer(r"```(?:json)?\s*([\s\S]*?)```", raw):
        stats_by_fixture.clear()
        block = m.group(1).strip()
        try:
            data = json.loads(block)
            if _try_parse_array(data, raw):
                # Quitar el bloque JSON del análisis para no duplicar
                analysis = raw.replace(m.group(0), "").strip()
                return {"analysis": analysis, "stats_by_fixture": stats_by_fixture}
        except json.JSONDecodeError:
            continue

    # 3) Fallback final: usar datos de match_data como tabla de referencia
    if match_data and not stats_by_fixture:
        for m in match_data:
            fid = m.get("fixture_id")
            if fid is not None:
                try:
                    key = int(float(fid)) if not isinstance(fid, int) else int(fid)
                except (TypeError, ValueError):
                    continue
                stats_by_fixture[key] = {
                    "prob_home_win": m.get("prob_home_win"),
                    "prob_draw": m.get("prob_draw"),
                    "prob_away_win": m.get("prob_away_win"),
                    "expected_goals": m.get("expected_goals"),
                    "clean_sheet_home": m.get("clean_sheet_home"),
                    "clean_sheet_away": m.get("clean_sheet_away"),
                    "prob_btts": m.get("prob_btts"),
                    "prob_over25": m.get("prob_over25"),
                    "value_bet": None,  # Referencia, sin value bet de modelo
                }

    return {"analysis": analysis, "stats_by_fixture": stats_by_fixture}


def _build_match_data_context(match_data: List[Dict[str, Any]]) -> str:
    """Construye el bloque de datos de partidos (BD/casas) para el prompt. Sin códigos de partido de cara al usuario."""
    lines = []
    fixture_ids = []
    for m in match_data:
        fid = m.get("fixture_id")
        if fid is not None:
            fixture_ids.append(fid)
        home = m.get("home_team") or m.get("home") or "Local"
        away = m.get("away_team") or m.get("away") or "Visitante"
        league = m.get("league_name") or m.get("league") or ""
        date_ = m.get("date") or ""
        p1 = m.get("prob_home_win")
        px = m.get("prob_draw")
        p2 = m.get("prob_away_win")
        xg = m.get("expected_goals")
        cs_h = m.get("clean_sheet_home")
        cs_a = m.get("clean_sheet_away")
        btts = m.get("prob_btts")
        over25 = m.get("prob_over25")
        parts = [f"{home} vs {away} — {league}, {date_}"]
        if p1 is not None and px is not None and p2 is not None:
            parts.append(f"Victoria local: {p1:.0%}, Empate: {px:.0%}, Victoria visitante: {p2:.0%}")
        if xg is not None:
            parts.append(f"Goles esperados: {xg:.1f}")
        if cs_h is not None and cs_a is not None:
            parts.append(f"Clean sheet local: {cs_h:.0%}, visitante: {cs_a:.0%}")
        if btts is not None:
            parts.append(f"BTTS: {btts:.0%}, Over 2.5: {over25:.0%}" if over25 is not None else f"BTTS: {btts:.0%}")
        # Métricas tácticas (cuando están disponibles en la BD)
        cards_h = m.get("avg_yellow_cards_home")
        cards_a = m.get("avg_yellow_cards_away")
        shots_h = m.get("avg_shots_on_target_home")
        shots_a = m.get("avg_shots_on_target_away")
        offside_h = m.get("avg_offsides_home")
        offside_a = m.get("avg_offsides_away")
        if cards_h is not None and cards_a is not None:
            parts.append(f"Tarjetas amarillas/ptdo: local {cards_h:.1f}, visitante {cards_a:.1f}")
        if shots_h is not None and shots_a is not None:
            parts.append(f"Disparos al arco/ptdo: local {shots_h:.1f}, visitante {shots_a:.1f}")
        if offside_h is not None and offside_a is not None:
            parts.append(f"Offsides/ptdo: local {offside_h:.1f}, visitante {offside_a:.1f}")
        lines.append(" | ".join(parts))
    out = "\n".join(lines)
    if fixture_ids:
        out += "\n\nPara el JSON de respuesta, asigna a cada partido (en el orden anterior) este fixture_id: " + str(fixture_ids)
    return out


def ask_grok_proposal_analysis(
    match_data: List[Dict[str, Any]],
    db_context: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Pide a Alfred un análisis completo de los partidos seleccionados según el sistema de propuestas.
    Usa siempre las mismes instrucciones (datos a–o + efecto Europa, FIFA, Biscotto, árbitro).
    Devuelve el análisis en texto (markdown).
    match_data: lista de dicts con datos de cada partido (de la BD/API).
    db_context: opcional, texto adicional de la BD (forma, H2H, etc.) para inyectar.
    Devuelve dict con "analysis" (markdown) y "stats_by_fixture" (probabilidades por fixture_id).
    """
    if not GROK_API_KEY:
        return {"analysis": "No está configurada la API key de Alfred. Añade XAI_API_KEY o GROK_API_KEY en el archivo .env.", "stats_by_fixture": {}}
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
    messages = [
        {"role": "system", "content": PROPOSAL_SYSTEM_PROMPT},
        {"role": "user", "content": user_content},
    ]
    raw = _call_grok(messages, model=GROK_MODEL_FAST, timeout=180)
    return _parse_grok_proposal_response(raw or "", match_data)


def ask_grok_proposal_analysis_v3(
    match_data: List[Dict[str, Any]],
    db_context: Optional[str] = None,
    enriched_context: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Análisis V3 con contexto enriquecido (Índice de Asedio, Alerta de Fricción, Dinámica HT/FT).
    Usa PROPOSAL_SYSTEM_PROMPT_V3 y PROPOSAL_USER_INSTRUCTIONS_V3.
    """
    if not GROK_API_KEY:
        return {"analysis": "No está configurada la API key de Alfred. Añade XAI_API_KEY o GROK_API_KEY en el archivo .env.", "stats_by_fixture": {}}
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
    messages = [
        {"role": "system", "content": PROPOSAL_SYSTEM_PROMPT_V3},
        {"role": "user", "content": user_content},
    ]
    raw = _call_grok(messages, model=GROK_MODEL_FAST, timeout=180)
    return _parse_grok_proposal_response(raw or "", match_data)


def ask_grok_custom(system_prompt: str, user_content: str, timeout: int = 180) -> str:
    """Llama a Alfred con un system y un mensaje de usuario (consenso, etc.). Usa modelo económico."""
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_content},
    ]
    return _call_grok(messages, model=GROK_MODEL_FAST, timeout=timeout)


def ask_grok_chat(
    user_message: str,
    conversation_history: Optional[List[Dict[str, Any]]] = None,
    match_context: Optional[str] = None,
    image_parts: Optional[List[Dict[str, Any]]] = None,
) -> str:
    """
    Chat libre con Alfred. conversation_history = [{"role":"user","content":"..."}, {"role":"assistant","content":"..."}].
    Si match_context está definido, se usa como contexto del sistema.
    image_parts: lista de {"bytes": b..., "mime": "image/jpeg"} o {"bytes": b...} (mime por defecto image/jpeg).
    Si hay image_parts, el último mensaje user tiene content multimodal (imágenes + texto) para visión.
    """
    system = "Eres un asistente experto en análisis de fútbol. Respondes en español."
    if match_context:
        system += f"\n\nContexto del partido actual:\n{match_context}"
    messages: List[Dict[str, Any]] = [{"role": "system", "content": system}]
    if conversation_history:
        for m in conversation_history[-20:]:
            messages.append({"role": m["role"], "content": m.get("content", "")})
    if image_parts:
        content_parts: List[Dict[str, Any]] = []
        for part in image_parts:
            b = part.get("bytes")
            if not b:
                continue
            mime = part.get("mime") or "image/jpeg"
            b64 = base64.standard_b64encode(b).decode("ascii")
            content_parts.append({
                "type": "image_url",
                "image_url": {"url": f"data:{mime};base64,{b64}", "detail": "high"},
            })
        if user_message and user_message.strip():
            content_parts.append({"type": "text", "text": user_message.strip()})
        if not content_parts:
            content_parts.append({"type": "text", "text": "¿Qué ves en esta imagen? (verificación de datos)"})
        messages.append({"role": "user", "content": content_parts})
    else:
        messages.append({"role": "user", "content": user_message or ""})
    return _call_grok(messages)
