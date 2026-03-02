#!/usr/bin/env python3
"""
Genera las 3 fichas de prensa (PDF) para:
- PSG vs Chelsea
- Real Madrid vs Manchester City
- Newcastle vs Barcelona
Llama a Gemini 2.5 Pro con el prompt de Asistente de Prensa y genera un PDF por partido.
"""

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
except ImportError:
    pass

import requests

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
if not GEMINI_API_KEY:
    raise SystemExit("Necesitas GEMINI_API_KEY en .env")

GEMINI_BASE = "https://generativelanguage.googleapis.com/v1beta"
MODEL_NAME = "gemini-2.0-flash"


# ---------------------------------------------------------------------------
# Contextos de los 3 partidos (datos reales extraídos de la BD y del análisis
# previo del transcripto de la sesión)
# ---------------------------------------------------------------------------

MATCHES = [
    {
        "title": "Paris Saint-Germain vs Chelsea",
        "subtitle": "Champions League 2025/26 | Fase de eliminacion directa",
        "home": "Paris Saint-Germain",
        "away": "Chelsea",
        "league": "Champions League",
        "season": "2025/26",
        "context": """
PARTIDO: Paris Saint-Germain vs Chelsea
LIGA: Champions League | TEMPORADA: 2025/26
FASE: Eliminatoria directa

CONTEXTO EN LA TABLA:
- PSG: Campeon vigente de la Champions League (temporal 2024/25, final 5-0 vs Inter).
- Chelsea: Regresa a la elite continental. Su campana en CL 2025/26 incluye victorias ante Pafos y Napoli, pero derrotas ante Atalanta y Bayern Munich. Logro ganar al Barcelona.

FORMA RECIENTE — PSG (ultimos 8 partidos):
- 3 victorias, 2 empates, 3 derrotas. Media de goles: 2.45 a favor, 0.98 en contra por partido (temporada completa).
- En CL esta temporada: perdio ante Sporting CP, empato ante Newcastle y Athletic Club.
- Resultado reciente notable: victoria 3-2 vs Monaco en Champions.

FORMA RECIENTE — Chelsea (ultimos 8 partidos):
- 5 victorias, 2 empates, 1 derrota. Media: 1.86 goles a favor, 1.17 en contra.
- Victorias destacadas: ante Napoli y Barcelona (CL). Derrota ante Bayern Munich.

H2H (ultimos 5 anos, competicion europea):
- Sin enfrentamientos directos en los ultimos 5 anos. Es un duelo inedito en la era moderna de la CL.

ESTADISTICAS TACTICAS (temporada actual):
- PSG: 2.45 goles/partido. Alta capacidad ofensiva. En CL, racha de 3 partidos sin ganar.
- Chelsea: 1.86 goles/partido. Compacto defensivamente. Capaz de sorprender a grandes rivales.
""",
    },
    {
        "title": "Real Madrid vs Manchester City",
        "subtitle": "Champions League 2025/26 | Fase de eliminacion directa",
        "home": "Real Madrid",
        "away": "Manchester City",
        "league": "Champions League",
        "season": "2025/26",
        "context": """
PARTIDO: Real Madrid vs Manchester City
LIGA: Champions League | TEMPORADA: 2025/26
FASE: Eliminatoria directa

CONTEXTO EN LA TABLA:
- Real Madrid: solido en casa, 7 victorias en los ultimos 8 partidos.
- Manchester City: superado un bache de resultados. Encadena victorias en la Premier League.

H2H (ultimos 10 encuentros en Champions League):
- Real Madrid: 4 victorias | Empates: 3 | Manchester City: 3 victorias
- Total de goles en 10 partidos: 40. Promedio de 4.0 goles por partido.
- Ningun encuentro termino con menos de 2 goles.
- Resultados recientes: City 1-2 Real Madrid (Bernabeu, diciembre 2025); empates 1-1 y 3-3 en eliminatoria 2024.
- Final 2022/23: Manchester City 4-0 Real Madrid.
- Semifinal 2021/22: Real Madrid remonto para avanzar.

FORMA RECIENTE — Real Madrid:
- 7W-0D-1L en los ultimos 8. Unica derrota: Osasuna 2-1.
- Media ofensiva: 2.23 goles a favor. Media defensiva: 1.62 goles en contra.
- Promedio de tiros a puerta: 6.7/partido. Corners: 6.5/partido. Offsides: 1.4/partido.

FORMA RECIENTE — Manchester City:
- Encadena victorias en Premier tras 2 derrotas consecutivas en enero.
- Ultimo resultado CL notable: victoria ante Atalanta.
- Media: 2.10 goles a favor, 2.00 en contra. Tiros: 6.2/partido. Corners: 5.7/partido. Offsides: 1.1/partido.
""",
    },
    {
        "title": "Newcastle United vs FC Barcelona",
        "subtitle": "Champions League 2025/26 | Fase de eliminacion directa",
        "home": "Newcastle United",
        "away": "FC Barcelona",
        "league": "Champions League",
        "season": "2025/26",
        "context": """
PARTIDO: Newcastle United vs FC Barcelona
LIGA: Champions League | TEMPORADA: 2025/26
FASE: Eliminatoria directa

CONTEXTO EN LA TABLA:
- Newcastle United: retorna a las eliminatorias europeas tras decadas de ausencia. Proyecto inversor que impulsa sus ambiciones.
- FC Barcelona: uno de los equipos mas en forma de Europa. Maquina ofensiva con 3+ goles por partido de promedio en CL.

H2H:
- Unico enfrentamiento reciente: fase de liga CL 2025/26 (18 sept 2025). Newcastle 1-2 Barcelona en St. James' Park.
- Barcelona ya sabe ganar en este estadio esta misma temporada.

FORMA RECIENTE — Newcastle (ultimos 8 partidos):
- Fuerte en Europa: elimino a Qarabag con global 9-3.
- En Premier League: 4 derrotas consecutivas ante rivales Top-6.
- Media: 1.73 goles a favor, 1.32 en contra por partido. Tiros a puerta: 5.2/partido. Corners: 6.1/partido.
- Empates 1-1 ante PSG (CL, noviembre 2023 y enero 2026).

FORMA RECIENTE — Barcelona (ultimos 8 partidos):
- 7 victorias, 1 derrota (vs Girona). Alta confianza.
- Media CL: 4.62 goles por partido en sus encuentros de Champions.
- Media general: 3.08 goles a favor. Tiros a puerta: 6.9/partido.
- Victoria ante Chelsea y ante Bayern este temporada.
""",
    },
]


PRESS_PROMPT_TEMPLATE = """Eres un asistente editorial para periodistas y comentaristas deportivos de futbol.
Tu tarea es redactar una FICHA DE PRENSA completa para el siguiente partido.

El tono debe ser periodistico, preciso y con narrativa fluida.
No inventes datos. Solo usa la informacion que te entrego.
Si un dato no esta disponible, omitelo sin mencionarlo.

---
{context}
---

Con toda la informacion anterior, redacta una FICHA DE PRENSA estructurada exactamente asi:

## 1. PRESENTACION DEL PARTIDO
Contextualiza el duelo: que se juega cada equipo, importancia del momento en la temporada,
rivalidad historica si la hay. Hazlo atractivo para abrir una transmision o un articulo.
Minimo 3 parrafos completos.

## 2. EL DATO QUE DEFINE LA HISTORIA
Un solo parrafo destacado con el dato historico mas llamativo o curioso que encuentres
en los datos. Debe ser concreto, con numeros, y comenzar con una frase de impacto.

## 3. CARA A CARA - Los numeros que importan
Presenta los datos H2H mas relevantes en formato periodistico fluido (no como lista tecnica).
Destaca tendencias, rachas, records entre ambos equipos.

## 4. EL MOMENTO DE AMBOS EQUIPOS
Un parrafo por equipo explicando su forma reciente: racha, goles, confianza o crisis actual.

## 5. LA DIMENSION TACTICA
Parrafo sobre el perfil de juego de cada equipo basado en los datos de la temporada:
estilo de juego, fortalezas, vulnerabilidades.

## 6. TRES PREGUNTAS PARA EL PARTIDO
Las tres preguntas narrativas que este partido deja abiertas antes del pitido inicial.
Deben surgir naturalmente de los datos (no inventadas). Formato de pregunta directa,
una linea cada una.

## 7. FRASE PARA EL ARRANQUE
Una sola frase, lista para usar al inicio de una transmision o como titular.
Maximo 20 palabras. Directa, con impacto.
"""


def call_gemini(prompt: str, match_title: str) -> str:
    print(f"  Llamando a Gemini para: {match_title}...", end=" ", flush=True)
    url = f"{GEMINI_BASE}/models/{MODEL_NAME}:generateContent"
    payload = {
        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.7, "maxOutputTokens": 4096},
    }
    headers = {"x-goog-api-key": GEMINI_API_KEY, "Content-Type": "application/json"}
    try:
        resp = requests.post(url, json=payload, headers=headers, timeout=120)
        resp.raise_for_status()
        data = resp.json()
        text = data["candidates"][0]["content"]["parts"][0]["text"]
        print(f"OK ({len(text)} chars)")
        return text
    except Exception as e:
        print(f"ERROR: {e}")
        return f"Error al llamar a Gemini: {e}"


def main():
    from pdf_report import generate_journalist_pdf

    output_dir = ROOT / "generated_pdfs"
    output_dir.mkdir(exist_ok=True)

    pdfs = []
    for m in MATCHES:
        print(f"\n[{m['title']}]")
        prompt = PRESS_PROMPT_TEMPLATE.format(context=m["context"].strip())
        analysis = call_gemini(prompt, m["title"])

        out_path = str(output_dir / f"prensa_{m['home'].lower().replace(' ', '_')[:15]}_vs_{m['away'].lower().replace(' ', '_')[:15]}.pdf")
        path = generate_journalist_pdf(
            match_title=m["title"],
            subtitle=m["subtitle"],
            analysis_text=analysis,
            output_path=out_path,
        )
        pdfs.append(path)
        print(f"  PDF generado: {path}")

    print("\n✓ Los 3 PDFs de prensa estan en:")
    for p in pdfs:
        print(f"  {p}")


if __name__ == "__main__":
    main()
