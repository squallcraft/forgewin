# Prompt que enviamos a Reginald (análisis de propuestas)

Se envía en **dos partes** a la API de Reginald (`generateContent`):

- **system_instruction**: el texto del bloque "System prompt" siguiente.
- **contents[0].parts[0].text**: el texto del bloque "User content" siguiente.

---

## System prompt (instrucción de sistema)

```
Eres un analista experto en fútbol. Respondes en español.
Tu tarea es analizar los partidos seleccionados basándote exclusivamente en el contexto de texto que se te proporciona a continuación y en tu conocimiento interno.
Debes presentar TODOS los datos solicitados por partido y, a continuación, tu análisis y recomendación.

Analiza TODOS los partidos que te envían, tanto si son de hoy como de mañana o de otra fecha; no omitas ni rechaces partidos por la fecha del partido.

IMPORTANTE - Fecha y plantillas: La fecha actual se te indicará en cada petición. Verifica siempre la información de plantillas y trasferencias antes de mencionar jugadores: comprueba que cada jugador que cites pertenece al equipo correcto en la temporada actual. Si no puedes verificarlo, no afirmes el equipo del jugador o indica que se contrasten con fuentes actualizadas.
```

---

## User content (contenido de usuario)

Se construye así (en código):

1. **Encabezado** (fijo):
```
Fecha actual (día de esta petición): YYYY-MM-DD. Usa esta fecha para contexto de temporada y trasferencias.

Los partidos listados pueden ser de hoy, de mañana o de otra fecha; debes analizar TODOS los partidos que se te envían, sin excluir ninguno por la fecha.

Datos de los partidos desde las bases de datos y APIs conectadas a esta app:

```

2. **Bloque de partidos** (uno por línea), generado por `_build_match_data_context`. Cada línea tiene la forma:
```
Partido {fixture_id}: {Local} vs {Visitante} — {Liga}, {fecha} | Victoria local: X%, Empate: Y%, Victoria visitante: Z% | Goles esperados: N | Clean sheet local: %, visitante: % | BTTS: %, Over 2.5: %
```
Ejemplo real:
```
Partido 12345: Club Brugge KV vs Club Atlético de Madrid — UEFA Europa League, 2026-02-18 17:00:00 | Victoria local: 12%, Empate: 76%, Victoria visitante: 12% | Goles esperados: 0.3 | Clean sheet local: 86%, visitante: 86% | BTTS: 2%, Over 2.5: 0%
Partido 67890: PAE Olympiakos SFP vs Bayer 04 Leverkusen — UEFA Europa League, 2026-02-18 17:00:00 | ...
```

3. **Instrucciones de análisis** (fijas, igual que abajo).

---

## Instrucciones de análisis (PROPOSAL_USER_INSTRUCTIONS)

Este bloque se concatena al final del user content:

```
Haz un análisis de los partidos seleccionados.
Quiero los siguientes resultados por partido en tu análisis:

Para hacer tu análisis usa exclusivamente el contexto de texto que se te proporciona (datos de partidos y métricas) y tu conocimiento interno para el razonamiento y los análisis.

Para tu análisis toma en cuenta por partido:

a) Los 5 últimos partidos de los equipos a analizar.
b) Los últimos 3 enfrentamientos entre ellos en cualquier competición.
c) Probabilidad de victoria por equipo.
d) Probabilidad de empate.
e) Probabilidad de 0.5 goles.
f) Probabilidad de 1.5 goles.
g) Probabilidad de 2.5 goles.
h) Probabilidad de partido sin goles.
i) Cantidad de tarjetas amarillas y rojas.
j) Cantidad de corners.
k) Impacto del árbitro en el resultado.
l) Probabilidad de que ambos equipos anoten (BTTS).
m) Chances claras de gol por partido (últimos 5 partidos).
n) Chances claras de gol concedidas (últimos 5 partidos).
o) Goleador del equipo, goles y disponibilidad.

Presenta todos esos datos por partido. Luego, usando esos datos, haz un análisis del partido como recomendación de lo que podría suceder.

Para esa recomendación incorpora además estos análisis:

- **Efecto Europa**: Si un equipo jugó Champions o Europa League el jueves (con viaje largo incluido) y juega liga el domingo al mediodía, su rendimiento físico suele caer un 15-20% en la segunda mitad.
- **Fecha FIFA**: Los equipos con muchos internacionales sufren el "virus FIFA": jugadores que llegan cansados, con jet lag o pequeñas molestias de sus selecciones.
- **Contexto situacional y de mercado**: ¿A quién le sirve el empate? En las últimas jornadas, si a ambos equipos les sirve un punto para salvarse o clasificar, el ritmo del partido suele bajar drásticamente (el "Biscotto").
- **Árbitro**: ¿Cómo es el historial del árbitro con las tarjetas? Si es un derbi caliente y el árbitro es de "gatillo fácil" con las rojas, los escenarios de expulsión o penaltis cobran valor informado.

Responde en markdown, bien estructurado por partido (títulos, listas, tablas si aplica) y al final tu recomendación por partido.

IMPORTANTE - Formato de respuesta (obligatorio):
1) Primero escribe exactamente esta línea: GROK_STATS_JSON
2) En la línea siguiente, un JSON válido: un array de objetos, UNO POR PARTIDO en el mismo orden de la lista de partidos. Cada objeto debe tener:
   - "fixture_id": número del partido
   - "prob_home_win": número 0-1 (ej: 0.35)
   - "prob_draw": número 0-1
   - "prob_away_win": número 0-1
   - "expected_goals": número (ej: 2.1)
   - "clean_sheet_home": número 0-1
   - "clean_sheet_away": número 0-1
   - "prob_btts": número 0-1
   - "prob_over25": número 0-1
   - "value_bet": "1", "X" o "2" (o "1,X" si hay más de un value). Indica qué pronóstico(s) tienen valor según tu análisis.
3) Luego escribe exactamente: END_GROK_STATS
4) Después, en las líneas siguientes, tu análisis completo en markdown (todos los datos a-o y recomendaciones).

Ejemplo de las dos primeras líneas de tu respuesta:
GROK_STATS_JSON
[{"fixture_id": 123, "prob_home_win": 0.35, "prob_draw": 0.28, "prob_away_win": 0.37, "expected_goals": 2.1, "clean_sheet_home": 0.32, "clean_sheet_away": 0.35, "prob_btts": 0.55, "prob_over25": 0.58, "value_bet": "1"}]
END_GROK_STATS
```

---

## Cómo se envía a la API

- **URL**: `https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent`
- **Headers**: `x-goog-api-key: <API_KEY>`, `Content-Type: application/json`
- **Body** (JSON):
```json
{
  "system_instruction": { "parts": [ { "text": "<System prompt de arriba>" } ] },
  "contents": [ { "role": "user", "parts": [ { "text": "<User content completo de arriba>" } ] } ],
  "safetySettings": [
    { "category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_NONE" },
    { "category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_NONE" },
    { "category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_NONE" },
    { "category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_NONE" }
  ],
  "generationConfig": {
    "temperature": 0.4,
    "maxOutputTokens": 8192
  }
}
```

---

## Posibles fuentes de error 400

- **system_instruction** debe ir en snake_case (no `systemInstruction`) en el JSON.
- **contents**: un solo elemento con `"role": "user"` y `"parts": [{"text": "..."}]` (Google es estricto con el role).
- Tamaño del texto: si hay muchos partidos, el `text` puede ser muy largo (límites de la API).
- Caracteres raros en nombres de equipos o fechas que rompan el JSON (ya usamos `json=payload` en requests para escapar).
- Comillas o saltos de línea dentro del system/user que no estén bien escapados en el JSON.
