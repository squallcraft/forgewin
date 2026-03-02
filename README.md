# ForgeWin – Estadísticas y análisis de partidos

Sistema en Python para barrido diario de partidos en las principales ligas, cálculo de probabilidades con modelo Poisson y análisis por consultas en lenguaje natural. **Estadísticas y análisis de partidos en segundos. Toma mejores decisiones.**

**API:** [football-data.org](https://www.football-data.org/) (v4).

## Requisitos

- Python 3.10+
- Dependencias: `requests`, `pandas`, `python-dotenv`, `scipy`, `streamlit`

## Instalación en local

### Opción rápida (un solo comando)

```bash
./run_local.sh
```

El script activa el venv (si existe), instala dependencias y arranca la app en `http://localhost:8501`. Si no tienes `.env`, copia `.env.example` y edita las API keys.

### Instalación paso a paso

1. **Entra en la carpeta del proyecto**
   ```bash
   cd <carpeta-del-proyecto>
   ```

2. **Crea un entorno virtual (recomendado)**
   ```bash
   python3 -m venv venv
   source venv/bin/activate   # En Windows: venv\Scripts\activate
   ```

3. **Instala las dependencias**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configura las API keys**
   ```bash
   cp .env.example .env
   ```
   Edita `.env` y añade tus claves:
   ```
   FOOTBALL_DATA_ORG_TOKEN=tu_api_key_football_data
   XAI_API_KEY=tu_api_key_grok          # Para Alfred (análisis IA)
   GEMINI_API_KEY=tu_api_key_gemini     # Para Reginald (consenso)
   API_FOOTBALL_KEY=tu_api_key_apisports  # Para más ligas y xG (opcional)
   ```
   Sin API keys puedes usar datos mock marcando "Usar datos mock" en la app.

5. **Arranca la app**
   ```bash
   streamlit run app.py
   ```
   Se abrirá el navegador en `http://localhost:8501`.

## Uso

### CLI

- **Barrido diario** (ligas por defecto: PL, PD, SA, BL1, FL1, DED, PPL, EL1):
  ```bash
  python main.py --mode=daily_scan
  ```
- **Ligas concretas** (códigos separados por coma):
  ```bash
  python main.py --mode=daily_scan --leagues=PL,EL1
  ```
- **Query**:
  ```bash
  python main.py --mode=query --query="análisis League One hoy"
  python main.py --mode=query --query="top 3 equipos league one >1 gol últimas 10"
  ```
- **Sin API key** (datos mock):
  ```bash
  python main.py --mode=daily_scan --mock
  python main.py --mode=query --query="análisis League One hoy" --mock
  ```

### UI Streamlit

```bash
streamlit run app.py
```

En el sidebar: elegir liga, modo (Partidos y probabilidades / Query libre / Top equipos por goles) y opcionalmente "Usar datos mock".

## Estructura

- `config.py`: Códigos de competición (PL, PD, SA, etc.), `FOOTBALL_DATA_ORG_TOKEN` desde `.env`, `BASE_URL` v4.
- `scraper.py`: `get_upcoming_matches(league_codes, days_ahead)` — próximos partidos vía `/competitions/{code}/matches`.
- `data_fetcher.py`: `get_standings`, `get_team_stats`, `get_h2h(match_id)`, `get_form_last_n`. Sin endpoint de lesiones en la API.
- `analyzer.py`: Poisson, `calculate_probabilities`, `analyze_specific_league`, `top_teams_avg_goals`, `run_query`.
- `main.py`: CLI. Logs en `output/*.json` y `output/*.csv`.
- `app.py`: Streamlit.

## Ligas configuradas (códigos football-data.org)

Premier League (PL), La Liga (PD), Serie A (SA), Bundesliga (BL1), Ligue 1 (FL1), Eredivisie (DED), Primeira Liga (PPL), Champions League (CL), League One (EL1), Championship (ELC).

## Notas

- Rate limit: plan gratuito ~10 req/min; el código aplica delay entre requests.
- Sin API key usa datos mock (`--mock` en CLI o checkbox en Streamlit).
- H2H se obtiene por `match_id` (GET `/matches/{id}/head2head`). Lesiones: la API no las expone en el plan estándar.
