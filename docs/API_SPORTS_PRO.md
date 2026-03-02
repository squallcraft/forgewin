# API-Sports: plan Pro (configuración ForgeWin)

**ForgeWin está configurado para usar API-Sports con plan Pro (de pago).**

- **Límite Pro:** 300 peticiones/minuto (7.500/día).
- **Delay por defecto:** 0,25 s entre peticiones (`api_sports_fetcher.py`, scripts de backfill). Opcional: override con `API_FOOTBALL_REQUEST_DELAY` en `.env` o `--delay` en los scripts.
- **Fuente principal de partidos:** API-Sports para todas las ligas (PL, PD, SA, BL1, FL1, CL, EL, CLI, etc.). football-data.org es secundaria.

Con Pro no hay restricción de temporadas; se obtienen datos de la temporada actual y histórico según la cuota. Si en algún flujo aparece un mensaje o comentario que haga referencia a "plan free" o "plan gratuito" de API-Sports, debe considerarse erróneo: el sistema está pensado para Pro.
