# Fuentes fallback para partidos

Cuando **football-data.org** no devuelve partidos (ej. Europa League en plan gratuito), ForgeWin intenta:

1. **API-Sports** (api-sports.io)
2. **TheSportsDB** (thesportsdb.com)

## Resultados de tests

### API-Sports
- **ForgeWin está configurado para API-Sports Pro** (plan de pago, 300 req/min). Con Pro se obtienen Europa League, Copa Libertadores y temporada actual sin restricción de temporadas.
- Obtener: https://www.api-football.com/

### TheSportsDB
- Gratis con key compartida "3".
- Límite ~1000 req/día.
- `eventsday.php` puede devolver 429 si se hacen muchas peticiones.
- El fetcher limita a 7 días para reducir rate limit.

### Conclusión
ForgeWin usa **API-Sports Pro** como fuente principal; con Pro se obtienen Europa League, Copa Libertadores y temporada actual. football-data.org es fuente secundaria.
