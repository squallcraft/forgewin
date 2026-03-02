# Descripción del algoritmo y fuentes de datos

## 1. Fuentes de datos (no hay datos “hasta 2024” fijos)

- **Partidos y competiciones**: API **football-data.org v4** (`scraper.py`, `data_fetcher.py`).
  - Partidos: `GET /competitions/{code}/matches` con `dateFrom` / `dateTo` calculados con la **fecha actual** (`datetime.utcnow()`, `date.today()`).
  - No usamos un año ni una temporada fija: la temporada se calcula con `_current_season()` en función de la fecha actual (ej. 2025 si estamos en julio 2025 o después, si no año anterior).
- **Estadísticas de equipos**: `GET /teams/{id}/matches` con `status=FINISHED` y `limit=20` (últimos partidos acabados). Son datos que devuelve la API en el momento de la petición.
- **Clasificación (standings)**: `GET /competitions/{code}/standings` para la temporada actual (`_current_season()`).
- **H2H, odds, etc.**: También vía API o integradores, con fechas/parámetros actuales.

Por tanto, **el algoritmo no está “anclado” a 2024**: las fechas y la temporada se derivan del día en que se ejecuta la app.

---

## 2. Modelo de probabilidades (Poisson + mezcla con cuotas)

Está en `analyzer.py`:

1. **Lambdas (goles esperados por equipo)**  
   - Local: `lambda_home` a partir de “ataque del local” (goles a favor en últimos partidos) y “defensa del visitante” (goles en contra del visitante).  
   - Visitante: `lambda_away` de forma análoga.  
   - Fórmula tipo: `lambda = goals_avg_for * (goals_avg_against_opponent / 1.5)` (con mínimos para no dividir por cero).

2. **Ajustes**  
   - **H2H**: si en los últimos enfrentamientos un equipo marca más, su lambda se sube un poco (mezcla 70% lambda base, 30% media goles H2H).  
   - **xG (opcional)**: si hay datos externos de xG, se ajustan las lambdas (factor xG/avg).  
   - **Lesiones**: reducen la lambda del equipo afectado (hasta ~25%).  
   - **Cuotas de casas**: si hay cuotas, se calculan probabilidades implícitas y se hace una **mezcla** con las del Poisson (por defecto 60% Poisson, 40% casas). Con eso se obtienen las probabilidades finales 1-X-2.

3. **Salidas del modelo**  
   - Con las lambdas se calculan:  
     - P(victoria local), P(empate), P(victoria visitante) (sumando P(home=i)*P(away=j) para i>j, i=j, i<j).  
     - BTTS, Over/Under 2.5, clean sheet local/visitante, goles esperados totales.  
   - Si hay cuotas, además: value bet (nuestra prob. > implícita + margen 2%).

Todo esto usa **partidos recientes y temporada actual** que devuelve la API en el momento de la consulta; no hay un “histórico fijo hasta 2024” en nuestro código.

---

## 3. Qué enviamos a Alfred

A Alfred (analista, API xAI) le pasamos:

- Lista de partidos: **fixture_id, equipos (nombre), liga, fecha del partido**.
- **Nuestras probabilidades** (las que salen del Poisson + cuotas): victoria local/empate/visitante, goles esperados, clean sheet, BTTS, Over 2.5.

**No** le enviamos:

- Base de datos de jugadores.
- Plantillas actuales (“quién juega en qué equipo”).
- Fecha de corte de nuestro sistema (nosotros usamos siempre fecha actual).

Es decir, **todo lo que Alfred escribe sobre jugadores (Mbappé, goleadores, plantillas) sale de su propio modelo**, no de nuestras tablas ni de una “info hasta 2024” nuestra.

---

## 4. Sobre el error de Mbappé (PSG vs Real Madrid)

- **Origen del error**: La afirmación de que “Mbappé está en el PSG” en una propuesta la genera **Alfred (el modelo de lenguaje de xAI)**. Nosotros no le damos ni una base de jugadores ni plantillas; solo nombres de equipos, fechas y nuestras probs.
- **Por qué puede pasar**: Los modelos de lenguaje tienen una **fecha de corte de entrenamiento** (o de conocimiento). Si esa fecha es anterior a la temporada 2024/25, el modelo puede seguir asociando a Mbappé con el PSG y no con el Real Madrid.
- **Qué hemos hecho en la app**:
  - En el **prompt del sistema** para propuestas se indica que use **solo información actualizada** sobre jugadores y plantillas, y que si no está seguro del equipo actual de un jugador, no lo afirme o recomiende contrastar con fuentes actuales.
  - En cada petición a Alfred se envía la **fecha actual** (“Fecha actual (día de esta petición): YYYY-MM-DD”) para que contextualice temporada y trasferencias.

Con eso se reducen afirmaciones incorrectas como “Mbappé en el PSG”, pero **la corrección definitiva depende del modelo de Alfred** (que xAI actualice conocimiento o que el modelo tenga una fecha de corte más reciente).

---

## 5. Resumen

| Componente        | Fuente de datos / lógica                    | ¿Puede estar “solo hasta 2024”? |
|------------------|--------------------------------------------|----------------------------------|
| Partidos, fechas | API football-data.org + fecha del sistema | No; usamos fecha actual         |
| Temporada        | `_current_season()` con fecha actual       | No                              |
| Stats equipos    | API, últimos 20 partidos FINISHED          | No; lo que devuelve la API      |
| Poisson + cuotas | Nuestro código en `analyzer.py`            | No; inputs son actuales         |
| Texto de Alfred  | Modelo xAI (Alfred)                        | Sí; depende del corte del modelo |

Si quieres, en el siguiente paso podemos revisar juntos un partido concreto (fixture, equipos, fecha) y trazar de dónde sale cada número (API vs Poisson vs Alfred) en el código.
