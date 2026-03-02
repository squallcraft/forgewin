# Uso de Gemini API en Forgewin vs Free Tier

Referencias oficiales:
- **Límites de frecuencia (RPM, TPM, RPD):** [ai.google.dev/gemini-api/docs/rate-limits](https://ai.google.dev/gemini-api/docs/rate-limits)
- **Precios y comparativa Free vs Pay:** [ai.google.dev/gemini-api/docs/pricing](https://ai.google.dev/gemini-api/docs/pricing)

---

## 1. Límites del Free Tier (resumen)

Los límites se aplican **por proyecto de Google Cloud**, no por API key. RPD se reinicia a medianoche (hora del Pacífico).

| Modelo / familia | RPM (req/min) | TPM (tokens/min) | RPD (req/día) |
|------------------|----------------|-------------------|----------------|
| **Gemini 2.5 Pro** | 5  | 250.000 | **100** |
| **Gemini 2.5 Flash** | 10 | 250.000 | 250 |
| **Gemini 2.5 Flash-Lite** | 15 | 250.000 | 1.000 |
| **Gemini 2.0 Flash** *(usado en Forgewin)* | ~10* | 250.000 | ~250* |

\* Gemini 2.0 Flash suele agruparse en la documentación con los modelos Flash; en la práctica se aplican límites del orden de 10 RPM y 250 RPD. Consulta la doc actual por si hay diferencias por nombre exacto del modelo.

---

## 2. Uso de Gemini en Forgewin

### Modelos configurados (`config.py`)

- **`GEMINI_MODEL`** (por defecto `gemini-2.0-flash`): análisis Opción 2 (Reginald) y paso 1 del consenso.
- **`GEMINI_MODEL_CONSENSUS`** (por defecto `gemini-2.5-pro`): solo **paso 6** del consenso (síntesis final Propuesta General 1+2).

### Peticiones por acción del usuario

| Acción | Llamadas Gemini | Modelos |
|--------|------------------|--------|
| **«Dale con todo» solo Opción 2** | 1 | 1× Flash (2.0) |
| **«Dale con todo» + Propuesta General 1+2 (consenso)** | 3 | 1× Flash (análisis) + 1× Flash (paso 1) + **1× Pro** (paso 6) |
| **Análisis v2 Reginald** | 1 | 1× Flash |

Origen en código:
- `ask_gemini_proposal_analysis`: 1 petición (Flash) — `gemini_client.py` + `app.py`.
- Consenso: paso 1 → `ask_gemini_custom` (Flash); paso 6 → `ask_gemini_custom(..., model=GEMINI_MODEL_CONSENSUS)` (Pro) — `consensus.py`.
- v2: `ask_gemini_custom` (Flash) — `app.py`.

---

## 3. ¿Estamos dentro del Free Tier?

### Sí, con estas condiciones

- **Uso normal** (varios «Dale con todo» y alguna Propuesta General 1+2 al día):  
  - Flash: 1–2 peticiones por análisis, 2 por consenso → bien por debajo de 10 RPM y 250 RPD.  
  - Pro: 1 petición por cada Propuesta General 1+2 → **máximo 100 Propuestas General 1+2 completas por día** (límite de 100 RPD de 2.5 Pro).

- **TPM (250.000/min):**  
  Una propuesta con muchos partidos puede ser ~20k–80k tokens de entrada; el paso 6 del consenso envía mucho contexto (ambos análisis + opiniones), por ejemplo ~50k–150k tokens. Con 1–2 flujos por minuto es razonable mantenerse bajo 250k TPM. Si en el mismo minuto se lanzan varios consensos con muchos partidos, podría acercarse al límite.

### Puntos donde puedes salirte del Free Tier

1. **RPM (Pro): 5 req/min**  
   Cada **Propuesta General 1+2** consume 1 petición a **Gemini 2.5 Pro** (paso 6). Si en un mismo minuto se completan **más de 5** flujos de consenso, se supera el límite y puedes recibir **429**. El código ya hace backoff ante 429 (véase `gemini_client.py`).

2. **RPD (Pro): 100 req/día**  
   Límite duro: **100 usos de Propuesta General 1+2 por día** (por proyecto). Si varios usuarios o muchas ejecuciones diarias superan 100, empezarán a fallar las llamadas al Pro hasta el día siguiente.

3. **TPM en picos**  
   Varias propuestas/consensos con muchos partidos en el mismo minuto pueden acercarse a 250k tokens/min; en ese caso también es posible 429 por TPM.

---

## 4. Recomendaciones

- **Monitorear 429:** El mensaje que devuelve el cliente («Demasiadas peticiones… Espera 1-2 minutos») indica que se ha tocado RPM o TPM.
- **Si abusas del Pro (429 por RPD/RPM):** En `.env` puedes usar solo Flash para todo, por ejemplo  
  `GEMINI_MODEL_CONSENSUS=gemini-2.0-flash` (o `gemini-2.5-flash` si está disponible), así el paso 6 deja de consumir cuota del Pro (100 RPD) y pasa a la cuota Flash (250 RPD, 10 RPM).
- **Scripts/test:** `query_pl_season_gemini.py`, `test_historical_gemini.py` y cualquier script que llame a `ask_gemini_*` suman a RPM/RPD/TPM; en uso intenso, espaciar ejecuciones o usar solo en desarrollo.

**Conclusión:** El uso típico de Forgewin (Opción 2 + alguna Propuesta General 1+2) **sí cabe en el Free Tier** de Gemini. El límite más restrictivo es **100 RPD para Gemini 2.5 Pro** (paso 6 del consenso). Si quieres estar cómodo o crecer uso sin pagar, conviene usar un modelo Flash también para el paso 6 (`GEMINI_MODEL_CONSENSUS=gemini-2.0-flash`).

---

## 5. Escenarios creíbles en los que pasarías a tier de pago

Sin restricciones adicionales, estos casos pueden hacer que superes el free tier y tengas que activar facturación (o recibir 429 de forma constante).

| Escenario | Qué pasa | Límite que se toca |
|-----------|----------|----------------------|
| **Varios usuarios activos el mismo día** | 15–20 usuarios hacen cada uno 5–10 «Propuesta General 1+2». Total 75–200 llamadas al Pro en un día. | **RPD Pro (100)** → 429 tras ~100 consensos/día en todo el proyecto. |
| **Un usuario muy activo** | Un solo usuario (o un integrador/script) lanza 10+ Propuestas General 1+2 en una mañana. | **RPD Pro (100)** se agota en pocas horas para todo el sistema. |
| **Hora pico** | 3–4 usuarios pulsan «Propuesta General 1+2» en el mismo minuto (p. ej. antes de una jornada). | **RPM Pro (5)** → 429 para las peticiones que pasen de 5 en ese minuto. |
| **Partidos por propuesta muy altos** | Usuarios Plus con listas combinadas: 8–12 partidos por análisis. Varios consensos seguidos. | **TPM (250k/min)** por peticiones muy grandes (paso 6 con mucho contexto). |
| **Abuso o script** | Alguien automatiza «Dale con todo» + consenso en bucle (por curiosidad, testing o mal uso). | **RPD Pro y Flash** se agotan en poco tiempo; 429 continuos. |
| **Crecimiento orgánico** | Forgewin gana usuarios: 30–50 usuarios que usan 2–3 consensos/día = 60–150 llamadas Pro/día. | **RPD Pro (100)** superado de forma estable → necesitas tier de pago o cambiar a Flash en paso 6. |

Resumen: el **cuello de botella** es **100 RPD de Gemini 2.5 Pro** (una llamada por cada Propuesta General 1+2). Cualquier situación donde el total de «Propuesta General 1+2» completadas por día pase de ~100 en tu proyecto te acerca o te lleva al límite.

---

## 6. Restricciones recomendadas para evitar pasar a tier de pago

Objetivo: repartir el uso entre usuarios y que el **total diario** no supere 100 llamadas al Pro (y opcionalmente proteger también Flash y TPM), sin dejar de dar buen servicio.

### 6.1 Por usuario (recomendado)

- **Límite diario de Propuesta General 1+2 por usuario**  
  Ejemplo: máximo **5** (o **10**) Propuestas General 1+2 por usuario y por día (UTC o hora Chile).  
  - Con 20 usuarios y tope 5 → máximo 100 llamadas Pro/día si todos hacen 5. Así te mantienes en el free tier.  
  - Implementación: tabla o contador `propuesta_general_count_per_user_per_day` (o reutilizar fechas de `proposals` y contar por `user_id` y fecha).

- **Límite por minuto por usuario**  
  Máximo **1** Propuesta General 1+2 por usuario por minuto (evita que un solo usuario dispare 5 en 1 minuto y sature RPM Pro para todos).

- **Créditos por consenso**  
  Ya descontáis créditos por partido. Opcional: que cada **Propuesta General 1+2** consuma **1 crédito extra** (o 2) además de los partidos, para que no sea “gratis” abusar del consenso.

### 6.2 Por producto / tier

- **Propuesta General 1+2 solo para Plus / Usuario++**  
  Si hoy está disponible para todos los que tienen créditos, limitarlo a `tier in ('plus','plus_plus')` reduce el volumen de uso del Pro (los base solo usarían Opción 1 y Opción 2 por separado).

- **Opción 2 (Reginald) y v2**  
  Mantenerlos por créditos (partidos) está bien. Opcional: tope diario por usuario para “análisis Reginald” (p. ej. 20/día) para no acercarte a 250 RPD de Flash si crece mucho el uso.

### 6.3 Globales (por proyecto / día)

- **Tope global diario de Propuesta General 1+2**  
  Si el número de consensos del día (por todos los usuarios) supera un umbral (p. ej. **95**), mostrar: *«Hemos alcanzado el límite de análisis de consenso por hoy. Vuelve mañana o usa solo Opción 1 / Opción 2.»*  
  Así nunca pasas de 100 RPD Pro aunque no controles bien por usuario.

- **Usar solo Flash en paso 6**  
  Configurar `GEMINI_MODEL_CONSENSUS=gemini-2.0-flash` (o `gemini-2.5-flash`). El paso 6 deja de consumir cuota Pro (100 RPD) y usa la de Flash (250 RPD, 10 RPM). La calidad sigue siendo buena y te da más margen sin pagar.

### 6.4 Resumen práctico

| Restricción | Efecto | Dificultad |
|-------------|--------|------------|
| Máx. N Propuesta General 1+2 por usuario y día (p. ej. 5) | Controla RPD Pro; reparte cuota entre usuarios. | Media (contador por user_id + fecha). |
| Máx. 1 Propuesta General 1+2 por usuario por minuto | Evita picos de RPM Pro. | Baja (timestamp última ejecución por usuario). |
| Propuesta General 1+2 solo tier Plus/Plus++ | Menos uso Pro. | Baja (ya tienes `tier`). |
| Tope global diario (p. ej. 95) y mensaje “límite del día” | No superar 100 RPD Pro nunca. | Media (contador global por día). |
| Paso 6 con Flash en lugar de Pro | Elimina el límite de 100 RPD Pro. | Muy baja (variable de entorno). |

Recomendación mínima para no pasar a tier de pago sin tocar mucho código: **(1)** poner `GEMINI_MODEL_CONSENSUS=gemini-2.0-flash` y **(2)** un tope por usuario (p. ej. 5 Propuestas General 1+2/día) o un tope global diario (95). Si además añades “1 consenso por minuto por usuario”, proteges también los picos de RPM.
