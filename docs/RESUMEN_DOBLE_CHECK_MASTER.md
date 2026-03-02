# Resumen: datos históricos y doble check para pasar a la tabla maestra

## Origen: `historical_matches`

La **tabla histórica** es `historical_matches`: datos crudos de partidos (CSV football-data.co.uk, API-Sports, etc.). Esa tabla **no** tiene doble check; solo alimenta el flujo.

---

## Flujo hasta la tabla final

```
historical_matches  →  master_table  →  master_table_checked
     (crudos)           (staging)           (solo con doble check)
```

- **master_table**: copia de `historical_matches` con nombres normalizados (`entity_aliases`) + columnas `verificado_1`, `verificado_2`, `source`.
- **master_table_checked**: misma estructura de datos que `master_table`, pero **solo** filas que tienen **doble check** (verificado_1=1 y verificado_2=1). Es la única fuente que usan los agentes IA.

---

## Qué debe tener doble check para pasar a la tabla final

Un **partido** (una fila por `fixture_id`) pasa a `master_table_checked` **solo si**:

| Check | Significado | Cómo se cumple |
|-------|-------------|----------------|
| **Verificación 1** (`verificado_1 = 1`) | Coincide con API-Sports (o se ha resuelto la discrepancia) | Script `run_detect_discrepancies.py` compara partido a partido (goles, equipos, ftr, etc.) con API-Sports. Si hay diferencias, se registran en `data_discrepancies`. Cuando un humano **resuelve** una discrepancia (master / API / manual), se marca `verificado_1 = 1` para ese partido (o para toda la liga/temporada en caso de campeón). |
| **Verificación 2** (`verificado_2 = 1`) | Un humano ha validado | Al **resolver** una discrepancia en el panel de Administración (o al confirmar en el flujo de verificación), se marca `verificado_2 = 1` y `verificado_2_by` = user_id del admin. |

Cuando **ambas** están a 1, el registro se **promociona** automáticamente a `master_table_checked` (INSERT/REPLACE por `fixture_id`).

---

## Datos que se comparan en la verificación 1 (partido a partido)

Para que un partido en `master_table` pueda considerarse verificado frente a API-Sports, se comparan estos campos (los que tienen discrepancia se registran en `data_discrepancies`):

| Campo | Descripción |
|-------|-------------|
| `home_goals` | Goles local |
| `away_goals` | Goles visitante |
| `home_team_name` | Nombre equipo local (normalizado con `entity_aliases`) |
| `away_team_name` | Nombre equipo visitante |
| `ftr` | Resultado final (H/D/A) |
| `hthg`, `htag`, `htr` | Resultado medio tiempo (si API lo devuelve) |

El emparejamiento con API-Sports se hace por **fecha + liga + nombres de equipos** (normalizados).

---

## Verificación a nivel de clasificación (campeón)

Además de partido a partido, se detectan discrepancias de **campeón** por liga/temporada (master_table vs API-Sports). Cuando un humano resuelve una de estas:

- Se escribe en `standings_override`.
- Se marcan **verificado_1 = 1** y **verificado_2 = 1** para **todos** los partidos de esa liga/temporada en `master_table`.
- Esos partidos se promocionan a `master_table_checked`.

---

## Resumen en una frase

**Para que un dato histórico pase a la tabla final (`master_table_checked`), ese partido debe tener `verificado_1 = 1` (coincide con API-Sports o discrepancia resuelta) y `verificado_2 = 1` (humano ha validado); en ese momento se copia a `master_table_checked` y es el único dato que usan los agentes IA.**
