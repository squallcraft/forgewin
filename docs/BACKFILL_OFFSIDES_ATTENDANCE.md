# Backfill Offsides y Attendance (plan Pro/Ultra)

Con el plan mayor de API-Sports tienes cuota suficiente para rellenar offsides y attendance vía llamadas directas por partido.

## Situación actual

| Métrica | Cobertura | Causa |
|---------|-----------|-------|
| **Offsides** | ~9% (11k) | El backfill de stats solo procesa filas con `home_shots IS NULL`. Las ~50k con stats pero sin offsides nunca se rellenaban. |
| **Attendance** | ~3% (3.5k) | El backfill bulk (liga+temporada) depende del matching por nombres; muchos fallan. API a veces no devuelve attendance. |

## Solución: llamadas directas por fixture_id

### 1. Offsides

```bash
python run_backfill_offsides_from_apisports.py --dry-run   # Ver pendientes
python run_backfill_offsides_from_apisports.py --continuous
```

- **Pendientes:** ~50k partidos con `api_sports_fixture_id` y `home_offsides`/`away_offsides` vacíos
- **Endpoint:** GET `/fixtures/statistics?fixture=ID`
- **Calls:** 1 por partido
- **Tiempo estimado:** ~3.5 h con delay 0.25s (Pro)
- **Bonus:** También rellena xG cuando falta

### 2. Attendance (y referee)

```bash
python run_backfill_attendance_via_fixture_id.py --dry-run
python run_backfill_attendance_via_fixture_id.py --continuous
```

- **Pendientes:** ~57k partidos con `api_sports_fixture_id` sin attendance o referee
- **Endpoint:** GET `/fixtures?id=ID`
- **Calls:** 1 por partido
- **Tiempo estimado:** ~4 h con delay 0.25s (Pro)

**Nota:** API-Sports no siempre tiene attendance (muchas ligas no lo reportan). Los que la API devuelve se guardan; los que no, quedan NULL.

## Orden recomendado

1. `run_backfill_statistics_from_apisports.py --continuous` (stats principales)
2. `run_backfill_cards_from_apisports.py --continuous`
3. `run_backfill_offsides_from_apisports.py --continuous`
4. `run_backfill_attendance_via_fixture_id.py --continuous`

O en una sola sesión nocturna:

```bash
python run_backfill_offsides_from_apisports.py --continuous &
python run_backfill_attendance_via_fixture_id.py --continuous
```

(El segundo puede esperar al primero, o correr en paralelo si la cuota Pro lo permite.)

## Cuota API-Sports (plan Pro)

- 300 req/min
- Con delay 0.25s → ~4 req/s = 240 req/min ✓
- 50k + 57k ≈ 107k calls → ~7.4 h total en secuencia

Con plan Ultra (más req/min) puedes reducir el delay y terminar antes.
