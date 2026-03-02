# Cron y actualización de datos históricos

## Producción: configuración recomendada

Para que todo funcione **sin intervención humana** en producción:

### 1. Script maestro diario

```bash
python scripts/run_production_daily.py
```

Orquesta en orden:
1. Partidos nuevos (últimos 2 días) desde API-Sports  
2. Backfill estadísticas (shots, corners, fouls, offsides)  
3. Backfill tarjetas (yellow/red)  
4. Backfill offsides (filas con stats pero sin offsides)  
5. Backfill attendance y referee (vía GET /fixtures?id=)  
6. Backfill xG  
7. Resumen final  

Opciones útiles:
- `--batch-limit 500` – procesa máximo 500 partidos por backfill (evita cron muy largo)
- `--skip-backfills` – solo sync nuevos partidos  
- `--dry-run` – simular sin modificar  

### 2. Revisión de completitud

```bash
python scripts/check_data_completeness.py
```

Detecta gaps: partidos con `api_sports_fixture_id` pero sin offsides, attendance, referee, stats, xG o tarjetas.

Opciones:
- `--json` – salida en JSON  
- `--log-to-db` – guarda el reporte en `data_completeness_log` para auditoría  
- `--fail-threshold 10000` – exit 1 si algún gap supera el umbral (para alertas)  

### 3. Ventana rolling

```bash
python manage.py maintain_rolling_window
```

Elimina partidos de temporadas antiguas (mantiene últimas 5 temporadas).

### Crontab de producción

Ver `cron/crontab.production.example`. Resumen:

| Hora (Chile) | Frecuencia | Comando |
|--------------|------------|---------|
| 02:00 | Diario | `scripts/run_production_daily.py` |
| 03:00 | Diario | `scripts/check_data_completeness.py --log-to-db` |
| 05:00 | Lun y Jue | `manage.py maintain_rolling_window` |

Configurar `TZ=America/Santiago` en el sistema. Crear `logs/` y ajustar rutas en el crontab.

---

Ver también `docs/BACKFILL_OFFSIDES_ATTENDANCE.md` para detalles de los backfills de offsides y attendance.

---

## Comandos manuales (legacy)

1. **Actualizar hasta ayer (script antiguo)**  
   ```bash
   python scripts/update_historical_through_yesterday.py
   ```  
   Añade partidos desde el último en BD hasta ayer y ejecuta backfills (attendance/referee, estadísticas, tarjetas).  
   **Nota:** no incluye offsides ni attendance vía fixture_id. Para producción usar `run_production_daily.py`.

2. **Actualización semanal vía manage**  
   ```bash
   python manage.py update_historical_data --weekly
   ```  
   Hace sync (7 días) + ventana rolling. Redundante si ya usas `run_production_daily` diario + `maintain_rolling_window` semanal.

3. **Descarga inicial (primera vez)**  
   ```bash
   python manage.py update_historical_data --full
   ```  
