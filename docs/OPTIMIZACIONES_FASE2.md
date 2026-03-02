# Fase 2 – Optimizaciones pendientes

Documento de diseño para implementar cuando la app y los datos lo permitan.

## 1. Motor de aprendizaje (recalibración λ por liga)

- **Objetivo:** Comparar resultado real vs esperado (ej. xG) y aplicar un multiplicador de sesgo al parámetro λ por liga cuando la desviación supere un umbral.
- **Condiciones de activación:**
  - Mínimo **30 partidos** por liga antes de activar el ajuste.
  - Tope de ajuste: **±12%** en el parámetro por liga (evitar que el modelo se desvíe por ruido o pocos datos).
- **Umbral:** Si la desviación acumulada en una liga es mayor al **15%**, aplicar el multiplicador de sesgo para los próximos cálculos de esa liga.
- **Implementación:** Worker o job que evalúe diariamente (o por lote) y persista el bias por liga para usarlo en los cálculos de Poisson / métricas.

## 2. Reembolso automático de créditos

- Si una propuesta de análisis **falla por error de API** (tras reintentos con backoff) o no puede completarse, el sistema debe:
  - Devolver el crédito al usuario de forma automática.
  - Notificar en la interfaz (mensaje claro, sin exigir ticket).
- Activar cuando el modelo de créditos esté operativo.

## 3. Facturación electrónica (Chile / SII)

- Integrar webhook de facturación (ej. LibreDTE o deFacto) para que cada **compra de paquete de créditos** dispare:
  - Emisión de **Boleta Electrónica** afecta a IVA.
  - Envío al correo del usuario en tiempo real.
- Dejar preparado el punto de extensión (webhook/cola) aunque la integración completa se haga al definir proveedor y flujo de pago.

---

*Fase 1 (enrutamiento Flash/Pro, Grok fast, recorte de prompts, backoff, paralelización) ya implementada en el código.*
