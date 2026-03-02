"""
Servidor de webhooks para Mercado Pago.
Recibe notificaciones de pagos y suscripciones. Debe ejecutarse con una URL pública (ngrok, etc.)
para que Mercado Pago pueda enviar las notificaciones.

Uso:
  uvicorn webhook_server:app --host 0.0.0.0 --port 8001

Configurar en Mercado Pago (Tus integraciones > Webhooks):
  - URL: https://tu-dominio.com/webhooks/mercadopago
  - Eventos: payment, subscription_preapproval
"""

import hashlib
import hmac
import logging
import os
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent / ".env")
except ImportError:
    pass

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

app = FastAPI(title="ForgeWin Webhooks", version="1.0")

MP_WEBHOOK_SECRET = os.getenv("MP_WEBHOOK_SECRET") or ""


def _verify_mp_signature(request: Request, raw_body: bytes) -> bool:
    """
    Verifica la firma x-signature de Mercado Pago.
    Docs: https://www.mercadopago.cl/developers/es/docs/your-integrations/notifications/webhooks
    Si MP_WEBHOOK_SECRET no está configurado, pasa sin verificar (modo desarrollo).
    """
    if not MP_WEBHOOK_SECRET:
        return True
    sig_header = request.headers.get("x-signature", "")
    ts_header = request.headers.get("x-request-id", "")
    # Formato: "ts=...,v1=..."
    parts = dict(p.split("=", 1) for p in sig_header.split(",") if "=" in p)
    ts = parts.get("ts", "")
    v1 = parts.get("v1", "")
    if not ts or not v1:
        return False
    manifest = f"id:{ts};request-id:{ts_header};ts:{ts};"
    expected = hmac.new(
        MP_WEBHOOK_SECRET.encode("utf-8"),
        manifest.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return hmac.compare_digest(expected, v1)


@app.post("/webhooks/mercadopago")
async def mercadopago_webhook(request: Request):
    """
    Mercado Pago envía POST con body JSON:
    { "type": "payment" | "subscription_preapproval", "data": { "id": "123" } }

    Siempre responde 200 para que MP no reintente. Los errores internos se loguean.
    """
    raw_body = await request.body()

    if not _verify_mp_signature(request, raw_body):
        logger.warning("webhook: firma inválida — posible petición no autorizada")
        # Devolvemos 200 igualmente para no exponer que rechazamos por firma.
        return JSONResponse({"received": True}, status_code=200)

    try:
        import json
        body = json.loads(raw_body)
        notif_type = body.get("type")
        data = body.get("data") or {}
        obj_id = data.get("id")

        if not obj_id:
            logger.warning("webhook: falta 'id' en el body")
            return JSONResponse({"received": True}, status_code=200)

        from payment_controller import process_payment_notification, process_preapproval_notification

        if notif_type == "payment":
            ok, msg = process_payment_notification(str(obj_id))
        elif notif_type == "subscription_preapproval":
            ok, msg = process_preapproval_notification(str(obj_id))
        else:
            logger.info("webhook: tipo desconocido %s, id=%s", notif_type, obj_id)
            return JSONResponse({"received": True}, status_code=200)

        if ok:
            logger.info("webhook: OK — %s id=%s msg=%s", notif_type, obj_id, msg)
        else:
            logger.error("webhook: error procesando %s id=%s: %s", notif_type, obj_id, msg)

        # Siempre 200: MP no debe reintentar por errores de lógica interna.
        return JSONResponse({"received": True, "ok": ok, "message": msg}, status_code=200)

    except Exception as e:
        logger.exception("webhook: excepción no controlada")
        # También 200 para evitar reintentos de MP en errores de parseo.
        return JSONResponse({"received": True, "error": str(e)}, status_code=200)


@app.get("/health")
async def health():
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("WEBHOOK_PORT", "8001"))
    uvicorn.run("webhook_server:app", host="0.0.0.0", port=port)