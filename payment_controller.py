"""
Controlador de pagos con Mercado Pago para ForgeWin.
Gestiona suscripciones recurrentes (Plus/Plus++) y compras únicas de packs de créditos.
Requiere: MERCADOPAGO_ACCESS_TOKEN en .env
"""

import logging
import os
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent / ".env")
except ImportError:
    pass

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

ACCESS_TOKEN = os.getenv("MERCADOPAGO_ACCESS_TOKEN") or os.getenv("MP_ACCESS_TOKEN") or ""
BASE_URL = os.getenv("FORGEWIN_BASE_URL") or os.getenv("BASE_URL") or "http://localhost:8501"

# Planes de suscripción (recurrentes mensuales)
SUBSCRIPTION_PLANS: Dict[str, Dict[str, Any]] = {
    "plus": {
        "tier": "plus",
        "reason": "ForgeWin Usuario+ - Análisis combinado, historial y más",
        "amount": 4990,  # CLP
        "currency_id": "CLP",
    },
    "plus_plus": {
        "tier": "plus_plus",
        "reason": "ForgeWin Usuario++ - Modo V2, Alfred+Reginald completo",
        "amount": 9990,  # CLP
        "currency_id": "CLP",
    },
}

# Packs de créditos (pagos únicos) — IVA incluido
CREDIT_PACKS: Dict[str, Dict[str, Any]] = {
    "mvp_10":  {"tier": "base", "credits": 10,  "amount": 20000,  "title": "10 análisis (IVA incluido)"},
    "mvp_25":  {"tier": "base", "credits": 25,  "amount": 50000,  "title": "25 análisis (IVA incluido)"},
    "mvp_50":  {"tier": "base", "credits": 50,  "amount": 100000, "title": "50 análisis (IVA incluido)"},
}

# Precio por crédito individual — 2.000 CLP IVA incluido. Mínimo de compra: 10.
PRICE_PER_CREDIT_CLP = 2000
MIN_CREDITS_PURCHASE = 10


def _get_sdk():
    """Inicializa el SDK de Mercado Pago."""
    if not ACCESS_TOKEN:
        return None
    try:
        import mercadopago
        return mercadopago.SDK(ACCESS_TOKEN)
    except ImportError:
        logger.warning("payment_controller: mercadopago no instalado (pip install mercadopago)")
        return None


def is_configured() -> bool:
    """True si Mercado Pago está configurado."""
    return bool(ACCESS_TOKEN)


# ---------------------------------------------------------------------------
# Suscripciones (preapproval - cobros recurrentes)
# ---------------------------------------------------------------------------


def create_subscription(
    user_id: int,
    tier: str,
    payer_email: str,
    back_url: Optional[str] = None,
) -> Tuple[Optional[str], str]:
    """
    Crea una suscripción en Mercado Pago y devuelve la URL init_point para redirigir al usuario.
    tier: 'plus' | 'plus_plus'
    Devuelve (init_point_url, error_message). Si error_message no está vacío, init_point es None.
    """
    if not is_configured():
        return None, "Mercado Pago no configurado (MERCADOPAGO_ACCESS_TOKEN faltante)."

    plan = SUBSCRIPTION_PLANS.get(tier)
    if not plan:
        return None, f"Plan '{tier}' no existe."

    sdk = _get_sdk()
    if not sdk:
        return None, "No se pudo inicializar el SDK de Mercado Pago."

    back = back_url or f"{BASE_URL.rstrip('/')}?payment=subscription&status=ok"
    start = datetime.utcnow()
    end = start + timedelta(days=365 * 2)  # 2 años

    preapproval_data = {
        "reason": plan.get("reason", f"ForgeWin {tier}"),
        "external_reference": f"fw-sub-{user_id}-{tier}-{int(time.time())}",
        "payer_email": payer_email.strip(),
        "back_url": back,
        "auto_recurring": {
            "frequency": 1,
            "frequency_type": "months",
            "start_date": start.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            "end_date": end.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            "transaction_amount": float(plan.get("amount", 4990)),
            "currency_id": plan.get("currency_id", "CLP"),
        },
    }

    try:
        resp = sdk.preapproval().create(preapproval_data)
        if resp.get("status") in (200, 201):
            body = resp.get("response") or {}
            init_point = body.get("init_point")
            preapproval_id = body.get("id")
            if init_point and preapproval_id:
                from db import upsert_mp_subscription
                upsert_mp_subscription(
                    user_id=user_id,
                    mp_preapproval_id=str(preapproval_id),
                    tier=tier,
                    status=body.get("status", "pending"),
                    payer_email=payer_email,
                )
                return init_point, ""
        err = resp.get("response") or resp
        msg = str(err.get("message", err) if isinstance(err, dict) else err)
        return None, f"Mercado Pago: {msg}"
    except Exception as e:
        logger.exception("payment_controller: error creando suscripción")
        return None, str(e)


# ---------------------------------------------------------------------------
# Pagos únicos (preference - packs de créditos)
# ---------------------------------------------------------------------------


def create_preference_for_pack(
    user_id: int,
    pack_id: str,
    payer_email: str,
    back_url: Optional[str] = None,
    notification_url: Optional[str] = None,
) -> Tuple[Optional[str], str]:
    """
    Crea una preferencia (pago único) para un pack de créditos.
    pack_id: 'base_10', 'base_50', 'plus_50', etc. (ver CREDIT_PACKS)
    Devuelve (init_point_url, error_message).
    """
    if not is_configured():
        return None, "Mercado Pago no configurado (MERCADOPAGO_ACCESS_TOKEN faltante)."

    pack = CREDIT_PACKS.get(pack_id)
    if not pack:
        return None, f"Pack '{pack_id}' no existe."

    sdk = _get_sdk()
    if not sdk:
        return None, "No se pudo inicializar el SDK de Mercado Pago."

    ext_ref = f"fw-pack-{user_id}-{pack_id}-{uuid.uuid4().hex[:8]}"
    back = back_url or f"{BASE_URL.rstrip('/')}?payment=pack&status=ok"
    if not back.startswith("http"):
        back = f"http://{back}" if "://" not in back else back
    notif = notification_url or os.getenv("MP_WEBHOOK_URL") or ""

    # No usar auto_return: MP exige back_url.success válida (HTTPS) y rechaza localhost.
    # Sin auto_return el pago funciona; el usuario vuelve con el botón de MP.
    preference_data = {
        "items": [
            {
                "title": pack.get("title", f"ForgeWin - {pack.get('credits', 0)} créditos"),
                "quantity": 1,
                "unit_price": float(pack.get("amount", 0)),
                "currency_id": "CLP",
            }
        ],
        "payer": {"email": payer_email.strip()} if payer_email and payer_email.strip() else None,
        "external_reference": ext_ref,
        "back_urls": {"success": back, "failure": back, "pending": back},
        "notification_url": notif if notif else None,
        "metadata": {"user_id": user_id, "pack_id": pack_id, "tier": pack.get("tier", "base"), "credits": pack.get("credits", 0)},
    }
    # Quitar keys con None
    preference_data = {k: v for k, v in preference_data.items() if v is not None}

    try:
        logger.info("payment_controller: creando preferencia pack=%s user=%s", pack_id, user_id)
        resp = sdk.preference().create(preference_data)
        logger.info("payment_controller: respuesta MP status=%s", resp.get("status"))
        if resp.get("status") in (200, 201):
            body = resp.get("response") or {}
            init_point = body.get("init_point") or body.get("sandbox_init_point")
            if init_point:
                from db import upsert_mp_payment
                upsert_mp_payment(
                    user_id=user_id,
                    external_reference=ext_ref,
                    tier=pack.get("tier", "base"),
                    credits=pack.get("credits", 0),
                    amount=float(pack.get("amount", 0)),
                    currency_id="CLP",
                    status="pending",
                    mp_preference_id=str(body.get("id", "")),
                )
                logger.info("payment_controller: init_point creado OK")
                return init_point, ""
        err = resp.get("response") or resp
        msg = str(err.get("message", err) if isinstance(err, dict) else err)
        logger.warning("payment_controller: MP rechazó preferencia: %s", msg)
        return None, f"Mercado Pago: {msg}"
    except Exception as e:
        logger.exception("payment_controller: error creando preferencia")
        return None, str(e)


def create_preference_for_credits(
    user_id: int,
    num_credits: int,
    payer_email: str,
    back_url: Optional[str] = None,
    notification_url: Optional[str] = None,
) -> Tuple[Optional[str], str]:
    """
    Crea una preferencia para comprar N créditos sueltos a 1000 CLP cada uno.
    Devuelve (init_point_url, error_message).
    """
    if not is_configured():
        return None, "Mercado Pago no configurado (MERCADOPAGO_ACCESS_TOKEN faltante)."
    if num_credits < 1:
        return None, "Debe comprar al menos 1 crédito."

    sdk = _get_sdk()
    if not sdk:
        return None, "No se pudo inicializar el SDK de Mercado Pago."

    unit_price = PRICE_PER_CREDIT_CLP
    total = unit_price * num_credits
    ext_ref = f"fw-credits-{user_id}-{num_credits}-{uuid.uuid4().hex[:8]}"
    back = back_url or f"{BASE_URL.rstrip('/')}?payment=pack&status=ok"
    if not back.startswith("http"):
        back = f"http://{back}" if "://" not in back else back
    notif = notification_url or os.getenv("MP_WEBHOOK_URL") or ""

    preference_data = {
        "items": [
            {
                "title": f"ForgeWin - {num_credits} crédito(s) para análisis",
                "quantity": num_credits,
                "unit_price": float(unit_price),
                "currency_id": "CLP",
            }
        ],
        "payer": {"email": payer_email.strip()} if payer_email and payer_email.strip() else None,
        "external_reference": ext_ref,
        "back_urls": {"success": back, "failure": back, "pending": back},
        "notification_url": notif if notif else None,
        "metadata": {"user_id": user_id, "credits": num_credits},
    }
    preference_data = {k: v for k, v in preference_data.items() if v is not None}

    try:
        logger.info("payment_controller: creando preferencia créditos user=%s num=%s", user_id, num_credits)
        resp = sdk.preference().create(preference_data)
        if resp.get("status") in (200, 201):
            body = resp.get("response") or {}
            init_point = body.get("init_point") or body.get("sandbox_init_point")
            if init_point:
                from db import upsert_mp_payment
                upsert_mp_payment(
                    user_id=user_id,
                    external_reference=ext_ref,
                    tier="base",
                    credits=num_credits,
                    amount=float(total),
                    currency_id="CLP",
                    status="pending",
                    mp_preference_id=str(body.get("id", "")),
                )
                return init_point, ""
        err = resp.get("response") or resp
        msg = str(err.get("message", err) if isinstance(err, dict) else err)
        return None, f"Mercado Pago: {msg}"
    except Exception as e:
        logger.exception("payment_controller: error creando preferencia créditos")
        return None, str(e)


# ---------------------------------------------------------------------------
# Webhooks: procesar notificaciones de Mercado Pago
# ---------------------------------------------------------------------------


def process_payment_notification(payment_id: str) -> Tuple[bool, str]:
    """
    Procesa una notificación de pago (webhook). Obtiene el pago de MP, verifica estado
    y si está aprobado añade créditos al usuario.
    Devuelve (ok, mensaje).
    """
    if not is_configured():
        return False, "Mercado Pago no configurado."

    sdk = _get_sdk()
    if not sdk:
        return False, "SDK no disponible."

    try:
        resp = sdk.payment().get(payment_id)
        if resp.get("status") != 200:
            return False, f"Error obteniendo pago: {resp}"

        payment = resp.get("response") or {}
        status = payment.get("status")
        ext_ref = payment.get("external_reference") or ""

        if not ext_ref or not (ext_ref.startswith("fw-pack-") or ext_ref.startswith("fw-credits-")):
            return False, f"external_reference inválido: {ext_ref}"

        from db import get_mp_payment_by_external_reference, upsert_mp_payment
        from auth import add_credits

        record = get_mp_payment_by_external_reference(ext_ref)
        if not record:
            return False, f"No existe registro para {ext_ref}"

        if str(record.get("status")) == "approved":
            return True, "Pago ya procesado."

        if status == "approved":
            user_id = int(record.get("user_id"))
            credits = int(record.get("credits", 0))
            tier = str(record.get("tier", "base"))
            ok, msg = add_credits(user_id, credits, tier)
            if ok:
                now = datetime.utcnow().isoformat()
                upsert_mp_payment(
                    user_id=user_id,
                    external_reference=ext_ref,
                    tier=tier,
                    credits=credits,
                    amount=float(record.get("amount", 0)),
                    status="approved",
                    mp_payment_id=payment_id,
                    mp_preference_id=record.get("mp_preference_id"),
                )
                return True, f"Créditos añadidos: {credits}. {msg}"
            return False, msg
        elif status in ("pending", "in_process", "in_mediation"):
            return True, f"Pago en estado {status}. Se procesará al aprobarse."
        else:
            return True, f"Pago en estado {status}. Sin acción."

    except Exception as e:
        logger.exception("payment_controller: error procesando pago %s", payment_id)
        return False, str(e)


def process_preapproval_notification(preapproval_id: str) -> Tuple[bool, str]:
    """
    Procesa notificación de suscripción (preapproval). Si status=authorized/active,
    actualiza el tier del usuario.
    """
    if not is_configured():
        return False, "Mercado Pago no configurado."

    sdk = _get_sdk()
    if not sdk:
        return False, "SDK no disponible."

    try:
        resp = sdk.preapproval().get(preapproval_id)
        if resp.get("status") != 200:
            return False, f"Error obteniendo preapproval: {resp}"

        sub = resp.get("response") or {}
        status = sub.get("status")
        ext_ref = sub.get("external_reference") or ""

        if not ext_ref or not ext_ref.startswith("fw-sub-"):
            return False, f"external_reference inválido: {ext_ref}"

        from db import get_mp_subscription_by_preapproval_id, update_mp_subscription_status
        from auth import set_user_tier

        record = get_mp_subscription_by_preapproval_id(preapproval_id)
        if not record:
            return False, f"No existe suscripción para {preapproval_id}"

        update_mp_subscription_status(preapproval_id, status)

        if status in ("authorized", "active"):
            user_id = int(record.get("user_id"))
            tier = str(record.get("tier", "plus"))
            set_user_tier(user_id, tier)
            return True, f"Suscripción activa. Usuario actualizado a {tier}."
        elif status in ("pending", "pending_conformity"):
            return True, f"Suscripción en estado {status}."
        elif status in ("cancelled", "paused", "expired"):
            # Bajar el tier a 'base' cuando la suscripción se cancela/expira.
            user_id = int(record.get("user_id"))
            set_user_tier(user_id, "base")
            return True, f"Suscripción {status}. Tier del usuario bajado a base."
        return True, f"Estado: {status}"

    except Exception as e:
        logger.exception("payment_controller: error procesando preapproval %s", preapproval_id)
        return False, str(e)


# ---------------------------------------------------------------------------
# Utilidades
# ---------------------------------------------------------------------------


def get_subscription_status(user_id: int) -> Optional[Dict[str, Any]]:
    """Devuelve la suscripción activa del usuario si existe."""
    from db import get_connection
    with get_connection() as conn:
        c = conn.cursor()
        c.execute(
            "SELECT * FROM mp_subscriptions WHERE user_id = ? ORDER BY created_at DESC LIMIT 1",
            (user_id,),
        )
        row = c.fetchone()
        return dict(row) if row else None


def list_plans() -> List[Dict[str, Any]]:
    """Lista planes de suscripción para mostrar en UI."""
    return [
        {"id": k, **v, "type": "subscription"}
        for k, v in SUBSCRIPTION_PLANS.items()
    ]


def list_packs() -> List[Dict[str, Any]]:
    """Lista packs de créditos para mostrar en UI."""
    return [
        {"id": k, **v, "type": "pack"}
        for k, v in CREDIT_PACKS.items()
    ]
