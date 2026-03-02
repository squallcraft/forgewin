"""
Sistema de referidos de ForgeWin.

Reglas de negocio:
- El vlogger genera un código único (ej: TOMAS20).
- Primera compra del usuario referido paga 30% de descuento → 14.000 CLP.
- El vlogger gana 3.000 CLP de comisión por esa compra (se registra en BD).
- Solo se paga comisión en la PRIMERA compra del usuario referido.
- Un usuario solo puede usar un código una vez.
"""

import logging
import random
import string
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# ── Constantes de negocio ─────────────────────────────────────────────────────
NORMAL_PACK_PRICE_CLP = 20_000       # precio normal del pack de 10 créditos
REFERRAL_DISCOUNT_PCT = 0.30         # 30 %
REFERRAL_PRICE_CLP = int(NORMAL_PACK_PRICE_CLP * (1 - REFERRAL_DISCOUNT_PCT))  # 14.000
REFERRAL_COMMISSION_CLP = 3_000      # comisión al vlogger
REFERRAL_CREDITS = 10                # créditos que recibe el usuario referido


# ── Helpers de DB ─────────────────────────────────────────────────────────────

def _get_conn():
    from db import get_connection
    return get_connection()


# ── Generación de código ──────────────────────────────────────────────────────

def generate_referral_code(user_id: int, username: str = "") -> Tuple[bool, str]:
    """
    Genera un código único para el usuario (o devuelve el existente).
    Formato: hasta 8 letras del username en mayúsculas + 4 dígitos. Ej: TOMAS2024.
    Retorna (ok, code_or_error_message).
    """
    existing = get_referral_code_by_user(user_id)
    if existing:
        return True, existing["code"]

    prefix = "".join(c.upper() for c in username if c.isalpha())[:8] or "FW"

    for _ in range(20):
        suffix = "".join(random.choices(string.digits, k=4))
        code = f"{prefix}{suffix}"[:12]
        if not get_referral_code_by_code(code):
            now = datetime.utcnow().isoformat()
            with _get_conn() as conn:
                c = conn.cursor()
                c.execute(
                    "INSERT INTO referral_codes (user_id, code, active, created_at) VALUES (?, ?, ?, ?)",
                    (user_id, code, True, now),
                )
            logger.info("referrals: código creado %s → user_id=%s", code, user_id)
            return True, code

    return False, "No se pudo generar código único. Intenta más tarde."


# ── Lectura de códigos ────────────────────────────────────────────────────────

def get_referral_code_by_user(user_id: int) -> Optional[Dict[str, Any]]:
    """Devuelve el registro del código activo del usuario (o None)."""
    with _get_conn() as conn:
        c = conn.cursor()
        c.execute(
            "SELECT * FROM referral_codes WHERE user_id = ? AND active = TRUE ORDER BY created_at DESC LIMIT 1",
            (user_id,),
        )
        row = c.fetchone()
        return dict(row) if row else None


def get_referral_code_by_code(code: str) -> Optional[Dict[str, Any]]:
    """Devuelve el registro del código (activo o no)."""
    with _get_conn() as conn:
        c = conn.cursor()
        c.execute(
            "SELECT * FROM referral_codes WHERE code = ?",
            (code.upper().strip(),),
        )
        row = c.fetchone()
        return dict(row) if row else None


# ── Validación ────────────────────────────────────────────────────────────────

def validate_referral_code(code: str, new_user_id: int) -> Tuple[bool, str]:
    """
    Valida si el código es aplicable para new_user_id.
    Retorna (ok, mensaje_para_el_usuario).

    Falla si:
    - El código no existe o está inactivo.
    - El usuario intenta usar su propio código.
    - El usuario ya usó algún código de referido antes.
    """
    if not code or not code.strip():
        return False, "Ingresa un código."

    code = code.upper().strip()
    record = get_referral_code_by_code(code)

    if not record:
        return False, "Código inválido."
    if not record.get("active"):
        return False, "Código inactivo."
    if record["user_id"] == new_user_id:
        return False, "No puedes usar tu propio código."

    with _get_conn() as conn:
        c = conn.cursor()
        c.execute(
            "SELECT id FROM referral_usage WHERE referred_user_id = ?",
            (new_user_id,),
        )
        if c.fetchone():
            return False, "Ya usaste un código de referido anteriormente."

    return True, f"✅ Código válido — pagas ${REFERRAL_PRICE_CLP:,} CLP (30% desc.)".replace(",", ".")


def user_has_used_referral(user_id: int) -> bool:
    """True si el usuario ya usó un código de referido en alguna compra."""
    with _get_conn() as conn:
        c = conn.cursor()
        c.execute("SELECT id FROM referral_usage WHERE referred_user_id = ?", (user_id,))
        return c.fetchone() is not None


# ── Registro de comisión post-pago ────────────────────────────────────────────

def process_referral_after_payment(
    referred_user_id: int,
    code: str,
    paid_amount: int,
) -> Tuple[bool, str]:
    """
    Llamar cuando el pago del primer pack es APROBADO.
    Registra la comisión en referral_usage con status='pending'.
    Idempotente: si ya existe un registro para el usuario referido, no duplica.
    """
    code = code.upper().strip()
    record = get_referral_code_by_code(code)
    if not record or not record.get("active"):
        return False, f"Código '{code}' no encontrado o inactivo."

    referrer_id = record["user_id"]

    # Idempotencia: ya procesado
    with _get_conn() as conn:
        c = conn.cursor()
        c.execute(
            "SELECT id FROM referral_usage WHERE referred_user_id = ?",
            (referred_user_id,),
        )
        if c.fetchone():
            return True, "Referido ya procesado (sin duplicar)."

    # Registrar
    now = datetime.utcnow().isoformat()
    with _get_conn() as conn:
        c = conn.cursor()
        c.execute(
            """INSERT INTO referral_usage
               (referred_user_id, referrer_user_id, code_used, purchase_amount, commission_paid, status, created_at)
               VALUES (?, ?, ?, ?, ?, 'pending', ?)""",
            (referred_user_id, referrer_id, code, paid_amount, REFERRAL_COMMISSION_CLP, now),
        )
    logger.info(
        "referrals: comisión registrada — referido=%s vlogger=%s code=%s amount=%s",
        referred_user_id, referrer_id, code, paid_amount,
    )
    return True, f"Comisión de ${REFERRAL_COMMISSION_CLP:,} CLP registrada para el vlogger.".replace(",", ".")


# ── Dashboard del vlogger ─────────────────────────────────────────────────────

def get_vlogger_dashboard(user_id: int) -> Dict[str, Any]:
    """
    Devuelve datos para el panel del vlogger:
    - código personal, enlace de referido, referidos, comisiones.
    """
    import os
    code_record = get_referral_code_by_user(user_id)
    base_url = os.getenv("FORGEWIN_BASE_URL", "https://forgewin.cl")

    if not code_record:
        return {
            "has_code": False,
            "code": None,
            "referral_link": None,
            "referral_count": 0,
            "total_commission": 0,
            "pending_commission": 0,
            "paid_commission": 0,
            "usages": [],
        }

    code = code_record["code"]
    referral_link = f"{base_url}?ref={code}"

    with _get_conn() as conn:
        c = conn.cursor()
        c.execute(
            """SELECT ru.id, ru.referred_user_id, ru.purchase_amount,
                      ru.commission_paid, ru.status, ru.created_at,
                      u.username AS referred_username
               FROM referral_usage ru
               JOIN users u ON u.id = ru.referred_user_id
               WHERE ru.referrer_user_id = ?
               ORDER BY ru.created_at DESC""",
            (user_id,),
        )
        usages = [dict(r) for r in c.fetchall()]

    total = sum(u["commission_paid"] for u in usages)
    pending = sum(u["commission_paid"] for u in usages if u["status"] == "pending")
    paid = sum(u["commission_paid"] for u in usages if u["status"] == "paid")

    return {
        "has_code": True,
        "code": code,
        "referral_link": referral_link,
        "referral_count": len(usages),
        "total_commission": total,
        "pending_commission": pending,
        "paid_commission": paid,
        "usages": usages,
    }


# ── Admin: marcar comisiones como pagadas ─────────────────────────────────────

def mark_commission_paid(usage_id: int) -> Tuple[bool, str]:
    """Admin: marca una comisión como pagada."""
    with _get_conn() as conn:
        c = conn.cursor()
        c.execute(
            "UPDATE referral_usage SET status = 'paid' WHERE id = ? AND status = 'pending'",
            (usage_id,),
        )
    return True, f"Comisión #{usage_id} marcada como pagada."


def get_all_pending_commissions() -> List[Dict[str, Any]]:
    """Admin: lista todas las comisiones pendientes de pago."""
    with _get_conn() as conn:
        c = conn.cursor()
        c.execute(
            """SELECT ru.*, vlogger.username AS vlogger_username, referred.username AS referred_username
               FROM referral_usage ru
               JOIN users vlogger ON vlogger.id = ru.referrer_user_id
               JOIN users referred ON referred.id = ru.referred_user_id
               WHERE ru.status = 'pending'
               ORDER BY ru.created_at DESC""",
        )
        return [dict(r) for r in c.fetchall()]
