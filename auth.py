"""
Autenticación y gestión de usuarios.
Contraseñas con hash; admin puede crear usuarios y activar grok_enabled.
"""

import hashlib
import secrets
from typing import Optional, Tuple

from db import get_connection


def _salt_hash(password: str, salt: Optional[str] = None) -> Tuple[str, str]:
    salt = salt or secrets.token_hex(16)
    h = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt.encode("utf-8"), 100000)
    return salt, h.hex()


def hash_password(password: str) -> str:
    """Devuelve 'salt:hex' para almacenar en BD."""
    salt, hexdig = _salt_hash(password)
    return f"{salt}:{hexdig}"


def verify_password(password: str, stored: str) -> bool:
    """Verifica contraseña contra el valor almacenado (salt:hex)."""
    if ":" not in stored:
        return False
    salt, hexdig = stored.split(":", 1)
    _, expected = _salt_hash(password, salt)
    return secrets.compare_digest(hexdig, expected)


def get_user_by_username(username: str) -> Optional[dict]:
    with get_connection() as conn:
        c = conn.cursor()
        c.execute(
            "SELECT id, username, email, password_hash, role, grok_enabled, tier, credits_balance, created_at FROM users WHERE username = ?",
            (username.strip(),)
        )
        row = c.fetchone()
        if row:
            d = dict(row)
            if "tier" not in d:
                d["tier"] = "base"
            if "credits_balance" not in d:
                d["credits_balance"] = 0
            d["grok_enabled"] = bool(d.get("grok_enabled"))
            return d
        return None


def get_user_by_id(user_id: int) -> Optional[dict]:
    with get_connection() as conn:
        c = conn.cursor()
        c.execute(
            "SELECT id, username, email, password_hash, role, grok_enabled, tier, credits_balance, created_at FROM users WHERE id = ?",
            (user_id,)
        )
        row = c.fetchone()
        if row:
            d = dict(row)
            if "tier" not in d:
                d["tier"] = "base"
            if "credits_balance" not in d:
                d["credits_balance"] = 0
            d["grok_enabled"] = bool(d.get("grok_enabled"))
            return d
        return None


def create_user(
    username: str,
    password: str,
    email: str = "",
    role: str = "user",
    grok_enabled: bool = False,
    tier: str = "base",
) -> Tuple[bool, str]:
    """
    Crea usuario (admin desde panel, o autoregistro con role=user, tier=base). Devuelve (ok, mensaje).
    tier = 'base' | 'plus' (solo para role=user).
    """
    username = (username or "").strip()
    if not username:
        return False, "El usuario es obligatorio."
    if len(username) < 3 or len(username) > 40:
        return False, "El usuario debe tener entre 3 y 40 caracteres."
    if not password:
        return False, "La contraseña es obligatoria."
    if len(password) < 8:
        return False, "La contraseña debe tener al menos 8 caracteres."
    tier = "plus_plus" if tier == "plus_plus" else ("plus" if tier == "plus" else "base")
    with get_connection() as conn:
        c = conn.cursor()
        c.execute("SELECT id FROM users WHERE username = ?", (username,))
        if c.fetchone():
            return False, "El usuario ya existe."
        from datetime import datetime
        c.execute(
            "INSERT INTO users (username, email, password_hash, role, grok_enabled, tier, credits_balance, created_at) VALUES (?, ?, ?, ?, ?, ?, 0, ?)",
            (username, email.strip(), hash_password(password), role, 1 if grok_enabled else 0, tier, datetime.utcnow().isoformat())
        )
    return True, "Usuario creado."


def list_users() -> list:
    """Lista todos los usuarios (id, username, email, role, grok_enabled, tier, credits_balance). Para admin."""
    with get_connection() as conn:
        c = conn.cursor()
        c.execute("SELECT id, username, email, role, grok_enabled, tier, credits_balance FROM users ORDER BY username")
        rows = c.fetchall()
        out = []
        for r in rows:
            d = dict(r)
            if "tier" not in d:
                d["tier"] = "base"
            if "credits_balance" not in d:
                d["credits_balance"] = 0
            d["grok_enabled"] = bool(d.get("grok_enabled"))
            out.append(d)
        return out


def set_grok_enabled(user_id: int, enabled: bool) -> bool:
    """Activa o desactiva Alfred para un usuario. Solo admin."""
    with get_connection() as conn:
        conn.execute("UPDATE users SET grok_enabled = ? WHERE id = ?", (1 if enabled else 0, user_id))
    return True


def set_user_tier(user_id: int, tier: str) -> bool:
    """Asigna tier (base | plus | plus_plus) a un usuario. Solo admin. No aplica a role=admin."""
    tier = "plus_plus" if tier == "plus_plus" else ("plus" if tier == "plus" else "base")
    with get_connection() as conn:
        conn.execute("UPDATE users SET tier = ? WHERE id = ? AND role != 'admin'", (tier, user_id))
    return True


_TIER_RANK = {"base": 0, "plus": 1, "plus_plus": 2}


def add_credits(user_id: int, amount: int, pack_tier: str) -> Tuple[bool, str]:
    """
    Añade créditos y sube el tier si el pack comprado es superior al actual.
    El tier nunca baja por una compra: un usuario plus_plus que compra un pack base
    conserva su tier plus_plus.
    pack_tier = 'base' | 'plus' | 'plus_plus'.
    Devuelve (ok, mensaje).
    """
    if amount <= 0:
        return False, "La cantidad debe ser mayor que 0."
    pack_tier = "plus_plus" if pack_tier == "plus_plus" else ("plus" if pack_tier == "plus" else "base")
    with get_connection() as conn:
        c = conn.cursor()
        c.execute("SELECT role, credits_balance, tier FROM users WHERE id = ?", (user_id,))
        row = c.fetchone()
        if not row:
            return False, "Usuario no encontrado."
        r = dict(row)
        role = r.get("role")
        balance = int(r.get("credits_balance") or 0)
        if role == "admin":
            return False, "No se asignan créditos al admin."
        current_tier = r.get("tier") or "base"
        # Solo subir el tier, nunca bajarlo.
        new_tier = pack_tier if _TIER_RANK.get(pack_tier, 0) > _TIER_RANK.get(current_tier, 0) else current_tier
        new_balance = balance + amount
        c.execute("UPDATE users SET credits_balance = ?, tier = ? WHERE id = ?", (new_balance, new_tier, user_id))
    return True, f"Se añadieron {amount} créditos. Perfil: {new_tier}. Total: {new_balance}."


def deduct_credits(user_id: int, amount: int = 1) -> Tuple[bool, str]:
    """
    Resta créditos tras un análisis. Solo usuarios no admin. Devuelve (ok, mensaje).
    Si el saldo queda insuficiente no cambia el tier (el tier solo se actualiza al comprar).
    """
    if amount <= 0:
        return True, "Sin deducción."
    with get_connection() as conn:
        c = conn.cursor()
        c.execute("SELECT role, credits_balance FROM users WHERE id = ?", (user_id,))
        row = c.fetchone()
        if not row:
            return False, "Usuario no encontrado."
        r = dict(row)
        if r.get("role") == "admin":
            return True, "Admin: no se descuentan créditos."
        balance = int(r.get("credits_balance") or 0)
        if balance < amount:
            return False, "Créditos insuficientes."
        new_balance = balance - amount
        c.execute("UPDATE users SET credits_balance = ? WHERE id = ?", (new_balance, user_id))
    return True, str(new_balance)


def ensure_admin_exists() -> None:
    """Crea o actualiza usuario admin por defecto (admin / admin2026!)."""
    from datetime import datetime
    pwd_hash = hash_password("admin2026!")
    now = datetime.utcnow().isoformat()
    with get_connection() as conn:
        c = conn.cursor()
        c.execute("SELECT id FROM users WHERE username = 'admin' LIMIT 1")
        row = c.fetchone()
        if row is None:
            c.execute(
                "INSERT INTO users (username, email, password_hash, role, grok_enabled, tier, credits_balance, created_at) VALUES (?, ?, ?, 'admin', 1, 'base', 0, ?)",
                ("admin", "admin@local", pwd_hash, now)
            )
        else:
            c.execute("UPDATE users SET password_hash = ? WHERE username = 'admin'", (pwd_hash,))
