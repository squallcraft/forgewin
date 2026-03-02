#!/usr/bin/env python3
"""Crea usuarios de prueba: usuario1 (base) y usuario2 (plus). Ejecutar desde la raíz del proyecto."""
import sys
from pathlib import Path

# Asegurar que el proyecto está en el path
root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(root))

from db import init_db
from auth import create_user, ensure_admin_exists

def main():
    init_db()
    ensure_admin_exists()

    users = [
        ("usuario1", "123", "base"),
        ("usuario2", "123", "plus"),
    ]
    for username, password, tier in users:
        ok, msg = create_user(username, password, email="", role="user", grok_enabled=False, tier=tier)
        if ok:
            print(f"OK: {username} (perfil {tier}) — {msg}")
        else:
            print(f"SKIP/ERROR: {username} — {msg}")

if __name__ == "__main__":
    main()
