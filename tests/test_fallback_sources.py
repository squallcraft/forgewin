"""
Tests de fuentes fallback (API-Sports, TheSportsDB).
Ejecutar: pytest tests/test_fallback_sources.py -v -s
"""
import os
import sys

import pytest

# Añadir raíz al path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def test_api_sports_has_key():
    """Comprueba que API_FOOTBALL_KEY esté configurada."""
    from api_sports_fetcher import API_KEY
    assert API_KEY, "Configura API_FOOTBALL_KEY en .env para tests de fallback"


@pytest.mark.skipif(
    not os.getenv("API_FOOTBALL_KEY"),
    reason="API_FOOTBALL_KEY no configurada",
)
def test_api_sports_el_fixtures():
    """
    ForgeWin usa API-Sports Pro. Con Pro deberían devolverse partidos para el rango indicado.
    """
    from api_sports_fetcher import get_fixtures

    m = get_fixtures("EL", "2026-02-18", "2026-03-31")
    assert isinstance(m, list)


@pytest.mark.skipif(
    not os.getenv("API_FOOTBALL_KEY"),
    reason="API_FOOTBALL_KEY no configurada",
)
def test_api_sports_cli_fixtures():
    """Copa Libertadores - con API-Sports Pro se obtienen partidos."""
    from api_sports_fetcher import get_fixtures

    m = get_fixtures("CLI", "2026-02-18", "2026-04-30")
    assert isinstance(m, list)


def test_fallback_orchestrator():
    """El orquestador no debe fallar aunque las APIs devuelvan vacío."""
    from datetime import datetime, timedelta
    from fallback_fixtures import fetch_fallback

    today = datetime.utcnow().date()
    date_from = today.isoformat()
    date_to = (today + timedelta(days=7)).isoformat()
    m = fetch_fallback(["EL", "CLI"], date_from, date_to)
    assert isinstance(m, list)
