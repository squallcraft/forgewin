#!/usr/bin/env python3
"""
Diagnóstico: qué devuelve football-data.org para Champions en las fechas del test.
Muestra partidos devueltos y nombres de equipos para ver si el fallo es por nombres o por fecha.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
except ImportError:
    pass

def main():
    from config import API_KEY, BASE_URL, REQUEST_DELAY_SECONDS
    import requests
    import time

    if not API_KEY:
        print("FOOTBALL_DATA_ORG_TOKEN no configurada. No se puede llamar a la API.")
        return

    # Fechas de nuestros partidos CL (2026-02-17 y 2026-02-18)
    from datetime import datetime, timedelta
    fechas = ["2026-02-17", "2026-02-18"]
    # Nombres que tenemos en nuestra BD (los del test)
    nuestros = [
        ("Qarabag", "Newcastle"),
        ("Bodo/Glimt", "Inter"),
        ("Olympiakos Piraeus", "Bayer Leverkusen"),
        ("Benfica", "Real Madrid"),
        ("Monaco", "Paris Saint Germain"),
        ("Borussia Dortmund", "Atalanta"),
    ]

    print("=" * 70)
    print("Diagnóstico: football-data.org – Champions League")
    print("=" * 70)
    print("\nNombres en nuestra BD (los que buscamos):")
    for h, a in nuestros:
        print(f"  {h} vs {a}")
    print()

    url_base = BASE_URL.rstrip("/")
    path = "competitions/CL/matches"

    for date_str in fechas:
        d = datetime.strptime(date_str[:10], "%Y-%m-%d")
        date_to = (d + timedelta(days=1)).strftime("%Y-%m-%d")
        params = {"dateFrom": date_str[:10], "dateTo": date_to}
        url = f"{url_base}/{path}"
        print(f"--- GET {path} dateFrom={date_str} dateTo={date_to} ---")
        time.sleep(REQUEST_DELAY_SECONDS)
        try:
            r = requests.get(url, headers={"X-Auth-Token": API_KEY}, params=params, timeout=15)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            print(f"  Error: {e}")
            if hasattr(e, "response") and e.response is not None:
                print(f"  Status: {e.response.status_code}")
            continue

        if data.get("message"):
            print(f"  Mensaje API: {data.get('message')}")
        matches = data.get("matches") or []
        print(f"  Partidos devueltos: {len(matches)}")
        for m in matches:
            ht = (m.get("homeTeam") or {}).get("name") or ""
            at = (m.get("awayTeam") or {}).get("name") or ""
            mid = m.get("id")
            print(f"    id={mid}  {ht} vs {at}")
        print()

    print("=" * 70)
    print("Si 'Partidos devueltos: 0' → la API no tiene partidos en esa fecha (p. ej. fechas futuras).")
    print("Si hay partidos pero nombres distintos (ej. Bodø vs Bodo, Olympiacos vs Olympiakos)")
    print("→ el fallo es por coincidencia de nombres, no por IDs.")
    print("=" * 70)


if __name__ == "__main__":
    main()
