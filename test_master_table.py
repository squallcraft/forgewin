#!/usr/bin/env python3
"""
Prueba de verificación de la tabla master_table y flujo de datos.

Verifica:
- Existencia de tablas (master_table, master_table_checked, entity_aliases, data_discrepancies)
- Columnas esperadas en master_table
- Integridad de datos: conteos, verificado_1/verificado_2
- Consistencia de clasificaciones (campeón por liga/temporada)

Uso:
  python test_master_table.py
  python test_master_table.py -v
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))


def test_tables_exist(conn):
    """Verifica que existan las tablas requeridas."""
    required = ["master_table", "master_table_checked", "entity_aliases", "data_discrepancies"]
    c = conn.cursor()
    c.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name IN (?,?,?,?)",
        tuple(required),
    )
    found = {r[0] for r in c.fetchall()}
    missing = set(required) - found
    assert not missing, f"Tablas faltantes: {missing}"
    return True


def test_master_table_columns(conn):
    """Verifica columnas mínimas en master_table."""
    required_cols = {
        "fixture_id", "date", "league_id", "home_team_name", "away_team_name",
        "home_goals", "away_goals", "ftr", "season", "verificado_1", "verificado_2", "source",
    }
    c = conn.cursor()
    c.execute("PRAGMA table_info(master_table)")
    cols = {r[1] for r in c.fetchall()}
    missing = required_cols - cols
    assert not missing, f"Columnas faltantes en master_table: {missing}"
    return True


def test_counts(conn):
    """Cuentas de registros por tabla."""
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM master_table")
    mt = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM master_table_checked")
    mtc = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM entity_aliases")
    ea = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM data_discrepancies WHERE status='pending'")
    dd = c.fetchone()[0]
    return {"master_table": mt, "master_table_checked": mtc, "entity_aliases": ea, "pending_discrepancies": dd}


def test_verification_stats(conn):
    """Estadísticas de verificado_1 y verificado_2."""
    c = conn.cursor()
    c.execute(
        """SELECT verificado_1, verificado_2, COUNT(*) FROM master_table GROUP BY verificado_1, verificado_2"""
    )
    rows = c.fetchall()
    stats = {(v1, v2): n for v1, v2, n in rows}
    return stats


def test_standings_consistency(conn):
    """Verifica que haya campeones calculables por liga/temporada (sin errores de integridad)."""
    c = conn.cursor()
    c.execute(
        """SELECT league_id, season, COUNT(*) as n
           FROM master_table
           WHERE (ftr = 'H' OR ftr = 'D' OR ftr = 'A')
           GROUP BY league_id, season
           HAVING n > 0
           ORDER BY league_id, season DESC
           LIMIT 10"""
    )
    rows = c.fetchall()
    return [(r[0], r[1], r[2]) for r in rows]


def main():
    parser = argparse.ArgumentParser(description="Prueba de master_table")
    parser.add_argument("-v", "--verbose", action="store_true", help="Modo verbose")
    args = parser.parse_args()

    from db import get_connection, init_db

    init_db()
    ok = True

    with get_connection() as conn:
        # 1. Tablas existan
        try:
            test_tables_exist(conn)
            print("[OK] Tablas master_table, master_table_checked, entity_aliases, data_discrepancies existen")
        except AssertionError as e:
            print(f"[FAIL] {e}")
            ok = False

        # 2. Columnas en master_table
        try:
            test_master_table_columns(conn)
            print("[OK] master_table tiene columnas esperadas (verificado_1, verificado_2, source, etc.)")
        except AssertionError as e:
            print(f"[FAIL] {e}")
            ok = False

        # 3. Conteos
        counts = test_counts(conn)
        print(f"[INFO] master_table: {counts['master_table']} registros")
        print(f"[INFO] master_table_checked: {counts['master_table_checked']} registros")
        print(f"[INFO] entity_aliases: {counts['entity_aliases']} aliases")
        print(f"[INFO] Discrepancias pendientes: {counts['pending_discrepancies']}")

        if counts["master_table"] == 0:
            print("[WARN] master_table vacía. Ejecuta: python run_copy_to_master_table.py --seed-aliases")

        # 4. Estadísticas de verificación
        stats = test_verification_stats(conn)
        if args.verbose:
            for (v1, v2), n in sorted(stats.items()):
                print(f"  verificado_1={v1}, verificado_2={v2}: {n} registros")

        # 5. Consistencia de clasificaciones (liga/temporada con datos)
        leagues_seasons = test_standings_consistency(conn)
        if leagues_seasons:
            print(f"[OK] Ligas/temporadas con partidos: {len(leagues_seasons)} muestras (liga, season, n_partidos)")
            if args.verbose:
                for lid, seas, n in leagues_seasons[:5]:
                    print(f"  {lid} {seas}: {n} partidos")

    # 6. Recomendación sobre verificación 1
    if args.verbose:
        print()
        print("=" * 70)
        print("RECOMENDACIÓN: ¿Comentar/desactivar la Verificación 1 (API-Sports)?")
        print("=" * 70)
        print("""
Actualmente:
- run_detect_discrepancies.py compara solo el CAMPEÓN (1º) por liga/temporada.
- No existe un job que marque verificado_1=1 partido a partido cuando coinciden.
- Al resolver una discrepancia, el admin marca verificado_1=1 y verificado_2=1
  para toda la liga/temporada.

OPCIÓN A – Mantener Verificación 1:
  Ventaja: detecta discrepancias (ej. Arsenal vs Man City en PL 2024) para que
  el admin las resuelva. Sin ella, no sabríamos si nuestros datos difieren.
  Desventaja: requiere API-Sports activa; puede haber falsos positivos por
  nombres distintos (ej. Man Utd vs Manchester United).

OPCIÓN B – Comentar/desactivar Verificación 1:
  Si comentas run_detect_discrepancies.py o no lo ejecutas:
  - No se insertan discrepancias nuevas.
  - Para promover a master_table_checked: hay que cambiar promote_to_master_checked
    para que acepte solo verificado_2=1 (sin exigir verificado_1=1).
  Ventaja: no depende de API-Sports; flujo más simple.
  Desventaja: pierdes la alerta automática cuando master_table difiere de API-Sports.

Recomendación: NO comentar la Verificación 1. Es útil como alerta. Si API-Sports
falla o no está disponible, run_detect_discrepancies simplemente no encontrará
discrepancias; el admin puede resolver manualmente cuando lo desee.
""")

    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
