"""
Migración de datos: SQLite → PostgreSQL.

Uso:
    DATABASE_URL=postgresql://user:pass@host:5432/forgewin \
    python scripts/migrate_sqlite_to_pg.py

El script:
1. Conecta a la SQLite local (forgewin.db).
2. Conecta a PostgreSQL usando DATABASE_URL.
3. Crea todas las tablas en PG (init_db).
4. Copia tabla por tabla en orden (respetando FK).
5. Es idempotente: usa INSERT ... ON CONFLICT DO NOTHING.
"""

import os
import sys
import sqlite3
import time
from pathlib import Path

# Asegurarnos de estar en el directorio raíz del proyecto
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    print("ERROR: Define DATABASE_URL=postgresql://... antes de ejecutar.")
    sys.exit(1)

DB_PATH = os.getenv("FOOTBALL_DB_PATH") or str(ROOT / "forgewin.db")
if not Path(DB_PATH).exists():
    print(f"ERROR: SQLite no encontrada en {DB_PATH}")
    sys.exit(1)

import psycopg2
import psycopg2.extras

# Orden de migración (respeta FOREIGN KEYs)
TABLES_IN_ORDER = [
    "users",
    "matches",
    "proposals",
    "proposal_matches",
    "match_results",
    "proposal_outcomes",
    "mp_subscriptions",
    "mp_payments",
    "error_reports",
    "historical_matches",
    "league_standings",
    "entity_aliases",
    "master_table",
    "master_table_checked",
    "data_discrepancies",
    "data_completeness_log",
    "standings_override",
    "pending_team_aliases",
]


def get_sqlite_columns(sqlite_conn: sqlite3.Connection, table: str) -> list[str]:
    cur = sqlite_conn.execute(f"PRAGMA table_info({table})")
    return [row[1] for row in cur.fetchall()]


SERIAL_TABLES = {
    "users", "mp_subscriptions", "mp_payments", "error_reports",
    "historical_matches", "entity_aliases", "master_table",
    "master_table_checked", "data_discrepancies", "data_completeness_log",
    "pending_team_aliases",
}
BATCH_SIZE = 2000


def migrate_table(sqlite_conn: sqlite3.Connection, pg_conn, table: str) -> int:
    cols = get_sqlite_columns(sqlite_conn, table)
    if not cols:
        print(f"  {table}: tabla no encontrada en SQLite, omitida.")
        return 0

    insert_cols = [c for c in cols if not (c == "id" and table in SERIAL_TABLES)]

    sqlite_cur = sqlite_conn.execute(f"SELECT {', '.join(cols)} FROM {table}")
    rows = sqlite_cur.fetchall()
    if not rows:
        print(f"  {table}: 0 filas.")
        return 0

    col_str = ", ".join(insert_cols)
    conflict_col = _conflict_col(table)
    on_conflict = f"ON CONFLICT ({conflict_col}) DO NOTHING" if conflict_col else "ON CONFLICT DO NOTHING"
    upsert_sql = f"INSERT INTO {table} ({col_str}) VALUES %s {on_conflict}"

    pg_cur = pg_conn.cursor()
    inserted = 0
    skipped = 0
    total = len(rows)

    for batch_start in range(0, total, BATCH_SIZE):
        batch = rows[batch_start: batch_start + BATCH_SIZE]
        values = []
        for row in batch:
            row_dict = dict(zip(cols, row))
            values.append(tuple(row_dict[c] for c in insert_cols))

        try:
            psycopg2.extras.execute_values(pg_cur, upsert_sql, values, page_size=BATCH_SIZE)
            pg_conn.commit()
            inserted += len(batch)
            pct = int((batch_start + len(batch)) / total * 100)
            print(f"  {table}: {batch_start + len(batch)}/{total} ({pct}%)...", end="\r")
        except Exception as e:
            pg_conn.rollback()
            skipped += len(batch)
            if skipped <= BATCH_SIZE * 3:
                print(f"\n    ⚠ lote omitido en {table}: {e}")

    # Sincronizar secuencias SERIAL
    if table in SERIAL_TABLES:
        try:
            pg_cur.execute(
                f"SELECT setval(pg_get_serial_sequence('{table}', 'id'), COALESCE(MAX(id), 1)) FROM {table}"
            )
            pg_conn.commit()
        except Exception:
            pg_conn.rollback()

    print(f"  {table}: {inserted} insertadas, {skipped} omitidas.        ")
    return inserted


def _conflict_col(table: str) -> str:
    """Devuelve la columna unique/PK para ON CONFLICT."""
    mapping = {
        "users": "username",
        "matches": "fixture_id",
        "proposals": "id",
        "match_results": "fixture_id",
        "mp_subscriptions": "mp_preapproval_id",
        "mp_payments": "external_reference",
        "historical_matches": "fixture_id",
        "league_standings": "league_id, season, rank",
        "entity_aliases": "entity_type, alias, league_id",
        "master_table": "fixture_id",
        "master_table_checked": "fixture_id",
        "standings_override": "league_id, season, rank",
        "pending_team_aliases": "league_id, name_variant_1, name_variant_2",
    }
    return mapping.get(table, "")


def main():
    print(f"SQLite origen : {DB_PATH}")
    print(f"PostgreSQL destino: {DATABASE_URL.split('@')[-1]}")
    print()

    sqlite_conn = sqlite3.connect(DB_PATH)
    sqlite_conn.row_factory = sqlite3.Row

    pg_conn = psycopg2.connect(DATABASE_URL)
    pg_conn.autocommit = False

    # Crear tablas en PG
    print("Creando tablas en PostgreSQL...")
    os.environ["DATABASE_URL"] = DATABASE_URL  # asegura que db.init_db() use PG
    import db as forgewin_db
    forgewin_db.init_db()
    print("✓ Tablas creadas.\n")

    total_rows = 0
    t0 = time.time()

    for table in TABLES_IN_ORDER:
        n = migrate_table(sqlite_conn, pg_conn, table)
        total_rows += n

    elapsed = time.time() - t0
    print(f"\n✓ Migración completa: {total_rows} filas en {elapsed:.1f}s")

    sqlite_conn.close()
    pg_conn.close()


if __name__ == "__main__":
    main()
