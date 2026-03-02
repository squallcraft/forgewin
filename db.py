"""
Base de datos — compatible con SQLite (dev) y PostgreSQL (producción).
Selección automática: si DATABASE_URL está definida usa PostgreSQL, si no usa SQLite.
"""

import json
import os
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import datetime, date, time, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

DATABASE_URL = os.getenv("DATABASE_URL")  # postgresql://user:pass@host:5432/dbname
_DB_MODE = "pg" if DATABASE_URL else "sqlite"

# Ruta SQLite (solo cuando _DB_MODE == "sqlite")
DB_PATH = os.getenv("FOOTBALL_DB_PATH") or str(Path(__file__).resolve().parent / "forgewin.db")

if _DB_MODE == "pg":
    import psycopg2
    import psycopg2.extras

    def _pg_clean(val):
        """Convierte tipos numpy/pandas a tipos Python nativos que psycopg2 entiende."""
        tp = type(val).__module__
        if tp == "numpy" or (hasattr(val, "item") and callable(val.item)):
            try:
                return val.item()
            except Exception:
                pass
        if tp.startswith("pandas"):
            try:
                import math
                if math.isnan(float(val)):
                    return None
            except Exception:
                pass
            try:
                return val.item()
            except Exception:
                return None
        return val

    def _pg_clean_params(params):
        if params is None:
            return None
        return [_pg_clean(v) for v in params]

    class _PGCursor:
        """Adapta psycopg2 DictCursor para aceptar '?' como placeholder y exponer .lastrowid."""

        def __init__(self, raw_cursor):
            self._c = raw_cursor
            self._lastrowid: Optional[int] = None

        def execute(self, sql: str, params=None):
            sql_pg = sql.replace("?", "%s")
            self._c.execute(sql_pg, _pg_clean_params(params))
            if sql_pg.lstrip().upper().startswith("INSERT"):
                try:
                    self._c.execute("SELECT LASTVAL()")
                    row = self._c.fetchone()
                    self._lastrowid = row[0] if row else None
                except Exception:
                    self._lastrowid = None
            return self

        def executemany(self, sql: str, params_list):
            self._c.executemany(sql.replace("?", "%s"), [_pg_clean_params(p) for p in params_list])

        def fetchone(self):
            return self._c.fetchone()

        def fetchall(self):
            return self._c.fetchall()

        def __iter__(self):
            return iter(self._c.fetchall())

        @property
        def rowcount(self) -> int:
            return self._c.rowcount

        @property
        def lastrowid(self) -> Optional[int]:
            return self._lastrowid

    class _PGConnection:
        """Adapta psycopg2 connection para emular la API de sqlite3."""

        def __init__(self, raw_conn):
            self._conn = raw_conn

        def cursor(self) -> "_PGCursor":
            return _PGCursor(self._conn.cursor())

        def execute(self, sql: str, params=None) -> "_PGCursor":
            c = self.cursor()
            c.execute(sql, params)
            return c

        def commit(self):
            self._conn.commit()

        def rollback(self):
            self._conn.rollback()

        def close(self):
            self._conn.close()

    @contextmanager
    def get_connection():
        raw = psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.DictCursor)
        # autocommit=True: cada statement es su propia transacción atómica.
        # Evita que un error en una query aborte el resto del bloque (PG behavior).
        raw.autocommit = True
        conn = _PGConnection(raw)
        try:
            yield conn
        finally:
            raw.close()

else:
    @contextmanager
    def get_connection():
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

# PK auto-incremental compatible con ambos motores
_PK_AUTO = "SERIAL PRIMARY KEY" if _DB_MODE == "pg" else "INTEGER PRIMARY KEY AUTOINCREMENT"


def _try_add_column(c, table: str, col: str, typ: str) -> None:
    """Agrega columna si no existe. IF NOT EXISTS en PG, try/except en SQLite."""
    if _DB_MODE == "pg":
        c.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {col} {typ}")
    else:
        try:
            c.execute(f"ALTER TABLE {table} ADD COLUMN {col} {typ}")
        except Exception:
            pass


def _try_insert_ignore_duplicate(c, sql: str, params) -> bool:
    """Ejecuta INSERT y devuelve True si insertó, False si hubo clave duplicada.
    Usa SAVEPOINT en PG para no abortar la transacción en curso."""
    if _DB_MODE == "pg":
        c.execute("SAVEPOINT _ins_sp")
        try:
            c.execute(sql, params)
            c.execute("RELEASE SAVEPOINT _ins_sp")
            return True
        except Exception:
            c.execute("ROLLBACK TO SAVEPOINT _ins_sp")
            return False
    else:
        try:
            c.execute(sql, params)
            return True
        except sqlite3.IntegrityError:
            return False


def init_db() -> None:
    """Crea todas las tablas si no existen. Compatible con SQLite y PostgreSQL."""
    with get_connection() as conn:
        c = conn.cursor()

        # Usuarios
        c.execute(f"""
            CREATE TABLE IF NOT EXISTS users (
                id {_PK_AUTO},
                username TEXT UNIQUE NOT NULL,
                email TEXT,
                password_hash TEXT NOT NULL,
                role TEXT NOT NULL DEFAULT 'user',
                grok_enabled INTEGER NOT NULL DEFAULT 0,
                tier TEXT NOT NULL DEFAULT 'base',
                credits_balance INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL
            )
        """)
        _try_add_column(c, "users", "tier", "TEXT NOT NULL DEFAULT 'base'")
        _try_add_column(c, "users", "credits_balance", "INTEGER NOT NULL DEFAULT 0")

        # Partidos (cache de football-data + probs calculadas)
        c.execute("""
            CREATE TABLE IF NOT EXISTS matches (
                fixture_id INTEGER PRIMARY KEY,
                home_team TEXT,
                away_team TEXT,
                home_team_id INTEGER,
                away_team_id INTEGER,
                league_code TEXT,
                league_name TEXT,
                match_date_utc TEXT,
                status TEXT DEFAULT 'SCHEDULED',
                home_goals INTEGER,
                away_goals INTEGER,
                prob_home_win REAL,
                prob_draw REAL,
                prob_away_win REAL,
                expected_goals REAL,
                clean_sheet_home REAL,
                clean_sheet_away REAL,
                prob_btts REAL,
                prob_over25 REAL,
                raw_json TEXT,
                updated_at TEXT NOT NULL
            )
        """)
        c.execute("CREATE INDEX IF NOT EXISTS idx_matches_league ON matches(league_code)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_matches_date ON matches(match_date_utc)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_matches_status ON matches(status)")

        # Propuestas
        c.execute("""
            CREATE TABLE IF NOT EXISTS proposals (
                id TEXT PRIMARY KEY,
                user_id INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                pdf_path TEXT,
                email_sent_at TEXT,
                FOREIGN KEY (user_id) REFERENCES users(id)
            )
        """)

        # Partidos incluidos en cada propuesta
        c.execute("""
            CREATE TABLE IF NOT EXISTS proposal_matches (
                proposal_id TEXT NOT NULL,
                fixture_id INTEGER NOT NULL,
                risk_level TEXT NOT NULL,
                prediction TEXT,
                description_text TEXT,
                PRIMARY KEY (proposal_id, fixture_id, risk_level),
                FOREIGN KEY (proposal_id) REFERENCES proposals(id),
                FOREIGN KEY (fixture_id) REFERENCES matches(fixture_id)
            )
        """)
        _try_add_column(c, "proposal_matches", "details_json", "TEXT")
        _try_add_column(c, "proposals", "grok_analysis", "TEXT")
        _try_add_column(c, "proposals", "grok_stats_json", "TEXT")
        for col, typ in [
            ("evaluated_at", "TEXT"),
            ("tips_total", "INTEGER"),
            ("tips_fulfilled", "INTEGER"),
            ("accuracy_pct", "REAL"),
        ]:
            _try_add_column(c, "proposals", col, typ)
        _try_add_column(c, "proposals", "proposal_number", "INTEGER")

        # Backfill proposal_number para propuestas existentes sin número asignado
        c.execute("SELECT id FROM proposals WHERE proposal_number IS NULL ORDER BY created_at")
        existing = c.fetchall()
        if existing:
            c.execute("SELECT COALESCE(MAX(proposal_number), 999) FROM proposals")
            max_row = c.fetchone()
            next_num = (max_row[0] or 999) + 1
            for i, row in enumerate(existing):
                pid = row[0]
                c.execute("UPDATE proposals SET proposal_number = ? WHERE id = ?", (next_num + i, pid))

        # Resultado real del partido
        c.execute("""
            CREATE TABLE IF NOT EXISTS match_results (
                fixture_id INTEGER PRIMARY KEY,
                home_goals INTEGER NOT NULL,
                away_goals INTEGER NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (fixture_id) REFERENCES matches(fixture_id)
            )
        """)

        # Evaluación: por cada partido de la propuesta, si acertó o no
        c.execute("""
            CREATE TABLE IF NOT EXISTS proposal_outcomes (
                proposal_id TEXT NOT NULL,
                fixture_id INTEGER NOT NULL,
                risk_level TEXT NOT NULL,
                hit INTEGER NOT NULL,
                actual_home INTEGER NOT NULL,
                actual_away INTEGER NOT NULL,
                PRIMARY KEY (proposal_id, fixture_id, risk_level),
                FOREIGN KEY (proposal_id) REFERENCES proposals(id)
            )
        """)

        # Mercado Pago: suscripciones recurrentes
        c.execute(f"""
            CREATE TABLE IF NOT EXISTS mp_subscriptions (
                id {_PK_AUTO},
                user_id INTEGER NOT NULL,
                mp_preapproval_id TEXT UNIQUE NOT NULL,
                tier TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                payer_email TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users(id)
            )
        """)
        c.execute("CREATE INDEX IF NOT EXISTS idx_mp_sub_user ON mp_subscriptions(user_id)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_mp_sub_mpid ON mp_subscriptions(mp_preapproval_id)")

        # Mercado Pago: pagos únicos / packs de créditos
        c.execute(f"""
            CREATE TABLE IF NOT EXISTS mp_payments (
                id {_PK_AUTO},
                user_id INTEGER NOT NULL,
                mp_payment_id TEXT,
                mp_preference_id TEXT,
                external_reference TEXT UNIQUE NOT NULL,
                tier TEXT NOT NULL,
                credits INTEGER NOT NULL DEFAULT 0,
                amount REAL NOT NULL,
                currency_id TEXT NOT NULL DEFAULT 'CLP',
                status TEXT NOT NULL DEFAULT 'pending',
                created_at TEXT NOT NULL,
                updated_at TEXT,
                FOREIGN KEY (user_id) REFERENCES users(id)
            )
        """)
        c.execute("CREATE INDEX IF NOT EXISTS idx_mp_pay_user ON mp_payments(user_id)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_mp_pay_ext ON mp_payments(external_reference)")

        # Reportes de error de usuarios
        c.execute(f"""
            CREATE TABLE IF NOT EXISTS error_reports (
                id {_PK_AUTO},
                user_id INTEGER,
                username TEXT,
                created_at TEXT NOT NULL,
                context TEXT,
                error_message TEXT,
                comment TEXT,
                mode_or_screen TEXT,
                FOREIGN KEY (user_id) REFERENCES users(id)
            )
        """)
        c.execute("CREATE INDEX IF NOT EXISTS idx_error_reports_created ON error_reports(created_at)")

        # Historial rolling: últimas 3 temporadas + actual
        c.execute(f"""
            CREATE TABLE IF NOT EXISTS historical_matches (
                id {_PK_AUTO},
                fixture_id INTEGER UNIQUE NOT NULL,
                date TEXT NOT NULL,
                league_id TEXT NOT NULL,
                home_team_id INTEGER,
                away_team_id INTEGER,
                home_goals INTEGER,
                away_goals INTEGER,
                status TEXT NOT NULL DEFAULT 'FT',
                season INTEGER NOT NULL,
                home_xg REAL,
                away_xg REAL,
                created_at TEXT NOT NULL
            )
        """)
        c.execute("CREATE INDEX IF NOT EXISTS idx_historical_matches_fixture ON historical_matches(fixture_id)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_historical_matches_date ON historical_matches(date)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_historical_matches_league ON historical_matches(league_id)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_historical_matches_season ON historical_matches(season)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_historical_matches_home_team ON historical_matches(home_team_id)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_historical_matches_away_team ON historical_matches(away_team_id)")

        _try_add_column(c, "historical_matches", "api_sports_fixture_id", "INTEGER")
        for col in ("home_team_name", "away_team_name"):
            _try_add_column(c, "historical_matches", col, "TEXT")
        for col, typ in [
            ("kickoff_time", "TEXT"),
            ("ftr", "TEXT"),
            ("hthg", "INTEGER"),
            ("htag", "INTEGER"),
            ("htr", "TEXT"),
            ("attendance", "INTEGER"),
            ("referee", "TEXT"),
            ("home_shots", "INTEGER"), ("away_shots", "INTEGER"),
            ("home_shots_target", "INTEGER"), ("away_shots_target", "INTEGER"),
            ("home_corners", "INTEGER"), ("away_corners", "INTEGER"),
            ("home_fouls", "INTEGER"), ("away_fouls", "INTEGER"),
            ("home_yellow", "INTEGER"), ("away_yellow", "INTEGER"),
            ("home_red", "INTEGER"), ("away_red", "INTEGER"),
            ("home_offsides", "INTEGER"), ("away_offsides", "INTEGER"),
        ]:
            _try_add_column(c, "historical_matches", col, typ)

        # Clasificaciones oficiales
        c.execute("""
            CREATE TABLE IF NOT EXISTS league_standings (
                league_id TEXT NOT NULL,
                season INTEGER NOT NULL,
                rank INTEGER NOT NULL,
                team_name TEXT NOT NULL,
                team_id INTEGER,
                points INTEGER NOT NULL,
                wins INTEGER,
                draws INTEGER,
                losses INTEGER,
                PRIMARY KEY (league_id, season, rank)
            )
        """)
        c.execute("CREATE INDEX IF NOT EXISTS idx_league_standings_league_season ON league_standings(league_id, season)")

        # entity_aliases: normalización de nombres de equipos/ligas
        c.execute(f"""
            CREATE TABLE IF NOT EXISTS entity_aliases (
                id {_PK_AUTO},
                entity_type TEXT NOT NULL,
                canonical_name TEXT NOT NULL,
                alias TEXT NOT NULL,
                league_id TEXT,
                created_at TEXT NOT NULL,
                UNIQUE(entity_type, alias, league_id)
            )
        """)
        c.execute("CREATE INDEX IF NOT EXISTS idx_entity_aliases_type ON entity_aliases(entity_type)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_entity_aliases_alias ON entity_aliases(alias)")

        # master_table: staging con doble verificación
        c.execute(f"""
            CREATE TABLE IF NOT EXISTS master_table (
                id {_PK_AUTO},
                fixture_id INTEGER UNIQUE NOT NULL,
                date TEXT NOT NULL,
                league_id TEXT NOT NULL,
                home_team_id INTEGER,
                away_team_id INTEGER,
                home_team_name TEXT,
                away_team_name TEXT,
                home_goals INTEGER,
                away_goals INTEGER,
                status TEXT NOT NULL DEFAULT 'FT',
                season INTEGER NOT NULL,
                home_xg REAL,
                away_xg REAL,
                api_sports_fixture_id INTEGER,
                kickoff_time TEXT, ftr TEXT, hthg INTEGER, htag INTEGER, htr TEXT,
                attendance INTEGER, referee TEXT,
                home_shots INTEGER, away_shots INTEGER, home_shots_target INTEGER, away_shots_target INTEGER,
                home_corners INTEGER, away_corners INTEGER, home_fouls INTEGER, away_fouls INTEGER,
                home_yellow INTEGER, away_yellow INTEGER, home_red INTEGER, away_red INTEGER,
                home_offsides INTEGER, away_offsides INTEGER,
                created_at TEXT NOT NULL,
                source TEXT,
                verificado_1 INTEGER NOT NULL DEFAULT 0,
                verificado_1_at TEXT,
                verificado_2 INTEGER NOT NULL DEFAULT 0,
                verificado_2_at TEXT,
                verificado_2_by INTEGER
            )
        """)
        c.execute("CREATE INDEX IF NOT EXISTS idx_master_table_league_season ON master_table(league_id, season)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_master_table_fixture ON master_table(fixture_id)")

        # master_table_checked: solo datos con doble verificación (fuente para IA)
        c.execute(f"""
            CREATE TABLE IF NOT EXISTS master_table_checked (
                id {_PK_AUTO},
                fixture_id INTEGER UNIQUE NOT NULL,
                date TEXT NOT NULL,
                league_id TEXT NOT NULL,
                home_team_id INTEGER,
                away_team_id INTEGER,
                home_team_name TEXT,
                away_team_name TEXT,
                home_goals INTEGER,
                away_goals INTEGER,
                status TEXT NOT NULL DEFAULT 'FT',
                season INTEGER NOT NULL,
                home_xg REAL,
                away_xg REAL,
                api_sports_fixture_id INTEGER,
                kickoff_time TEXT, ftr TEXT, hthg INTEGER, htag INTEGER, htr TEXT,
                attendance INTEGER, referee TEXT,
                home_shots INTEGER, away_shots INTEGER, home_shots_target INTEGER, away_shots_target INTEGER,
                home_corners INTEGER, away_corners INTEGER, home_fouls INTEGER, away_fouls INTEGER,
                home_yellow INTEGER, away_yellow INTEGER, home_red INTEGER, away_red INTEGER,
                home_offsides INTEGER, away_offsides INTEGER,
                checked_at TEXT NOT NULL,
                checked_by INTEGER
            )
        """)
        c.execute("CREATE INDEX IF NOT EXISTS idx_master_checked_league_season ON master_table_checked(league_id, season)")

        # data_discrepancies: auditoría entre fuentes
        c.execute(f"""
            CREATE TABLE IF NOT EXISTS data_discrepancies (
                id {_PK_AUTO},
                entity_type TEXT NOT NULL,
                entity_id TEXT NOT NULL,
                field TEXT NOT NULL,
                value_source_a TEXT,
                value_source_b TEXT,
                source_a TEXT,
                source_b TEXT,
                status TEXT NOT NULL DEFAULT 'pending',
                resolved_value TEXT,
                resolved_at TEXT,
                resolved_by INTEGER,
                created_at TEXT NOT NULL,
                notes TEXT
            )
        """)
        c.execute("CREATE INDEX IF NOT EXISTS idx_data_discrepancies_status ON data_discrepancies(status)")
        _try_add_column(c, "data_discrepancies", "league_id", "TEXT")
        _try_add_column(c, "data_discrepancies", "season", "INTEGER")

        # data_completeness_log
        c.execute(f"""
            CREATE TABLE IF NOT EXISTS data_completeness_log (
                id {_PK_AUTO},
                checked_at TEXT NOT NULL,
                total_matches INTEGER NOT NULL,
                with_api_id INTEGER NOT NULL,
                max_date TEXT,
                sin_stats INTEGER NOT NULL DEFAULT 0,
                sin_offsides INTEGER NOT NULL DEFAULT 0,
                sin_attendance_referee INTEGER NOT NULL DEFAULT 0,
                sin_xg INTEGER NOT NULL DEFAULT 0,
                sin_cards INTEGER NOT NULL DEFAULT 0
            )
        """)
        c.execute("CREATE INDEX IF NOT EXISTS idx_data_completeness_log_checked ON data_completeness_log(checked_at)")

        # standings_override: clasificación corregida manualmente
        c.execute("""
            CREATE TABLE IF NOT EXISTS standings_override (
                league_id TEXT NOT NULL,
                season INTEGER NOT NULL,
                rank INTEGER NOT NULL,
                team_name TEXT NOT NULL,
                points INTEGER NOT NULL,
                wins INTEGER, draws INTEGER, losses INTEGER,
                PRIMARY KEY (league_id, season, rank)
            )
        """)

        # pending_team_aliases: candidatos a alias para resolución humana
        c.execute(f"""
            CREATE TABLE IF NOT EXISTS pending_team_aliases (
                id {_PK_AUTO},
                league_id TEXT NOT NULL,
                name_variant_1 TEXT NOT NULL,
                name_variant_2 TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                resolved_canonical TEXT,
                resolved_at TEXT,
                resolved_by INTEGER,
                created_at TEXT NOT NULL
            )
        """)
        c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_pending_team_aliases_pair ON pending_team_aliases(league_id, name_variant_1, name_variant_2)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_pending_team_aliases_status ON pending_team_aliases(status)")

        # Sesiones de usuario (token en URL para persistencia cross-reload)
        c.execute(f"""
            CREATE TABLE IF NOT EXISTS user_sessions (
                id {_PK_AUTO},
                token TEXT UNIQUE NOT NULL,
                user_id INTEGER NOT NULL,
                expires_at TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
        """)
        c.execute("CREATE INDEX IF NOT EXISTS idx_user_sessions_token ON user_sessions(token)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_user_sessions_user ON user_sessions(user_id)")


# ── Sesiones de usuario ──────────────────────────────────────────────────────

def create_user_session(user_id: int, days: int = 7) -> str:
    """Crea un token de sesión para el usuario. Devuelve el token (UUID)."""
    import uuid
    token = uuid.uuid4().hex
    now = datetime.utcnow()
    expires = (now + timedelta(days=days)).isoformat()
    with get_connection() as conn:
        c = conn.cursor()
        c.execute(
            "INSERT INTO user_sessions (token, user_id, expires_at, created_at) VALUES (?, ?, ?, ?)",
            (token, user_id, expires, now.isoformat()),
        )
    return token


def get_session_user(token: str) -> Optional[Dict[str, Any]]:
    """Devuelve los datos del usuario asociado al token si es válido y no expiró. None si inválido."""
    if not token:
        return None
    now = datetime.utcnow().isoformat()
    with get_connection() as conn:
        c = conn.cursor()
        c.execute(
            """SELECT u.id, u.username, u.email, u.role, u.grok_enabled, u.tier, u.credits_balance
               FROM user_sessions s
               JOIN users u ON u.id = s.user_id
               WHERE s.token = ? AND s.expires_at > ?""",
            (token, now),
        )
        row = c.fetchone()
        return dict(row) if row else None


def delete_user_session(token: str) -> None:
    """Elimina el token de sesión (logout)."""
    if not token:
        return
    with get_connection() as conn:
        c = conn.cursor()
        c.execute("DELETE FROM user_sessions WHERE token = ?", (token,))


def cleanup_expired_sessions() -> None:
    """Elimina sesiones expiradas (llamar desde cron o init)."""
    now = datetime.utcnow().isoformat()
    with get_connection() as conn:
        c = conn.cursor()
        c.execute("DELETE FROM user_sessions WHERE expires_at < ?", (now,))


def save_error_report(
    user_id: Optional[int] = None,
    username: Optional[str] = None,
    context: str = "",
    error_message: str = "",
    comment: str = "",
    mode_or_screen: str = "",
) -> int:
    """Guarda un reporte de error enviado por el usuario. Devuelve el id del reporte."""
    now = datetime.utcnow().isoformat()
    with get_connection() as conn:
        c = conn.cursor()
        c.execute(
            """INSERT INTO error_reports (user_id, username, created_at, context, error_message, comment, mode_or_screen)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (user_id, username or "", now, (context or "")[:500], (error_message or "")[:2000], (comment or "")[:2000], (mode_or_screen or "")[:200]),
        )
        return c.lastrowid or 0


def get_error_reports(limit: int = 50) -> List[Dict[str, Any]]:
    """Lista los últimos reportes de error (para administración)."""
    with get_connection() as conn:
        c = conn.cursor()
        c.execute(
            """SELECT id, user_id, username, created_at, context, error_message, comment, mode_or_screen
               FROM error_reports ORDER BY created_at DESC LIMIT ?""",
            (limit,),
        )
        return [dict(row) for row in c.fetchall()]


# ---------- Matches ----------

def upsert_match(m: Dict[str, Any]) -> None:
    """Inserta o actualiza un partido (datos + probs)."""
    raw = json.dumps(m) if isinstance(m.get("raw_json"), dict) else (m.get("raw_json") or "")
    with get_connection() as conn:
        c = conn.cursor()
        c.execute("""
            INSERT INTO matches (
                fixture_id, home_team, away_team, home_team_id, away_team_id,
                league_code, league_name, match_date_utc, status, home_goals, away_goals,
                prob_home_win, prob_draw, prob_away_win, expected_goals,
                clean_sheet_home, clean_sheet_away, prob_btts, prob_over25, raw_json, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(fixture_id) DO UPDATE SET
                home_team=excluded.home_team, away_team=excluded.away_team,
                home_team_id=excluded.home_team_id, away_team_id=excluded.away_team_id,
                league_code=excluded.league_code, league_name=excluded.league_name,
                match_date_utc=excluded.match_date_utc, status=excluded.status,
                home_goals=excluded.home_goals, away_goals=excluded.away_goals,
                prob_home_win=excluded.prob_home_win, prob_draw=excluded.prob_draw,
                prob_away_win=excluded.prob_away_win, expected_goals=excluded.expected_goals,
                clean_sheet_home=excluded.clean_sheet_home, clean_sheet_away=excluded.clean_sheet_away,
                prob_btts=excluded.prob_btts, prob_over25=excluded.prob_over25,
                raw_json=excluded.raw_json, updated_at=excluded.updated_at
        """, (
            m.get("fixture_id"),
            m.get("home_team"),
            m.get("away_team"),
            m.get("home_team_id"),
            m.get("away_team_id"),
            m.get("league_code") or m.get("league_id"),
            m.get("league_name"),
            m.get("match_date_utc") or m.get("date"),
            m.get("status", "SCHEDULED"),
            m.get("home_goals"),
            m.get("away_goals"),
            m.get("prob_home_win"),
            m.get("prob_draw"),
            m.get("prob_away_win"),
            m.get("expected_goals"),
            m.get("clean_sheet_home"),
            m.get("clean_sheet_away"),
            m.get("prob_btts"),
            m.get("prob_over25"),
            raw,
            datetime.utcnow().isoformat(),
        ))


def get_matches_by_fixture_ids(fixture_ids: List[int]) -> List[Dict[str, Any]]:
    """Devuelve partidos por lista de fixture_id (para usar cache en UI)."""
    if not fixture_ids:
        return []
    with get_connection() as conn:
        c = conn.cursor()
        placeholders = ",".join("?" * len(fixture_ids))
        c.execute(
            f"SELECT * FROM matches WHERE fixture_id IN ({placeholders}) ORDER BY match_date_utc",
            fixture_ids
        )
        return [_row_to_dict(r) for r in c.fetchall()]


def get_matches_today(league_codes: List[str]) -> List[Dict[str, Any]]:
    """Partidos de hoy desde BD (por league_code). Filtro por fecha actual."""
    from datetime import date
    return get_matches_by_date(league_codes, date.today().isoformat())


def get_matches_by_date(league_codes: List[str], target_date_iso: str) -> List[Dict[str, Any]]:
    """Partidos de una fecha dada (YYYY-MM-DD) desde BD.
    Nota: usa fecha UTC en BD. Para filtrar por fecha local (ej. Chile), usar get_matches_by_local_date."""
    with get_connection() as conn:
        c = conn.cursor()
        placeholders = ",".join("?" * len(league_codes))
        c.execute(
            f"""SELECT * FROM matches
                WHERE date(match_date_utc) = ? AND league_code IN ({placeholders})
                ORDER BY match_date_utc""",
            [target_date_iso] + list(league_codes)
        )
        return [_row_to_dict(r) for r in c.fetchall()]


def get_matches_by_local_date(
    league_codes: List[str],
    target_local_date: date,
    tz_str: str = "America/Santiago",
) -> List[Dict[str, Any]]:
    """Partidos cuya fecha/hora en la zona horaria dada cae en target_local_date.
    Corrige el bug de UTC vs local: partidos a las 21:00 Chile (18 feb) ya no aparecen en «Mañana»."""
    from zoneinfo import ZoneInfo

    tz = ZoneInfo(tz_str)
    start_local = datetime.combine(target_local_date, time.min).replace(tzinfo=tz)
    end_local = start_local + timedelta(days=1)
    start_utc = start_local.astimezone(timezone.utc)
    end_utc = end_local.astimezone(timezone.utc)
    start_str = start_utc.strftime("%Y-%m-%dT%H:%M:%S")
    end_str = end_utc.strftime("%Y-%m-%dT%H:%M:%S")
    # Normalizar para comparación: fechas con "Z" o "+00:00" deben ordenar bien (Z rompe el orden lexicográfico)
    date_expr = "REPLACE(REPLACE(COALESCE(match_date_utc,''), 'Z', ''), '+00:00', '')"

    with get_connection() as conn:
        c = conn.cursor()
        placeholders = ",".join("?" * len(league_codes))
        c.execute(
            f"""SELECT * FROM matches
                WHERE {date_expr} >= ? AND {date_expr} < ? AND league_code IN ({placeholders})
                ORDER BY {date_expr}""",
            [start_str, end_str] + list(league_codes)
        )
        rows = c.fetchall()

    result = []
    for row in rows:
        d = dict(row)
        mtc_str = d.get("match_date_utc") or ""
        if not mtc_str:
            continue
        try:
            s = mtc_str.strip().replace("Z", "+00:00")
            if "+" not in s and "T" in s:
                s = s + "+00:00"
            dt_utc = datetime.fromisoformat(s)
            if dt_utc.tzinfo is None:
                dt_utc = dt_utc.replace(tzinfo=timezone.utc)
            dt_local = dt_utc.astimezone(tz)
            if dt_local.date() == target_local_date:
                result.append(_row_to_dict(row))
        except Exception:
            pass
    return result


def get_upcoming_matches_from_db(league_codes: List[str], days_ahead: int = 3) -> List[Dict[str, Any]]:
    """Próximos partidos desde BD en el rango de días."""
    from datetime import date, timedelta
    today = date.today()
    end_date = today + timedelta(days=days_ahead)
    start_str = today.isoformat() + "T00:00:00"
    end_str = end_date.isoformat() + "T23:59:59"
    date_expr = "REPLACE(REPLACE(COALESCE(match_date_utc,''), 'Z', ''), '+00:00', '')"
    with get_connection() as conn:
        c = conn.cursor()
        placeholders = ",".join("?" * len(league_codes))
        c.execute(
            f"""SELECT * FROM matches
                WHERE {date_expr} >= ? AND {date_expr} <= ? AND league_code IN ({placeholders})
                AND (status = 'SCHEDULED' OR status IS NULL)
                ORDER BY {date_expr}""",
            [start_str, end_str] + list(league_codes)
        )
        return [_row_to_dict(r) for r in c.fetchall()]


def update_match_result(fixture_id: int, home_goals: int, away_goals: int) -> None:
    """Actualiza resultado de un partido (y tabla match_results)."""
    now = datetime.utcnow().isoformat()
    with get_connection() as conn:
        c = conn.cursor()
        c.execute(
            "UPDATE matches SET status = 'FINISHED', home_goals = ?, away_goals = ?, updated_at = ? WHERE fixture_id = ?",
            (home_goals, away_goals, now, fixture_id)
        )
        c.execute(
            "INSERT INTO match_results (fixture_id, home_goals, away_goals, updated_at) VALUES (?, ?, ?, ?) ON CONFLICT(fixture_id) DO UPDATE SET home_goals=?, away_goals=?, updated_at=?",
            (fixture_id, home_goals, away_goals, now, home_goals, away_goals, now)
        )


def _row_to_dict(row) -> Dict[str, Any]:
    d = dict(row)
    d.setdefault("home_team", d.get("home_team"))
    d.setdefault("away_team", d.get("away_team"))
    d.setdefault("date", d.get("match_date_utc"))
    d.setdefault("league_id", d.get("league_code"))
    d.setdefault("league_name", d.get("league_name"))
    return d


# ---------- Proposals ----------

def create_proposal(
    user_id: int,
    match_data: List[Dict],
    grok_analysis: str,
    grok_stats: Optional[Dict[int, Dict[str, Any]]] = None,
) -> str:
    """
    Crea una propuesta con ID único. Guarda análisis de Alfred, stats por fixture (opcional) y vincula partidos.
    grok_stats: opcional, {fixture_id: {prob_home_win, prob_draw, ...}} para rellenar la tabla.
    """
    proposal_id = str(uuid.uuid4())
    now = datetime.utcnow().isoformat()
    grok_stats_json = json.dumps(grok_stats) if grok_stats else None
    with get_connection() as conn:
        c = conn.cursor()
        c.execute("SELECT COALESCE(MAX(proposal_number), 999) + 1 FROM proposals")
        next_num = c.fetchone()[0]
        c.execute(
            "INSERT INTO proposals (id, user_id, created_at, grok_analysis, grok_stats_json, proposal_number) VALUES (?, ?, ?, ?, ?, ?)",
            (proposal_id, user_id, now, grok_analysis or "", grok_stats_json, next_num),
        )
        for m in match_data:
            fid = m.get("fixture_id")
            if fid is not None:
                c.execute(
                    """INSERT INTO proposal_matches (proposal_id, fixture_id, risk_level, prediction, description_text, details_json)
                       VALUES (?, ?, 'analysis', '', '', NULL)""",
                    (proposal_id, fid),
                )
    return proposal_id


def get_proposal(proposal_id: str) -> Optional[Dict[str, Any]]:
    """
    Devuelve propuesta por id (UUID) o por proposal_number (ej. "1000").
    Incluye grok_analysis, bets_by_risk y proposal_number.
    """
    with get_connection() as conn:
        c = conn.cursor()
        if proposal_id.isdigit():
            c.execute("SELECT * FROM proposals WHERE proposal_number = ?", (int(proposal_id),))
        else:
            c.execute("SELECT * FROM proposals WHERE id = ?", (proposal_id,))
        row = c.fetchone()
        if not row:
            return None
        prop = _row_to_dict(row)
        prop.setdefault("grok_analysis", "")
        if prop.get("grok_stats_json"):
            try:
                raw = json.loads(prop["grok_stats_json"])
                prop["grok_stats"] = {int(k): v for k, v in raw.items()}
            except (TypeError, ValueError, KeyError):
                prop["grok_stats"] = {}
        else:
            prop["grok_stats"] = {}
        c.execute("SELECT * FROM proposal_matches WHERE proposal_id = ?", (proposal_id,))
        rows = c.fetchall()
        by_risk = {}
        for r in rows:
            rdict = dict(r)
            if rdict.get("details_json"):
                try:
                    rdict["details"] = json.loads(rdict["details_json"])
                except Exception:
                    rdict["details"] = {}
            else:
                rdict["details"] = {}
            lev = rdict["risk_level"]
            if lev not in by_risk:
                by_risk[lev] = []
            by_risk[lev].append(rdict)
        prop["bets_by_risk"] = by_risk
        return prop


def get_proposals_for_history() -> List[Dict[str, Any]]:
    """
    Lista todas las propuestas para historial: por día, con ID numérico.
    Orden: created_at desc. Incluye id, proposal_number, created_at, tips_total, accuracy_pct y count de partidos.
    """
    with get_connection() as conn:
        c = conn.cursor()
        c.execute(
            """SELECT p.id, p.proposal_number, p.created_at, p.tips_total, p.accuracy_pct,
                      (SELECT COUNT(*) FROM proposal_matches pm WHERE pm.proposal_id = p.id) AS match_count
               FROM proposals p
               ORDER BY p.created_at DESC"""
        )
        rows = c.fetchall()
    cols = ["id", "proposal_number", "created_at", "tips_total", "accuracy_pct", "match_count"]
    return [dict(zip(cols, (r[0], r[1], r[2], r[3], r[4], r[5]))) for r in rows]


def set_proposal_pdf(proposal_id: str, pdf_path: str) -> None:
    with get_connection() as conn:
        conn.execute("UPDATE proposals SET pdf_path = ? WHERE id = ?", (pdf_path, proposal_id))


def set_proposal_email_sent(proposal_id: str) -> None:
    with get_connection() as conn:
        conn.execute("UPDATE proposals SET email_sent_at = ? WHERE id = ?", (datetime.utcnow().isoformat(), proposal_id))


# ---------- Evaluación (comparar predicción vs resultado) ----------

def _result_to_1x2(home_goals: int, away_goals: int) -> str:
    if home_goals > away_goals:
        return "1"
    if away_goals > home_goals:
        return "2"
    return "X"


def _value_bet_to_set(value_bet: Any) -> set:
    """Convierte value_bet ('1', 'X', '2', '1,X', etc.) a set {'1','X','2'}."""
    if value_bet is None:
        return set()
    s = str(value_bet).strip().replace(",", " ").replace(";", " ")
    return {x.strip() for x in s.split() if x.strip() in ("1", "X", "2")}


def evaluate_proposals() -> int:
    """
    Para cada propuesta con partidos ya finalizados, compara predicción vs resultado real.
    Rellena proposal_outcomes. Devuelve número de propuestas evaluadas.
    """
    with get_connection() as conn:
        c = conn.cursor()
        c.execute("SELECT id, grok_stats_json FROM proposals")
        rows = c.fetchall()
        evaluated = 0
        for row in rows:
            pid = row[0]
            grok_stats_json = row[1]
            c.execute("SELECT DISTINCT fixture_id FROM proposal_matches WHERE proposal_id = ?", (pid,))
            fids = [r[0] for r in c.fetchall()]
            if not fids:
                continue
            c.execute(
                "SELECT fixture_id, home_goals, away_goals FROM match_results WHERE fixture_id IN (" + ",".join("?" * len(fids)) + ")",
                fids
            )
            results = {r[0]: (r[1], r[2]) for r in c.fetchall()}
            if not results:
                continue
            # 1) Outcomes por risk_level (poco, moderado, arriesgada)
            c.execute("SELECT fixture_id, risk_level, prediction FROM proposal_matches WHERE proposal_id = ?", (pid,))
            for fid, risk_level, prediction in c.fetchall():
                if fid not in results or risk_level not in ("poco", "moderado", "arriesgada"):
                    continue
                h, a = results[fid]
                actual_1x2 = _result_to_1x2(h, a)
                pred_norm = (prediction or "").strip().upper()
                if pred_norm in ("1", "LOCAL", "VICTORIA LOCAL", "HOME"):
                    pred_1x2 = "1"
                elif pred_norm in ("2", "VISITANTE", "VICTORIA VISITANTE", "AWAY"):
                    pred_1x2 = "2"
                elif pred_norm in ("X", "EMPATE", "DRAW"):
                    pred_1x2 = "X"
                else:
                    pred_1x2 = pred_norm[:1] if pred_norm else ""
                hit = 1 if pred_1x2 == actual_1x2 else 0
                c.execute(
                    "INSERT OR REPLACE INTO proposal_outcomes (proposal_id, fixture_id, risk_level, hit, actual_home, actual_away) VALUES (?, ?, ?, ?, ?, ?)",
                    (pid, fid, risk_level, hit, h, a)
                )
            # 2) Fase 1: acierto por propuesta completa (value_bet de grok_stats)
            grok_stats = {}
            if grok_stats_json:
                try:
                    grok_stats = json.loads(grok_stats_json)
                    grok_stats = {int(k): v for k, v in grok_stats.items()}
                except (TypeError, ValueError, KeyError):
                    pass
            tips_total = 0
            tips_fulfilled = 0
            for fid in fids:
                if fid not in results:
                    continue
                h, a = results[fid]
                actual_1x2 = _result_to_1x2(h, a)
                vb_set = _value_bet_to_set((grok_stats.get(fid) or {}).get("value_bet"))
                if not vb_set:
                    continue
                tips_total += 1
                tips_fulfilled += 1 if actual_1x2 in vb_set else 0
                c.execute(
                    "INSERT OR REPLACE INTO proposal_outcomes (proposal_id, fixture_id, risk_level, hit, actual_home, actual_away) VALUES (?, ?, ?, ?, ?, ?)",
                    (pid, fid, "1x2", 1 if actual_1x2 in vb_set else 0, h, a)
                )
            if tips_total > 0:
                accuracy_pct = round(100.0 * tips_fulfilled / tips_total, 1)
                now = datetime.utcnow().isoformat()
                c.execute(
                    "UPDATE proposals SET evaluated_at = ?, tips_total = ?, tips_fulfilled = ?, accuracy_pct = ? WHERE id = ?",
                    (now, tips_total, tips_fulfilled, accuracy_pct, pid)
                )
            evaluated += 1
        return evaluated


def get_match_result(fixture_id: int) -> Optional[tuple]:
    """Devuelve (home_goals, away_goals) del partido si existe en match_results, si no None."""
    with get_connection() as conn:
        c = conn.cursor()
        c.execute("SELECT home_goals, away_goals FROM match_results WHERE fixture_id = ?", (fixture_id,))
        row = c.fetchone()
        return (row[0], row[1]) if row else None


def get_proposal_outcomes(proposal_id: str) -> Dict[tuple, Dict[str, Any]]:
    """
    Devuelve por cada (fixture_id, risk_level) el resultado real y si acertó.
    Clave: (fixture_id, risk_level). Valor: { "actual_home", "actual_away", "actual_1x2", "hit" }.
    """
    with get_connection() as conn:
        c = conn.cursor()
        c.execute(
            "SELECT fixture_id, risk_level, hit, actual_home, actual_away FROM proposal_outcomes WHERE proposal_id = ?",
            (proposal_id,),
        )
        out = {}
        for r in c.fetchall():
            fid, lev, hit, ah, aa = r
            actual_1x2 = _result_to_1x2(ah, aa)
            out[(fid, lev)] = {"actual_home": ah, "actual_away": aa, "actual_1x2": actual_1x2, "hit": hit}
        return out


def get_matches_by_fixture_ids(fixture_ids: List[int]) -> List[Dict[str, Any]]:
    """Devuelve datos de partidos (home_team, away_team, league_name, etc.) por fixture_id para mostrar en búsqueda."""
    if not fixture_ids:
        return []
    with get_connection() as conn:
        c = conn.cursor()
        placeholders = ",".join("?" * len(fixture_ids))
        c.execute(
            f"SELECT fixture_id, home_team, away_team, league_name, league_code, match_date_utc, status, home_goals, away_goals FROM matches WHERE fixture_id IN ({placeholders})",
            fixture_ids,
        )
        cols = ["fixture_id", "home_team", "away_team", "league_name", "league_code", "match_date_utc", "status", "home_goals", "away_goals"]
        return [dict(zip(cols, row)) for row in c.fetchall()]


def get_accuracy_stats() -> Dict[str, Any]:
    """
    Nivel de acierto: por propuesta completa y por partidos en general.
    Devuelve: { "proposals": { "total", "hits", "pct" }, "by_risk": { "poco": {...}, "moderado": {...}, "arriesgada": {...} }, "matches_evaluated": N }
    """
    with get_connection() as conn:
        c = conn.cursor()
        c.execute("SELECT proposal_id, risk_level, hit FROM proposal_outcomes")
        rows = c.fetchall()
    total_bets = len(rows)
    hits = sum(r[2] for r in rows)
    by_risk = {}
    for r in rows:
        lev = r[1]
        if lev not in by_risk:
            by_risk[lev] = {"total": 0, "hits": 0}
        by_risk[lev]["total"] += 1
        by_risk[lev]["hits"] += r[2]
    for k in by_risk:
        t = by_risk[k]["total"]
        by_risk[k]["pct"] = round(100 * by_risk[k]["hits"] / t, 1) if t else 0
    return {
        "proposals": {"total": total_bets, "hits": hits, "pct": round(100 * hits / total_bets, 1) if total_bets else 0},
        "by_risk": by_risk,
        "matches_evaluated": total_bets,
    }


# ---------- Mercado Pago ----------

def upsert_mp_subscription(
    user_id: int,
    mp_preapproval_id: str,
    tier: str,
    status: str,
    payer_email: Optional[str] = None,
) -> None:
    """Inserta o actualiza suscripción MP."""
    now = datetime.utcnow().isoformat()
    with get_connection() as conn:
        c = conn.cursor()
        c.execute(
            """INSERT INTO mp_subscriptions (user_id, mp_preapproval_id, tier, status, payer_email, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(mp_preapproval_id) DO UPDATE SET status = ?, updated_at = ?, payer_email = COALESCE(?, payer_email)""",
            (user_id, mp_preapproval_id, tier, status, payer_email or "", now, now, status, now, payer_email),
        )


def update_mp_subscription_status(mp_preapproval_id: str, status: str) -> bool:
    """Actualiza el estado de una suscripción."""
    now = datetime.utcnow().isoformat()
    with get_connection() as conn:
        c = conn.cursor()
        c.execute(
            "UPDATE mp_subscriptions SET status = ?, updated_at = ? WHERE mp_preapproval_id = ?",
            (status, now, mp_preapproval_id),
        )
        return c.rowcount > 0


def get_mp_subscription_by_preapproval_id(mp_preapproval_id: str) -> Optional[Dict[str, Any]]:
    """Devuelve la suscripción por ID de Mercado Pago."""
    with get_connection() as conn:
        c = conn.cursor()
        c.execute(
            "SELECT * FROM mp_subscriptions WHERE mp_preapproval_id = ?",
            (mp_preapproval_id,),
        )
        row = c.fetchone()
        return dict(row) if row else None


def upsert_mp_payment(
    user_id: int,
    external_reference: str,
    tier: str,
    credits: int,
    amount: float,
    currency_id: str = "CLP",
    status: str = "pending",
    mp_payment_id: Optional[str] = None,
    mp_preference_id: Optional[str] = None,
) -> None:
    """Inserta o actualiza un pago MP. external_reference debe ser único por intención de pago."""
    now = datetime.utcnow().isoformat()
    pid = mp_payment_id or ""
    pref = mp_preference_id or ""
    with get_connection() as conn:
        c = conn.cursor()
        c.execute(
            """INSERT INTO mp_payments (user_id, mp_payment_id, mp_preference_id, external_reference, tier, credits, amount, currency_id, status, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(external_reference) DO UPDATE SET
                   mp_payment_id = CASE WHEN ? != '' THEN ? ELSE mp_payments.mp_payment_id END,
                   mp_preference_id = CASE WHEN ? != '' THEN ? ELSE mp_payments.mp_preference_id END,
                   status = ?,
                   updated_at = ?""",
            (user_id, pid, pref, external_reference, tier, credits, amount, currency_id, status, now, now, pid, pid, pref, pref, status, now),
        )


def get_mp_payment_by_external_reference(external_reference: str) -> Optional[Dict[str, Any]]:
    """Devuelve el pago por external_reference."""
    with get_connection() as conn:
        c = conn.cursor()
        c.execute("SELECT * FROM mp_payments WHERE external_reference = ?", (external_reference,))
        row = c.fetchone()
        return dict(row) if row else None


# ---------- Historical matches (rolling window) ----------

def upsert_historical_match(
    fixture_id: int,
    date: str,
    league_id: str,
    home_goals: int,
    away_goals: int,
    season: int,
    status: str = "FT",
    home_team_id: Optional[int] = None,
    away_team_id: Optional[int] = None,
    home_xg: Optional[float] = None,
    away_xg: Optional[float] = None,
    home_team_name: Optional[str] = None,
    away_team_name: Optional[str] = None,
    *,
    api_sports_fixture_id: Optional[int] = None,
    kickoff_time: Optional[str] = None,
    ftr: Optional[str] = None,
    hthg: Optional[int] = None,
    htag: Optional[int] = None,
    htr: Optional[str] = None,
    attendance: Optional[int] = None,
    referee: Optional[str] = None,
    home_shots: Optional[int] = None,
    away_shots: Optional[int] = None,
    home_shots_target: Optional[int] = None,
    away_shots_target: Optional[int] = None,
    home_corners: Optional[int] = None,
    away_corners: Optional[int] = None,
    home_fouls: Optional[int] = None,
    away_fouls: Optional[int] = None,
    home_yellow: Optional[int] = None,
    away_yellow: Optional[int] = None,
    home_red: Optional[int] = None,
    away_red: Optional[int] = None,
    home_offsides: Optional[int] = None,
    away_offsides: Optional[int] = None,
) -> None:
    """Inserta o actualiza un partido histórico. Idempotente por fixture_id."""
    now = datetime.utcnow().isoformat()
    with get_connection() as conn:
        c = conn.cursor()
        try:
            c.execute(
                """INSERT INTO historical_matches (
                    fixture_id, date, league_id, home_team_id, away_team_id,
                    home_goals, away_goals, status, season, home_xg, away_xg,
                    home_team_name, away_team_name, api_sports_fixture_id,
                    kickoff_time, ftr, hthg, htag, htr, attendance, referee,
                    home_shots, away_shots, home_shots_target, away_shots_target,
                    home_corners, away_corners, home_fouls, away_fouls,
                    home_yellow, away_yellow, home_red, away_red,
                    home_offsides, away_offsides, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(fixture_id) DO UPDATE SET
                    date=excluded.date, league_id=excluded.league_id,
                    home_team_id=excluded.home_team_id, away_team_id=excluded.away_team_id,
                    home_goals=excluded.home_goals, away_goals=excluded.away_goals,
                    status=excluded.status, season=excluded.season,
                    home_xg=excluded.home_xg, away_xg=excluded.away_xg,
                    home_team_name=excluded.home_team_name, away_team_name=excluded.away_team_name,
                    api_sports_fixture_id=COALESCE(excluded.api_sports_fixture_id, historical_matches.api_sports_fixture_id),
                    kickoff_time=excluded.kickoff_time, ftr=excluded.ftr,
                    hthg=excluded.hthg, htag=excluded.htag, htr=excluded.htr,
                    attendance=excluded.attendance, referee=excluded.referee,
                    home_shots=excluded.home_shots, away_shots=excluded.away_shots,
                    home_shots_target=excluded.home_shots_target, away_shots_target=excluded.away_shots_target,
                    home_corners=excluded.home_corners, away_corners=excluded.away_corners,
                    home_fouls=excluded.home_fouls, away_fouls=excluded.away_fouls,
                    home_yellow=excluded.home_yellow, away_yellow=excluded.away_yellow,
                    home_red=excluded.home_red, away_red=excluded.away_red,
                    home_offsides=excluded.home_offsides, away_offsides=excluded.away_offsides
                """,
                (
                    fixture_id, date, league_id, home_team_id, away_team_id,
                    home_goals, away_goals, status, season, home_xg, away_xg,
                    home_team_name or "", away_team_name or "", api_sports_fixture_id,
                    kickoff_time, ftr, hthg, htag, htr, attendance, referee,
                    home_shots, away_shots, home_shots_target, away_shots_target,
                    home_corners, away_corners, home_fouls, away_fouls,
                    home_yellow, away_yellow, home_red, away_red,
                    home_offsides, away_offsides, now,
                ),
            )
        except Exception:
            try:
                c.execute(
                    """INSERT INTO historical_matches (
                        fixture_id, date, league_id, home_team_id, away_team_id,
                        home_goals, away_goals, status, season, home_xg, away_xg,
                        home_team_name, away_team_name, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(fixture_id) DO UPDATE SET
                        date=excluded.date, league_id=excluded.league_id,
                        home_team_id=excluded.home_team_id, away_team_id=excluded.away_team_id,
                        home_goals=excluded.home_goals, away_goals=excluded.away_goals,
                        status=excluded.status, season=excluded.season,
                        home_xg=excluded.home_xg, away_xg=excluded.away_xg,
                        home_team_name=excluded.home_team_name, away_team_name=excluded.away_team_name
                    """,
                    (
                        fixture_id, date, league_id, home_team_id, away_team_id,
                        home_goals, away_goals, status, season, home_xg, away_xg,
                        home_team_name or "", away_team_name or "", now,
                    ),
                )
            except Exception:
                c.execute(
                    """INSERT INTO historical_matches (
                        fixture_id, date, league_id, home_team_id, away_team_id,
                        home_goals, away_goals, status, season, home_xg, away_xg, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(fixture_id) DO UPDATE SET
                        date=excluded.date, league_id=excluded.league_id,
                        home_team_id=excluded.home_team_id, away_team_id=excluded.away_team_id,
                        home_goals=excluded.home_goals, away_goals=excluded.away_goals,
                        status=excluded.status, season=excluded.season,
                        home_xg=excluded.home_xg, away_xg=excluded.away_xg
                    """,
                    (
                        fixture_id, date, league_id, home_team_id, away_team_id,
                        home_goals, away_goals, status, season, home_xg, away_xg, now,
                    ),
                )


def count_historical_matches_before_season(cutoff_season: int) -> int:
    """Cuenta partidos con season < cutoff_season (para --dry-run). No borra."""
    with get_connection() as conn:
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM historical_matches WHERE season < ?", (cutoff_season,))
        return c.fetchone()[0] or 0


def get_historical_match_seasons_before(cutoff_season: int) -> List[Tuple[int, int]]:
    """Devuelve [(season, count), ...] para season < cutoff (para reporte dry-run)."""
    with get_connection() as conn:
        c = conn.cursor()
        c.execute(
            "SELECT season, COUNT(*) FROM historical_matches WHERE season < ? GROUP BY season ORDER BY season",
            (cutoff_season,),
        )
        return list(c.fetchall())


def delete_historical_matches_with_season_before(cutoff_season: int) -> int:
    """Elimina partidos con season < cutoff_season. Devuelve número de filas borradas."""
    with get_connection() as conn:
        c = conn.cursor()
        c.execute("DELETE FROM historical_matches WHERE season < ?", (cutoff_season,))
        return c.rowcount


def backfill_cl_el_api_sports_fixture_id() -> int:
    """
    Rellena api_sports_fixture_id en partidos de Champions League (CL) y Europa League (EL)
    cuando es NULL y fixture_id está en rango 9xx (origen API-Sports).
    Fórmula: api_sports_fixture_id = fixture_id - 900_000_000 (válido cuando el ID real < 99_999_999).
    Permite que run_backfill_statistics_from_apisports y run_backfill_cards puedan rellenar
    tiros a puerta, córners, faltas, árbitro, HT/FT. Devuelve número de filas actualizadas.
    """
    with get_connection() as conn:
        c = conn.cursor()
        c.execute(
            """
            UPDATE historical_matches
            SET api_sports_fixture_id = fixture_id - 900000000
            WHERE league_id IN ('CL', 'EL')
              AND api_sports_fixture_id IS NULL
              AND fixture_id >= 900000000
            """
        )
        return c.rowcount


def backfill_ftr_from_goals() -> int:
    """
    Rellena ftr (H/D/A) en historical_matches cuando está vacío, a partir de home_goals/away_goals.
    Útil tras cargar CL/EL desde API-Sports (que no envían FTR). Devuelve número de filas actualizadas.
    """
    with get_connection() as conn:
        c = conn.cursor()
        c.execute(
            """
            UPDATE historical_matches
            SET ftr = CASE
                WHEN home_goals > away_goals THEN 'H'
                WHEN home_goals < away_goals THEN 'A'
                ELSE 'D'
            END
            WHERE (ftr IS NULL OR ftr = '')
              AND home_goals IS NOT NULL
              AND away_goals IS NOT NULL
            """
        )
        return c.rowcount


def fix_historical_malformed_dates(dry_run: bool = False) -> int:
    """
    Corrige fechas mal parseadas (2093 → 1993, 2094 → 1994, etc.) por bug en año 2 dígitos.
    Fechas 209x-xx-xx se convierten a 199x-xx-xx.
    dry_run=True: no actualiza; devuelve número de filas afectadas.
    """
    with get_connection() as conn:
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM historical_matches WHERE date LIKE '209%'")
        count = c.fetchone()[0]
        if count == 0:
            return 0
        if dry_run:
            return count
        c.execute(
            "UPDATE historical_matches SET date = '19' || SUBSTR(date, 3) WHERE date LIKE '209%'"
        )
        return c.rowcount


def update_historical_cards_bulk(rows: List[Dict[str, Any]]) -> int:
    """
    Actualiza tarjetas amarillas/rojas en lote desde filas con fixture_id, home_yellow, away_yellow, home_red, away_red.
    Devuelve número de filas actualizadas. Útil tras parsear CSV cuando el upsert no actualiza columnas extra.
    """
    count = 0
    with get_connection() as conn:
        c = conn.cursor()
        for r in rows:
            fid = r.get("fixture_id")
            if fid is None:
                continue
            hy = r.get("home_yellow") if r.get("home_yellow") is not None else None
            ay = r.get("away_yellow") if r.get("away_yellow") is not None else None
            hr = r.get("home_red") if r.get("home_red") is not None else None
            ar = r.get("away_red") if r.get("away_red") is not None else None
            if hy is None and ay is None and hr is None and ar is None:
                continue
            c.execute(
                """UPDATE historical_matches SET
                       home_yellow = COALESCE(?, home_yellow),
                       away_yellow = COALESCE(?, away_yellow),
                       home_red = COALESCE(?, home_red),
                       away_red = COALESCE(?, away_red)
                   WHERE fixture_id = ?""",
                (hy, ay, hr, ar, fid),
            )
            if c.rowcount > 0:
                count += 1
    return count


def update_historical_match_cards(
    fixture_id: int,
    home_yellow: Optional[int] = None,
    away_yellow: Optional[int] = None,
    home_red: Optional[int] = None,
    away_red: Optional[int] = None,
) -> bool:
    """Actualiza tarjetas amarillas/rojas de un partido histórico. Devuelve True si se actualizó."""
    with get_connection() as conn:
        c = conn.cursor()
        c.execute(
            """UPDATE historical_matches SET
                   home_yellow = COALESCE(?, home_yellow),
                   away_yellow = COALESCE(?, away_yellow),
                   home_red = COALESCE(?, home_red),
                   away_red = COALESCE(?, away_red)
               WHERE fixture_id = ?""",
            (home_yellow, away_yellow, home_red, away_red, fixture_id),
        )
        return c.rowcount > 0


def update_historical_attendance_referee(
    fixture_id: int,
    attendance: Optional[int] = None,
    referee: Optional[str] = None,
    api_sports_fixture_id: Optional[int] = None,
    home_team_id: Optional[int] = None,
    away_team_id: Optional[int] = None,
) -> bool:
    """Actualiza asistencia, árbitro, api_sports_fixture_id y opcionalmente home/away_team_id."""
    with get_connection() as conn:
        c = conn.cursor()
        c.execute(
            """UPDATE historical_matches SET
                   attendance = COALESCE(?, attendance),
                   referee = COALESCE(?, referee),
                   api_sports_fixture_id = COALESCE(?, api_sports_fixture_id),
                   home_team_id = COALESCE(?, home_team_id),
                   away_team_id = COALESCE(?, away_team_id)
               WHERE fixture_id = ?""",
            (attendance, referee, api_sports_fixture_id, home_team_id, away_team_id, fixture_id),
        )
        return c.rowcount > 0


def update_historical_statistics(
    fixture_id: int,
    home_shots: Optional[int] = None,
    away_shots: Optional[int] = None,
    home_shots_target: Optional[int] = None,
    away_shots_target: Optional[int] = None,
    home_corners: Optional[int] = None,
    away_corners: Optional[int] = None,
    home_fouls: Optional[int] = None,
    away_fouls: Optional[int] = None,
    home_offsides: Optional[int] = None,
    away_offsides: Optional[int] = None,
) -> bool:
    """Actualiza estadísticas de partido (shots, corners, fouls, offsides) desde API-Sports /statistics."""
    with get_connection() as conn:
        c = conn.cursor()
        c.execute(
            """UPDATE historical_matches SET
                   home_shots = COALESCE(?, home_shots),
                   away_shots = COALESCE(?, away_shots),
                   home_shots_target = COALESCE(?, home_shots_target),
                   away_shots_target = COALESCE(?, away_shots_target),
                   home_corners = COALESCE(?, home_corners),
                   away_corners = COALESCE(?, away_corners),
                   home_fouls = COALESCE(?, home_fouls),
                   away_fouls = COALESCE(?, away_fouls),
                   home_offsides = COALESCE(?, home_offsides),
                   away_offsides = COALESCE(?, away_offsides)
               WHERE fixture_id = ?""",
            (
                home_shots, away_shots, home_shots_target, away_shots_target,
                home_corners, away_corners, home_fouls, away_fouls,
                home_offsides, away_offsides, fixture_id,
            ),
        )
        return c.rowcount > 0


def get_distinct_league_season_for_backfill(
    only_missing: bool = True,
) -> List[Tuple[str, int]]:
    """
    Devuelve (league_id, season) distintos para hacer backfill de attendance/referee.
    Si only_missing=True, solo pares donde hay filas con attendance NULL o referee vacío.
    """
    with get_connection() as conn:
        c = conn.cursor()
        if only_missing:
            c.execute(
                """SELECT DISTINCT league_id, season FROM historical_matches
                   WHERE (attendance IS NULL OR referee IS NULL OR referee = ''
                          OR home_team_id IS NULL OR away_team_id IS NULL)
                   ORDER BY league_id, season DESC"""
            )
        else:
            c.execute(
                """SELECT DISTINCT league_id, season FROM historical_matches
                   ORDER BY league_id, season DESC"""
            )
        return [(r[0], r[1]) for r in c.fetchall()]


def get_historical_match_rows_for_backfill(
    league_id: str,
    season: int,
) -> List[Dict[str, Any]]:
    """
    Filas de historical_matches para una liga y temporada (para emparejar con API-Sports).
    Devuelve [{"fixture_id", "date", "home_team_name", "away_team_name"}, ...].
    """
    with get_connection() as conn:
        c = conn.cursor()
        c.execute(
            """SELECT fixture_id, date, home_team_name, away_team_name
               FROM historical_matches
               WHERE league_id = ? AND season = ?
               ORDER BY date""",
            (league_id, season),
        )
        return [
            {"fixture_id": r[0], "date": r[1], "home_team_name": r[2], "away_team_name": r[3]}
            for r in c.fetchall()
        ]


def get_historical_matches_pending_attendance_referee_backfill(
    limit: int = 100,
) -> List[Dict[str, Any]]:
    """
    Partidos con api_sports_fixture_id que aún no tienen attendance o referee.
    Para backfill desde API-Sports GET /fixtures?id=.
    """
    with get_connection() as conn:
        c = conn.cursor()
        c.execute(
            """SELECT fixture_id, api_sports_fixture_id
               FROM historical_matches
               WHERE api_sports_fixture_id IS NOT NULL
                 AND (attendance IS NULL OR referee IS NULL OR referee = '')
               ORDER BY season DESC, date DESC
               LIMIT ?""",
            (limit,),
        )
        return [{"fixture_id": r[0], "api_sports_fixture_id": r[1]} for r in c.fetchall()]


def get_historical_matches_pending_statistics_backfill(limit: int = 50) -> List[Dict[str, Any]]:
    """
    Partidos con api_sports_fixture_id + home_team_id + away_team_id que no tienen estadísticas
    (shots, corners, fouls, offsides). Para backfill desde API-Sports /statistics.
    """
    with get_connection() as conn:
        c = conn.cursor()
        c.execute(
            """SELECT fixture_id, api_sports_fixture_id, home_team_id, away_team_id
               FROM historical_matches
               WHERE api_sports_fixture_id IS NOT NULL
                 AND home_team_id IS NOT NULL
                 AND away_team_id IS NOT NULL
                 AND home_shots IS NULL
               ORDER BY season DESC, date DESC
               LIMIT ?""",
            (limit,),
        )
        return [
            {"fixture_id": r[0], "api_sports_fixture_id": r[1], "home_team_id": r[2], "away_team_id": r[3]}
            for r in c.fetchall()
        ]


def get_historical_matches_for_football_data_enrichment(
    limit: int = 100,
    league_codes: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """
    Partidos con estadísticas vacías o 0-0 para intentar rellenar desde football-data.org.
    Incluye filas con o sin api_sports_fixture_id (cualquier liga soportada por FD).
    league_codes: ej. ["CL", "PL", "PD", "SA", "BL1", "FL1"]. None = todas las ligas FD.
    Devuelve fixture_id, league_id, date, home_team_name, away_team_name.
    """
    default_leagues = ("CL", "PL", "PD", "SA", "BL1", "FL1", "DED", "PPL", "EL")
    leagues = tuple(league_codes) if league_codes else default_leagues
    placeholders = ",".join("?" * len(leagues))
    with get_connection() as conn:
        c = conn.cursor()
        c.execute(
            f"""
            SELECT fixture_id, league_id, date, home_team_name, away_team_name
            FROM historical_matches
            WHERE league_id IN ({placeholders})
              AND (home_team_name IS NOT NULL OR away_team_name IS NOT NULL)
              AND date IS NOT NULL AND date != ''
              AND (
                (home_shots_target IS NULL AND away_shots_target IS NULL)
                OR (COALESCE(home_shots_target, 0) = 0 AND COALESCE(away_shots_target, 0) = 0)
              )
            ORDER BY date DESC
            LIMIT ?
            """,
            (*leagues, limit),
        )
        rows = c.fetchall()
    return [
        {
            "fixture_id": r[0],
            "league_id": r[1],
            "date": r[2],
            "home_team_name": r[3] or "",
            "away_team_name": r[4] or "",
        }
        for r in rows
    ]


def get_historical_match_for_fallback(fixture_id: int) -> Optional[Dict[str, Any]]:
    """
    Devuelve league_id, date, home_team_name, away_team_name para un fixture_id.
    Para usar en fallback a football-data.org cuando API-Sports devuelve 0-0.
    """
    with get_connection() as conn:
        c = conn.cursor()
        c.execute(
            """SELECT league_id, date, home_team_name, away_team_name
               FROM historical_matches WHERE fixture_id = ?""",
            (fixture_id,),
        )
        row = c.fetchone()
    if not row:
        return None
    return {
        "league_id": row[0],
        "date": row[1],
        "home_team_name": row[2] or "",
        "away_team_name": row[3] or "",
    }


def get_historical_matches_zero_stats_retry(limit: int = 50) -> List[Dict[str, Any]]:
    """
    Partidos que tienen tiros a puerta 0 y 0 (ambos equipos), para reintentar una vez
    la obtención de estadísticas desde API-Sports.
    """
    with get_connection() as conn:
        c = conn.cursor()
        c.execute(
            """SELECT fixture_id, api_sports_fixture_id, home_team_id, away_team_id
               FROM historical_matches
               WHERE api_sports_fixture_id IS NOT NULL
                 AND home_team_id IS NOT NULL
                 AND away_team_id IS NOT NULL
                 AND COALESCE(home_shots_target, 0) = 0
                 AND COALESCE(away_shots_target, 0) = 0
               ORDER BY date DESC
               LIMIT ?""",
            (limit,),
        )
        return [
            {"fixture_id": r[0], "api_sports_fixture_id": r[1], "home_team_id": r[2], "away_team_id": r[3]}
            for r in c.fetchall()
        ]


def get_historical_matches_pending_card_backfill(limit: int = 50) -> List[Dict[str, Any]]:
    """
    Partidos con api_sports_fixture_id que aún no tienen tarjetas (home_yellow IS NULL o 0).
    Para backfill desde API-Sports /statistics.
    """
    with get_connection() as conn:
        c = conn.cursor()
        c.execute(
            """SELECT fixture_id, api_sports_fixture_id, home_team_id, away_team_id
               FROM historical_matches
               WHERE api_sports_fixture_id IS NOT NULL
                 AND home_team_id IS NOT NULL
                 AND away_team_id IS NOT NULL
                 AND (home_yellow IS NULL OR away_yellow IS NULL)
               ORDER BY season DESC, date DESC
               LIMIT ?""",
            (limit,),
        )
        rows = c.fetchall()
    return [
        {
            "fixture_id": r[0],
            "api_sports_fixture_id": r[1],
            "home_team_id": r[2],
            "away_team_id": r[3],
        }
        for r in rows
    ]


def get_historical_match_seasons() -> List[int]:
    """Devuelve lista de temporadas presentes en historical_matches (para diagnóstico)."""
    with get_connection() as conn:
        c = conn.cursor()
        c.execute("SELECT DISTINCT season FROM historical_matches ORDER BY season")
        return [r[0] for r in c.fetchall()]


_TEAM_SUFFIX_RE = None

def _core_team_name(name: str) -> str:
    """
    Extrae el nombre "popular" del equipo quitando prefijos y sufijos futbolísticos comunes.
    'Real Madrid CF' → 'Real Madrid', 'AC Pisa 1909' → 'Pisa', 'Sport Lisboa e Benfica' → 'Benfica'
    """
    import re
    n = name.strip()
    n = re.sub(r"^(AC|ACF|AFC|SC|FC|AS|SS|SV|SD|UD|RC|RB|SL|GD|CD|VfB|VfL|IF|BK|FK|SK|TSV|FSV|MSV|KSC|BSC|Sporting|Sport\s+Lisboa\s+e)\s+", "", n, flags=re.IGNORECASE)
    n = re.sub(r"\s+(FC|CF|AC|SC|SD|UD|RC|Calcio|1\d{3}|\d{4})\s*$", "", n, flags=re.IGNORECASE)
    n = n.strip()
    return n if len(n) >= 3 else name.strip()


def _team_name_candidates(name: str) -> list:
    """
    Genera una lista de variantes del nombre para fuzzy matching, de más específica a más general.
    'Birmingham City FC' → ['Birmingham City FC', 'Birmingham City', 'Birmingham']
    'Real Madrid CF'     → ['Real Madrid CF', 'Real Madrid']
    """
    seen = []
    def _add(n):
        n = n.strip()
        if n and n not in seen:
            seen.append(n)
    import re
    _add(name)
    core = _core_team_name(name)
    _add(core)
    # Quitar también palabras de ubicación/tipo comunes como City, United, Athletic
    core2 = re.sub(r"\s+(City|United|Athletic|Atletico|County|Town|Rovers|Wanderers|Albion|Rangers|Villa|Palace|Forest|Wednesday|Thursday|Saturday)\s*$", "", core, flags=re.IGNORECASE).strip()
    _add(core2)
    # Primera palabra si tiene >= 5 caracteres
    parts = core2.split() if core2 else core.split()
    if parts and len(parts[0]) >= 5:
        _add(parts[0])
    return seen


def get_historical_matches_for_team(
    team_id: Optional[int] = None,
    team_name: Optional[str] = None,
    league_id: Optional[str] = None,
    last_n: int = 10,
) -> List[Dict[str, Any]]:
    """
    Partidos históricos donde el equipo participó (local o visitante).
    Busca por team_id (API-Sports) o por team_name (CSV/exact, luego fuzzy).
    Orden: más reciente primero.
    """
    _SEL = """SELECT fixture_id, date, league_id, home_team_id, away_team_id,
                     home_goals, away_goals, season, home_team_name, away_team_name
              FROM historical_matches"""

    def _build_output(rows, tid, tname):
        out = []
        for r in rows:
            d = dict(r)
            home_id = d.get("home_team_id")
            is_home = (tid is not None and home_id == tid) or (
                tname and d.get("home_team_name", "").lower() == tname.lower()
            )
            # fuzzy fallback: si no coincide exacto, heurística por posición
            if not is_home and tname:
                hn = d.get("home_team_name", "").lower()
                core = _core_team_name(tname).lower()
                is_home = core in hn or tname.lower() in hn
            gf = d["home_goals"] if is_home else d["away_goals"]
            ga = d["away_goals"] if is_home else d["home_goals"]
            out.append({"date": d["date"], "goals_for": gf, "goals_against": ga,
                        "league_id": d["league_id"], "season": d["season"]})
        return out

    with get_connection() as conn:
        c = conn.cursor()
        if team_id is not None:
            c.execute(
                f"{_SEL} WHERE (home_team_id = ? OR away_team_id = ?) ORDER BY date DESC LIMIT ?",
                (team_id, team_id, last_n),
            )
            rows = c.fetchall()
            if rows:
                return _build_output(rows, team_id, team_name)
            # fallback por nombre si team_id no encuentra nada
            if not team_name:
                return []

        if team_name and league_id:
            candidates = _team_name_candidates(team_name)
            # Primero exacto con liga, luego fuzzy con liga, luego sin liga
            for candidate in candidates:
                pattern = f"%{candidate}%"
                # Exacto con liga
                c.execute(
                    f"{_SEL} WHERE league_id = ? AND (home_team_name = ? OR away_team_name = ?) ORDER BY date DESC LIMIT ?",
                    (league_id, candidate, candidate, last_n),
                )
                rows = c.fetchall()
                if rows:
                    return _build_output(rows, None, team_name)
                # ILIKE con liga
                c.execute(
                    f"{_SEL} WHERE league_id = ? AND (home_team_name ILIKE ? OR away_team_name ILIKE ?) ORDER BY date DESC LIMIT ?",
                    (league_id, pattern, pattern, last_n),
                )
                rows = c.fetchall()
                if rows:
                    return _build_output(rows, None, team_name)

            # Fallback sin liga (league_id puede diferir entre fuentes)
            for candidate in candidates:
                pattern = f"%{candidate}%"
                c.execute(
                    f"{_SEL} WHERE (home_team_name ILIKE ? OR away_team_name ILIKE ?) ORDER BY date DESC LIMIT ?",
                    (pattern, pattern, last_n),
                )
                rows = c.fetchall()
                if rows:
                    return _build_output(rows, None, team_name)

        return []


def get_historical_matches_for_team_with_stats(
    team_id: Optional[int] = None,
    team_name: Optional[str] = None,
    league_id: Optional[str] = None,
    last_n: int = 10,
) -> List[Dict[str, Any]]:
    """
    Partidos históricos del equipo con columnas para análisis enriquecido V3:
    tiros a puerta, córners, faltas, árbitro, resultado al descanso (hthg, htag, htr), tarjetas.
    Cada fila incluye is_home y las stats del equipo en ese partido (shots_target_team, corners_team, fouls_team, etc.).
    """
    cols = (
        "fixture_id, date, league_id, home_team_id, away_team_id, home_goals, away_goals, season, home_team_name, away_team_name, "
        "home_shots_target, away_shots_target, home_corners, away_corners, home_fouls, away_fouls, "
        "referee, hthg, htag, htr, home_yellow, away_yellow"
    )
    with get_connection() as conn:
        c = conn.cursor()
        try:
            if team_id is not None:
                c.execute(
                    f"""SELECT {cols} FROM historical_matches
                       WHERE (home_team_id = ? OR away_team_id = ?)
                       ORDER BY date DESC LIMIT ?""",
                    (team_id, team_id, last_n),
                )
            elif team_name and league_id:
                c.execute(
                    f"""SELECT {cols} FROM historical_matches
                       WHERE league_id = ? AND (home_team_name = ? OR away_team_name = ?)
                       ORDER BY date DESC LIMIT ?""",
                    (league_id, team_name, team_name, last_n),
                )
            else:
                return []
        except Exception:
            return []
        rows = c.fetchall()
    out = []
    for r in rows:
        d = dict(r)
        home_id = d.get("home_team_id")
        away_id = d.get("away_team_id")
        is_home = (team_id is not None and home_id == team_id) or (team_name and d.get("home_team_name") == team_name)
        shots_target_team = (d.get("home_shots_target") if is_home else d.get("away_shots_target")) if d.get("home_shots_target") is not None or d.get("away_shots_target") is not None else None
        corners_team = (d.get("home_corners") if is_home else d.get("away_corners")) if d.get("home_corners") is not None or d.get("away_corners") is not None else None
        fouls_team = (d.get("home_fouls") if is_home else d.get("away_fouls")) if d.get("home_fouls") is not None or d.get("away_fouls") is not None else None
        yellow_team = (d.get("home_yellow") if is_home else d.get("away_yellow")) if d.get("home_yellow") is not None or d.get("away_yellow") is not None else None
        hthg, htag, htr = d.get("hthg"), d.get("htag"), d.get("htr")
        goals_at_ht_team = (hthg if is_home else htag) if (hthg is not None or htag is not None) else None
        out.append({
            "date": d["date"],
            "goals_for": d["home_goals"] if is_home else d["away_goals"],
            "goals_against": d["away_goals"] if is_home else d["home_goals"],
            "league_id": d["league_id"],
            "season": d.get("season"),
            "is_home": is_home,
            "shots_target_team": shots_target_team,
            "corners_team": corners_team,
            "fouls_team": fouls_team,
            "yellow_team": yellow_team,
            "referee": d.get("referee"),
            "hthg": hthg,
            "htag": htag,
            "htr": htr,
            "goals_at_ht_team": goals_at_ht_team,
        })
    return out


def get_referee_avg_cards(referee_name: Optional[str], league_id: Optional[str], last_n: int = 30) -> Optional[Dict[str, float]]:
    """
    Promedio de tarjetas amarillas y rojas por partido de un árbitro en la liga (historical_matches).
    Devuelve {"avg_yellow", "avg_red", "matches"} o None si no hay datos.
    """
    if not referee_name or not referee_name.strip():
        return None
    with get_connection() as conn:
        c = conn.cursor()
        try:
            if league_id:
                c.execute(
                    """SELECT home_yellow, away_yellow, home_red, away_red
                       FROM historical_matches
                       WHERE referee = ? AND league_id = ?
                       ORDER BY date DESC LIMIT ?""",
                    (referee_name.strip(), league_id, last_n),
                )
            else:
                c.execute(
                    """SELECT home_yellow, away_yellow, home_red, away_red
                       FROM historical_matches
                       WHERE referee = ?
                       ORDER BY date DESC LIMIT ?""",
                    (referee_name.strip(), last_n),
                )
            rows = c.fetchall()
        except Exception:
            return None
    if not rows:
        return None
    total_y = total_r = 0
    for r in rows:
        d = dict(r)
        total_y += (d.get("home_yellow") or 0) + (d.get("away_yellow") or 0)
        total_r += (d.get("home_red") or 0) + (d.get("away_red") or 0)
    n = len(rows)
    return {"avg_yellow": round(total_y / n, 2), "avg_red": round(total_r / n, 2), "matches": n}


def get_historical_h2h(
    home_id: Optional[int] = None,
    away_id: Optional[int] = None,
    home_name: Optional[str] = None,
    away_name: Optional[str] = None,
    league_id: Optional[str] = None,
    last_n: int = 8,
) -> List[Dict[str, Any]]:
    """Enfrentamientos directos desde historical_matches. Por IDs o por nombres. Fuzzy fallback si exacto falla."""
    _SEL = "SELECT date, home_team_name, away_team_name, home_goals, away_goals FROM historical_matches"

    def _rows_to_h2h(rows):
        return [{"date": r[0], "home_team_name": r[1], "away_team_name": r[2],
                 "home_goals": r[3], "away_goals": r[4]} for r in rows]

    with get_connection() as conn:
        c = conn.cursor()
        if home_id is not None and away_id is not None:
            c.execute(
                f"{_SEL} WHERE ((home_team_id = ? AND away_team_id = ?) OR (home_team_id = ? AND away_team_id = ?)) ORDER BY date DESC LIMIT ?",
                (home_id, away_id, away_id, home_id, last_n),
            )
            rows = c.fetchall()
            if rows:
                return _rows_to_h2h(rows)
            if not (home_name and away_name):
                return []
            # fallback a nombre si IDs no coinciden

        if home_name and away_name:
            # 1. Exacto con liga
            if league_id:
                c.execute(
                    f"{_SEL} WHERE league_id = ? AND ((home_team_name = ? AND away_team_name = ?) OR (home_team_name = ? AND away_team_name = ?)) ORDER BY date DESC LIMIT ?",
                    (league_id, home_name, away_name, away_name, home_name, last_n),
                )
                rows = c.fetchall()
                if rows:
                    return _rows_to_h2h(rows)

            # 2. Exacto sin liga
            c.execute(
                f"{_SEL} WHERE (home_team_name = ? AND away_team_name = ?) OR (home_team_name = ? AND away_team_name = ?) ORDER BY date DESC LIMIT ?",
                (home_name, away_name, away_name, home_name, last_n),
            )
            rows = c.fetchall()
            if rows:
                return _rows_to_h2h(rows)

            # 3. Fuzzy cascada con candidatos de nombre
            home_cands = _team_name_candidates(home_name)
            away_cands = _team_name_candidates(away_name)
            for hc in home_cands:
                for ac in away_cands:
                    hp = f"%{hc}%"
                    ap = f"%{ac}%"
                    c.execute(
                        f"{_SEL} WHERE (home_team_name ILIKE ? AND away_team_name ILIKE ?) OR (home_team_name ILIKE ? AND away_team_name ILIKE ?) ORDER BY date DESC LIMIT ?",
                        (hp, ap, ap, hp, last_n),
                    )
                    rows = c.fetchall()
                    if rows:
                        return _rows_to_h2h(rows)
            return []

        return []


def get_historical_matches_for_team_from_master_checked(
    team_id: Optional[int] = None,
    team_name: Optional[str] = None,
    league_id: Optional[str] = None,
    last_n: int = 10,
) -> List[Dict[str, Any]]:
    """
    Forma reciente desde master_table_checked (datos verificados). Mismo formato que get_historical_matches_for_team.
    Por team_id o team_name+league_id.
    """
    if team_id is None and not (team_name and league_id):
        return []
    with get_connection() as conn:
        c = conn.cursor()
        if team_id is not None:
            c.execute(
                """SELECT fixture_id, date, league_id, home_team_id, away_team_id,
                          home_goals, away_goals, season, home_team_name, away_team_name
                   FROM master_table_checked
                   WHERE (home_team_id = ? OR away_team_id = ?)
                   ORDER BY date DESC LIMIT ?""",
                (team_id, team_id, last_n),
            )
        else:
            c.execute(
                """SELECT fixture_id, date, league_id, home_team_id, away_team_id,
                          home_goals, away_goals, season, home_team_name, away_team_name
                   FROM master_table_checked
                   WHERE league_id = ? AND (home_team_name = ? OR away_team_name = ?)
                   ORDER BY date DESC LIMIT ?""",
                (league_id, team_name, team_name, last_n),
            )
        rows = c.fetchall()
    out = []
    for r in rows:
        d = dict(r)
        home_id = d.get("home_team_id")
        away_id = d.get("away_team_id")
        is_home = (team_id is not None and home_id == team_id) or (team_name and d.get("home_team_name") == team_name)
        gf = d["home_goals"] if is_home else d["away_goals"]
        ga = d["away_goals"] if is_home else d["home_goals"]
        out.append({"date": d["date"], "goals_for": gf, "goals_against": ga, "league_id": d["league_id"], "season": d["season"]})
    return out


def get_historical_h2h_from_master_checked(
    home_id: Optional[int] = None,
    away_id: Optional[int] = None,
    home_name: Optional[str] = None,
    away_name: Optional[str] = None,
    league_id: Optional[str] = None,
    last_n: int = 8,
) -> List[Dict[str, Any]]:
    """Enfrentamientos directos desde master_table_checked (datos verificados). Mismo formato que get_historical_h2h."""
    with get_connection() as conn:
        c = conn.cursor()
        if home_id is not None and away_id is not None:
            c.execute(
                """SELECT date, home_team_name, away_team_name, home_goals, away_goals FROM master_table_checked
                   WHERE ((home_team_id = ? AND away_team_id = ?) OR (home_team_id = ? AND away_team_id = ?))
                   ORDER BY date DESC LIMIT ?""",
                (home_id, away_id, away_id, home_id, last_n),
            )
        elif home_name and away_name and league_id:
            c.execute(
                """SELECT date, home_team_name, away_team_name, home_goals, away_goals FROM master_table_checked
                   WHERE league_id = ? AND (
                     (home_team_name = ? AND away_team_name = ?) OR (home_team_name = ? AND away_team_name = ?)
                   ) ORDER BY date DESC LIMIT ?""",
                (league_id, home_name, away_name, away_name, home_name, last_n),
            )
        else:
            return []
        rows = c.fetchall()
    return [
        {"date": r[0], "home_team_name": r[1], "away_team_name": r[2], "home_goals": r[3], "away_goals": r[4]}
        for r in rows
    ]


def get_historical_league_goals(league_id: str, seasons: Optional[List[int]] = None) -> List[Dict[str, Any]]:
    """Por cada partido de la liga (y temporadas opcionales): home_goals, away_goals, season. Para lambda bias."""
    with get_connection() as conn:
        c = conn.cursor()
        if seasons:
            placeholders = ",".join("?" * len(seasons))
            c.execute(
                f"""SELECT home_goals, away_goals, season FROM historical_matches
                    WHERE league_id = ? AND season IN ({placeholders})""",
                [league_id] + list(seasons),
            )
        else:
            c.execute(
                "SELECT home_goals, away_goals, season FROM historical_matches WHERE league_id = ?",
                (league_id,),
            )
        rows = c.fetchall()
    return [{"home_goals": r[0], "away_goals": r[1], "season": r[2]} for r in rows]


def get_referee_losses_for_team(
    team_names: List[str],
    league_id: str,
    seasons: List[int],
    limit: int = 10,
) -> List[Dict[str, Any]]:
    """
    Árbitros con más derrotas del equipo en las temporadas dadas.
    team_names: ej. ["Milan", "AC Milan"] para matchear nombre en BD.
    FTR: H=home win, D=draw, A=away win. Derrota = (local y FTR=A) o (visitante y FTR=H).
    Devuelve [{"referee": str, "losses": int}, ...] ordenado por losses DESC.
    """
    if not team_names or not seasons:
        return []
    placeholders_s = ",".join("?" * len(seasons))
    placeholders_n = ",".join("?" * len(team_names))
    with get_connection() as conn:
        c = conn.cursor()
        c.execute(
            f"""SELECT referee, COUNT(*) AS losses
                FROM historical_matches
                WHERE league_id = ? AND season IN ({placeholders_s})
                  AND referee IS NOT NULL AND referee != ''
                  AND (
                    (ftr = 'A' AND home_team_name IN ({placeholders_n}))
                    OR (ftr = 'H' AND away_team_name IN ({placeholders_n}))
                  )
                GROUP BY referee
                ORDER BY losses DESC
                LIMIT ?""",
            [league_id] + list(seasons) + list(team_names) + list(team_names) + [limit],
        )
        rows = c.fetchall()
    return [{"referee": r[0], "losses": r[1]} for r in rows]


def get_top_teams_by_attendance(
    league_id: str,
    limit: int = 3,
    seasons: Optional[List[int]] = None,
) -> List[Dict[str, Any]]:
    """
    Top equipos de la liga por asistencia total (como local) en partidos con attendance no nulo.
    Devuelve [{"team_name": str, "total_attendance": int, "matches": int}, ...] ordenado por total_attendance DESC.
    """
    with get_connection() as conn:
        c = conn.cursor()
        if seasons:
            placeholders = ",".join("?" * len(seasons))
            c.execute(
                f"""SELECT home_team_name, SUM(attendance) AS total, COUNT(*) AS matches
                    FROM historical_matches
                    WHERE league_id = ? AND season IN ({placeholders})
                      AND attendance IS NOT NULL AND attendance > 0
                    GROUP BY home_team_name
                    ORDER BY total DESC
                    LIMIT ?""",
                [league_id] + list(seasons) + [limit],
            )
        else:
            c.execute(
                """SELECT home_team_name, SUM(attendance) AS total, COUNT(*) AS matches
                   FROM historical_matches
                   WHERE league_id = ? AND attendance IS NOT NULL AND attendance > 0
                   GROUP BY home_team_name
                   ORDER BY total DESC
                   LIMIT ?""",
                (league_id, limit),
            )
        rows = c.fetchall()
    return [
        {"team_name": r[0], "total_attendance": int(r[1]), "matches": r[2]}
        for r in rows
    ]


def get_total_attendance_for_league_season(
    league_id: str,
    season: int,
) -> Optional[int]:
    """
    Asistencia total de la liga en la temporada (suma de attendance de todos los partidos con dato).
    season = año fin (ej. 2022 = 2021/22). Devuelve None si no hay datos.
    """
    with get_connection() as conn:
        c = conn.cursor()
        c.execute(
            """SELECT SUM(attendance) FROM historical_matches
               WHERE league_id = ? AND season = ? AND attendance IS NOT NULL AND attendance > 0""",
            (league_id, season),
        )
        row = c.fetchone()
    if row and row[0] is not None:
        return int(row[0])
    return None


def get_referee_match_counts_by_league(
    league_id: str,
    season: Optional[int] = None,
    limit: int = 20,
) -> List[Dict[str, Any]]:
    """
    Árbitros que más partidos dirigieron en la liga (y opcionalmente temporada).
    Devuelve [{"referee": str, "matches": int}, ...] ordenado por matches DESC.
    """
    with get_connection() as conn:
        c = conn.cursor()
        if season is not None:
            c.execute(
                """SELECT referee, COUNT(*) AS matches
                   FROM historical_matches
                   WHERE league_id = ? AND season = ? AND referee IS NOT NULL AND referee != ''
                   GROUP BY referee ORDER BY matches DESC LIMIT ?""",
                (league_id, season, limit),
            )
        else:
            c.execute(
                """SELECT referee, COUNT(*) AS matches
                   FROM historical_matches
                   WHERE league_id = ? AND referee IS NOT NULL AND referee != ''
                   GROUP BY referee ORDER BY matches DESC LIMIT ?""",
                (league_id, limit),
            )
        rows = c.fetchall()
    return [{"referee": r[0], "matches": r[1]} for r in rows]


def get_matches_between_teams(
    league_id: str,
    team_a_names: List[str],
    team_b_names: List[str],
    season: int,
) -> List[Dict[str, Any]]:
    """
    Partidos entre dos conjuntos de nombres (ej. Inter vs Milan para Derby della Madonnina).
    team_a_names y team_b_names: listas de variantes de nombre (Inter, FC Internazionale, etc.).
    Devuelve [{"date": str, "home_team_name": str, "away_team_name": str, "referee": str|None}, ...].
    """
    if not team_a_names or not team_b_names:
        return []
    ph_a = ",".join("?" * len(team_a_names))
    ph_b = ",".join("?" * len(team_b_names))
    with get_connection() as conn:
        c = conn.cursor()
        c.execute(
            f"""SELECT date, home_team_name, away_team_name, referee
                FROM historical_matches
                WHERE league_id = ? AND season = ?
                  AND (
                    (home_team_name IN ({ph_a}) AND away_team_name IN ({ph_b}))
                    OR (home_team_name IN ({ph_b}) AND away_team_name IN ({ph_a}))
                  )
                ORDER BY date""",
            [league_id, season] + list(team_a_names) + list(team_b_names) + list(team_b_names) + list(team_a_names),
        )
        rows = c.fetchall()
    return [
        {"date": r[0], "home_team_name": r[1], "away_team_name": r[2], "referee": r[3]}
        for r in rows
    ]


def get_team_season_wins(team_name: str, league_id: str, season: int) -> int:
    """Número de victorias de un equipo en una temporada (FTR: H=local gana, A=visitante gana)."""
    with get_connection() as conn:
        c = conn.cursor()
        c.execute(
            """SELECT COUNT(*) FROM historical_matches
               WHERE league_id = ? AND season = ?
                 AND ((ftr = 'H' AND home_team_name = ?) OR (ftr = 'A' AND away_team_name = ?))""",
            (league_id, season, team_name, team_name),
        )
        row = c.fetchone()
    return (row[0] or 0) if row else 0


def replace_league_standings(
    league_id: str, season: int, rows: List[Dict[str, Any]]
) -> int:
    """
    Reemplaza la clasificación oficial de una liga/temporada. Fuente: API-Sports u otra.
    rows: [{"rank": int, "team_name": str, "points": int, "wins"?: int, "draws"?: int, "losses"?: int, "team_id"?: int}, ...]
    Devuelve el número de filas insertadas.
    """
    if not rows:
        return 0
    with get_connection() as conn:
        c = conn.cursor()
        c.execute(
            "DELETE FROM league_standings WHERE league_id = ? AND season = ?",
            (league_id, season),
        )
        for r in rows:
            rank = int(r.get("rank", 0))
            team_name = (r.get("team_name") or "").strip()
            points = int(r.get("points", 0))
            wins = r.get("wins")
            draws = r.get("draws")
            losses = r.get("losses")
            team_id = r.get("team_id")
            c.execute(
                """INSERT INTO league_standings (league_id, season, rank, team_name, team_id, points, wins, draws, losses)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    league_id,
                    season,
                    rank,
                    team_name,
                    team_id,
                    points,
                    wins if wins is not None else 0,
                    draws if draws is not None else 0,
                    losses if losses is not None else 0,
                ),
            )
        return len(rows)


def _compute_standings_from_historical_matches(
    league_id: str, season: int
) -> List[Dict[str, Any]]:
    """
    Calcula clasificación desde historical_matches (FTR, goles).
    Fallback cuando league_standings está vacía.
    Devuelve mismo formato que get_league_standings: team_name, points, wins, draws, losses.
    """
    with get_connection() as conn:
        c = conn.cursor()
        c.execute(
            """
            SELECT home_team_name, away_team_name, ftr, home_goals, away_goals
            FROM historical_matches
            WHERE league_id = ? AND season = ? AND (ftr = 'H' OR ftr = 'D' OR ftr = 'A')
            """,
            (league_id, season),
        )
        rows = c.fetchall()

    pts: Dict[str, int] = {}
    gf: Dict[str, int] = {}
    gc: Dict[str, int] = {}
    w: Dict[str, int] = {}
    d: Dict[str, int] = {}
    l: Dict[str, int] = {}

    def _init(t: str) -> None:
        if t:
            pts.setdefault(t, 0)
            gf.setdefault(t, 0)
            gc.setdefault(t, 0)
            w.setdefault(t, 0)
            d.setdefault(t, 0)
            l.setdefault(t, 0)

    for home, away, ftr, hg, ag in rows:
        _init(home)
        _init(away)
        gf[home] = gf.get(home, 0) + hg
        gc[home] = gc.get(home, 0) + ag
        gf[away] = gf.get(away, 0) + ag
        gc[away] = gc.get(away, 0) + hg
        if ftr == "H":
            pts[home] += 3
            w[home] += 1
            l[away] += 1
        elif ftr == "A":
            pts[away] += 3
            w[away] += 1
            l[home] += 1
        else:
            pts[home] += 1
            pts[away] += 1
            d[home] += 1
            d[away] += 1

    sorted_teams = sorted(
        pts.items(),
        key=lambda x: (-x[1], -(gf.get(x[0], 0) - gc.get(x[0], 0))),
    )
    return [
        {
            "team_name": team,
            "points": p,
            "wins": w.get(team, 0),
            "draws": d.get(team, 0),
            "losses": l.get(team, 0),
        }
        for team, p in sorted_teams
    ]


def get_league_standings(league_id: str, season: int) -> List[Dict[str, Any]]:
    """
    Clasificación de la liga/temporada. Usa historical_matches (football-data.co.uk) como fuente principal:
    season = año fin de la temporada (ej. 2022 = 2021/22). Si no hay datos, usa league_standings (API-Sports).
    Formato: [{"team_name": str, "points": int, "wins": int, "draws": int, "losses": int}, ...] ordenado por rank.
    """
    computed = _compute_standings_from_historical_matches(league_id, season)
    if computed:
        return computed
    with get_connection() as conn:
        c = conn.cursor()
        c.execute(
            """SELECT rank, team_name, points, wins, draws, losses
               FROM league_standings
               WHERE league_id = ? AND season = ?
               ORDER BY rank""",
            (league_id, season),
        )
        rows = c.fetchall()
    if rows:
        return [
            {
                "team_name": r[1],
                "points": int(r[2]),
                "wins": int(r[3] or 0),
                "draws": int(r[4] or 0),
                "losses": int(r[5] or 0),
            }
            for r in rows
        ]
    return []


# ---------- entity_aliases (normalización antes de master_table) ----------


def _load_entity_aliases_cache() -> Dict[str, str]:
    """Carga entity_aliases en un dict (alias_normalized -> canonical) para evitar queries por fila."""
    cache: Dict[str, str] = {}
    with get_connection() as conn:
        c = conn.cursor()
        c.execute(
            """SELECT LOWER(TRIM(alias)), canonical_name, league_id FROM entity_aliases WHERE entity_type = 'team'"""
        )
        for alias_lower, canonical, lid in c.fetchall():
            cache[f"{lid or ''}|{alias_lower}"] = canonical
            if f"|{alias_lower}" not in cache:
                cache[f"|{alias_lower}"] = canonical
    return cache


def normalize_team_name(team_name: Optional[str], league_id: Optional[str] = None, _cache: Optional[Dict[str, str]] = None) -> str:
    """
    Devuelve el nombre canónico del equipo usando entity_aliases.
    Si no hay alias, devuelve el nombre original (o "" si None).
    _cache: dict pre-cargado para evitar queries (usado por copy_historical_to_master).
    """
    if not team_name or not str(team_name).strip():
        return ""
    name = str(team_name).strip()
    alias_lower = name.lower().strip()
    if _cache is not None:
        key = f"{league_id or ''}|{alias_lower}"
        if key in _cache:
            return _cache[key]
        if league_id:
            gen_key = f"|{alias_lower}"
            if gen_key in _cache:
                return _cache[gen_key]
        return name
    with get_connection() as conn:
        c = conn.cursor()
        for lid in (league_id,) if league_id else (None,):
            c.execute(
                """SELECT canonical_name FROM entity_aliases
                   WHERE entity_type = 'team' AND LOWER(TRIM(alias)) = LOWER(TRIM(?))
                   AND (league_id IS NULL OR league_id = ?)""",
                (name, lid or ""),
            )
            row = c.fetchone()
            if row:
                return str(row[0])
    return name


def add_entity_alias(
    entity_type: str,
    canonical_name: str,
    alias: str,
    league_id: Optional[str] = None,
) -> None:
    """Añade un alias para normalización (team o league)."""
    now = datetime.utcnow().isoformat()
    with get_connection() as conn:
        c = conn.cursor()
        c.execute(
            """INSERT OR IGNORE INTO entity_aliases (entity_type, canonical_name, alias, league_id, created_at)
               VALUES (?, ?, ?, ?, ?)""",
            (entity_type, canonical_name.strip(), alias.strip(), league_id, now),
        )


# ---------- pending_team_aliases (normalización con control humano) ----------


def get_pending_team_aliases() -> List[Dict[str, Any]]:
    """Lista candidatos a alias pendientes de resolución (para UI Admin)."""
    with get_connection() as conn:
        c = conn.cursor()
        c.execute(
            """SELECT id, league_id, name_variant_1, name_variant_2, status, resolved_canonical, created_at
               FROM pending_team_aliases WHERE status = 'pending' ORDER BY league_id, created_at"""
        )
        return [dict(row) for row in c.fetchall()]


def resolve_team_alias(
    pending_id: int,
    canonical_name: str,
    resolved_by: Optional[int] = None,
) -> Tuple[bool, str]:
    """
    Resuelve un pending_team_alias: añade el alias a entity_aliases y marca como resuelto.
    canonical_name debe ser uno de name_variant_1 o name_variant_2; el otro se registra como alias.
    Devuelve (ok, mensaje).
    """
    if not canonical_name or not str(canonical_name).strip():
        return False, "Debes indicar el nombre canónico (uno de los dos mostrados)."
    canonical_name = str(canonical_name).strip()
    now = datetime.utcnow().isoformat()
    with get_connection() as conn:
        c = conn.cursor()
        c.execute(
            """SELECT id, league_id, name_variant_1, name_variant_2 FROM pending_team_aliases
               WHERE id = ? AND status = 'pending'""",
            (pending_id,),
        )
        row = c.fetchone()
        if not row:
            return False, "No existe o ya está resuelto."
        rid, league_id, n1, n2 = row["id"], row["league_id"], row["name_variant_1"], row["name_variant_2"]
        if canonical_name not in (n1, n2):
            return False, f"El canónico debe ser «{n1}» o «{n2}»."
        alias_name = n2 if canonical_name == n1 else n1
        add_entity_alias("team", canonical_name, alias_name, league_id)
        c.execute(
            """UPDATE pending_team_aliases SET status = 'resolved', resolved_canonical = ?, resolved_at = ?, resolved_by = ?
               WHERE id = ?""",
            (canonical_name, now, resolved_by, rid),
        )
    return True, f"Alias registrado: «{alias_name}» → «{canonical_name}» (liga {league_id})."


def insert_pending_team_alias_if_new(
    league_id: str,
    name_variant_1: str,
    name_variant_2: str,
) -> bool:
    """
    Inserta un candidato (name_variant_1, name_variant_2) para la liga si no existe ya.
    Guarda el par ordenado para no duplicar (A,B) y (B,A). Devuelve True si se insertó.
    """
    if not league_id or not name_variant_1 or not name_variant_2:
        return False
    n1 = str(name_variant_1).strip()
    n2 = str(name_variant_2).strip()
    if n1 == n2:
        return False
    n1, n2 = (n1, n2) if n1 < n2 else (n2, n1)
    now = datetime.utcnow().isoformat()
    with get_connection() as conn:
        c = conn.cursor()
        return _try_insert_ignore_duplicate(
            c,
            """INSERT INTO pending_team_aliases (league_id, name_variant_1, name_variant_2, status, created_at)
               VALUES (?, ?, ?, 'pending', ?)""",
            (league_id, n1, n2, now),
        )


def seed_default_entity_aliases() -> int:
    """Inserta aliases por defecto (Milan, Man United, Roma, etc.). Devuelve número insertados."""
    defaults = [
        ("team", "AC Milan", "Milan", "SA"),
        ("team", "Man United", "Man Utd", "PL"),
        ("team", "Man United", "Manchester United", "PL"),
        ("team", "Man City", "Manchester City", "PL"),
        ("team", "Crystal Palace", "Palace", "PL"),
        ("team", "Newcastle", "Newcastle Utd", "PL"),
        ("team", "Leicester", "Leicester City", "PL"),
        ("team", "Watford", "Watford FC", "PL"),
        ("team", "Roma", "AS Roma", "SA"),
        ("team", "Bayern Munich", "Bayern München", "BL1"),
        ("team", "Napoli", "Napoli", "SA"),
        ("team", "US Sassuolo Calcio", "Sassuolo", "SA"),
        ("team", "Hellas Verona FC", "Verona", "SA"),
        ("team", "Olympique de Marseille", "Marseille", "FL1"),
        ("team", "Stade Brestois 29", "Brest", "FL1"),
        ("team", "CF Estrela da Amadora", "Estrela", "PPL"),
        ("team", "CD Tondela", "Tondela", "PPL"),
    ]
    n = 0
    for row in defaults:
        try:
            add_entity_alias(row[0], row[1], row[2], row[3] if len(row) > 3 else None)
            n += 1
        except Exception:
            pass
    return n


# ---------- master_table y master_table_checked ----------


def copy_historical_to_master(
    league_id: Optional[str] = None,
    season: Optional[int] = None,
) -> int:
    """
    Copia historical_matches → master_table aplicando normalización de nombres.
    Si league_id/season se pasan, solo copia esa liga/temporada. Devuelve número de filas insertadas/actualizadas.
    """
    with get_connection() as conn:
        c = conn.cursor()
        where = []
        params: List[Any] = []
        if league_id:
            where.append("league_id = ?")
            params.append(league_id)
        if season is not None:
            where.append("season = ?")
            params.append(season)
        where_clause = (" AND " + " AND ".join(where)) if where else ""

        c.execute(
            f"""SELECT fixture_id, date, league_id, home_team_id, away_team_id, home_team_name, away_team_name,
                home_goals, away_goals, status, season, home_xg, away_xg, api_sports_fixture_id,
                kickoff_time, ftr, hthg, htag, htr, attendance, referee,
                home_shots, away_shots, home_shots_target, away_shots_target,
                home_corners, away_corners, home_fouls, away_fouls,
                home_yellow, away_yellow, home_red, away_red,
                home_offsides, away_offsides, created_at
                FROM historical_matches WHERE 1=1 {where_clause}""",
            params,
        )
        rows = c.fetchall()

    alias_cache = _load_entity_aliases_cache()
    n = 0
    with get_connection() as conn:
        curs = conn.cursor()
        for r in rows:
            fid, date_, lid, hid, aid, hname, aname = r[:7]
            rest = list(r[7:])
            hname_norm = normalize_team_name(hname, lid, _cache=alias_cache) if hname else hname
            aname_norm = normalize_team_name(aname, lid, _cache=alias_cache) if aname else aname
            src = "api_sports" if fid >= 900_000_000 else "football_data_csv"
            curs.execute(
                """INSERT INTO master_table (
                    fixture_id, date, league_id, home_team_id, away_team_id, home_team_name, away_team_name,
                    home_goals, away_goals, status, season, home_xg, away_xg, api_sports_fixture_id,
                    kickoff_time, ftr, hthg, htag, htr, attendance, referee,
                    home_shots, away_shots, home_shots_target, away_shots_target,
                    home_corners, away_corners, home_fouls, away_fouls,
                    home_yellow, away_yellow, home_red, away_red,
                    home_offsides, away_offsides, created_at,
                    source, verificado_1, verificado_2
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(fixture_id) DO UPDATE SET
                    date=excluded.date, home_team_name=excluded.home_team_name, away_team_name=excluded.away_team_name,
                    home_goals=excluded.home_goals, away_goals=excluded.away_goals, status=excluded.status,
                    season=excluded.season, ftr=excluded.ftr, kickoff_time=excluded.kickoff_time,
                    hthg=excluded.hthg, htag=excluded.htag, htr=excluded.htr,
                    attendance=excluded.attendance, referee=excluded.referee,
                    home_shots=excluded.home_shots, away_shots=excluded.away_shots,
                    home_shots_target=excluded.home_shots_target, away_shots_target=excluded.away_shots_target,
                    home_corners=excluded.home_corners, away_corners=excluded.away_corners,
                    home_fouls=excluded.home_fouls, away_fouls=excluded.away_fouls,
                    home_yellow=excluded.home_yellow, away_yellow=excluded.away_yellow,
                    home_red=excluded.home_red, away_red=excluded.away_red,
                    home_offsides=excluded.home_offsides, away_offsides=excluded.away_offsides
                """,
                (fid, date_, lid, hid, aid, hname_norm or hname, aname_norm or aname, *rest, src, 0, 0),
            )
            n += 1
    return n


def _get_standings_override(league_id: str, season: int) -> List[Dict[str, Any]]:
    """Clasificación corregida por humano (standings_override). Devuelve [] si no hay override."""
    with get_connection() as conn:
        c = conn.cursor()
        c.execute(
            """SELECT rank, team_name, points, wins, draws, losses
               FROM standings_override WHERE league_id = ? AND season = ?
               ORDER BY rank""",
            (league_id, season),
        )
        rows = c.fetchall()
    if not rows:
        return []
    return [
        {
            "team_name": r[1],
            "points": int(r[2]),
            "wins": int(r[3] or 0),
            "draws": int(r[4] or 0),
            "losses": int(r[5] or 0),
        }
        for r in rows
    ]


def get_standings_from_master_table(
    league_id: str, season: int
) -> List[Dict[str, Any]]:
    """Clasificación calculada desde master_table o standings_override si existe."""
    override = _get_standings_override(league_id, season)
    if override:
        return override
    with get_connection() as conn:
        c = conn.cursor()
        c.execute(
            """
            SELECT home_team_name, away_team_name, ftr, home_goals, away_goals
            FROM master_table
            WHERE league_id = ? AND season = ? AND (ftr = 'H' OR ftr = 'D' OR ftr = 'A')
            """,
            (league_id, season),
        )
        rows = c.fetchall()

    pts: Dict[str, int] = {}
    gf: Dict[str, int] = {}
    gc: Dict[str, int] = {}
    w: Dict[str, int] = {}
    d: Dict[str, int] = {}
    l: Dict[str, int] = {}

    def _init(t: str) -> None:
        if t:
            pts.setdefault(t, 0)
            gf.setdefault(t, 0)
            gc.setdefault(t, 0)
            w.setdefault(t, 0)
            d.setdefault(t, 0)
            l.setdefault(t, 0)

    for home, away, ftr, hg, ag in rows:
        _init(home)
        _init(away)
        gf[home] = gf.get(home, 0) + hg
        gc[home] = gc.get(home, 0) + ag
        gf[away] = gf.get(away, 0) + ag
        gc[away] = gc.get(away, 0) + hg
        if ftr == "H":
            pts[home] += 3
            w[home] += 1
            l[away] += 1
        elif ftr == "A":
            pts[away] += 3
            w[away] += 1
            l[home] += 1
        else:
            pts[home] += 1
            pts[away] += 1
            d[home] += 1
            d[away] += 1

    sorted_teams = sorted(
        pts.items(),
        key=lambda x: (-x[1], -(gf.get(x[0], 0) - gc.get(x[0], 0))),
    )
    return [
        {
            "team_name": team,
            "points": p,
            "wins": w.get(team, 0),
            "draws": d.get(team, 0),
            "losses": l.get(team, 0),
        }
        for team, p in sorted_teams
    ]


def get_matches_from_master_checked(
    league_id: Optional[str] = None,
    season: Optional[int] = None,
    limit: int = 1000,
) -> List[Dict[str, Any]]:
    """Partidos desde master_table_checked (solo para agentes IA). Si liga/season vacíos, devuelve todos hasta limit."""
    where = []
    params: List[Any] = []
    if league_id:
        where.append("league_id = ?")
        params.append(league_id)
    if season is not None:
        where.append("season = ?")
        params.append(season)
    where_clause = (" WHERE " + " AND ".join(where)) if where else ""
    params.append(limit)

    with get_connection() as conn:
        c = conn.cursor()
        c.execute(
            f"""SELECT fixture_id, date, league_id, home_team_name, away_team_name, home_goals, away_goals,
                ftr, season, home_yellow, away_yellow, home_red, away_red
                FROM master_table_checked {where_clause} ORDER BY date DESC LIMIT ?""",
            params,
        )
        rows = c.fetchall()
    return [
        {
            "fixture_id": r[0],
            "date": r[1],
            "league_id": r[2],
            "home_team_name": r[3],
            "away_team_name": r[4],
            "home_goals": r[5],
            "away_goals": r[6],
            "ftr": r[7],
            "season": r[8],
            "home_yellow": r[9],
            "away_yellow": r[10],
            "home_red": r[11],
            "away_red": r[12],
        }
        for r in rows
    ]


def _compute_standings_from_master_checked(
    league_id: str, season: int
) -> List[Dict[str, Any]]:
    """Clasificación calculada desde master_table_checked."""
    with get_connection() as conn:
        c = conn.cursor()
        c.execute(
            """
            SELECT home_team_name, away_team_name, ftr, home_goals, away_goals
            FROM master_table_checked
            WHERE league_id = ? AND season = ? AND (ftr = 'H' OR ftr = 'D' OR ftr = 'A')
            """,
            (league_id, season),
        )
        rows = c.fetchall()

    pts: Dict[str, int] = {}
    gf: Dict[str, int] = {}
    gc: Dict[str, int] = {}
    w: Dict[str, int] = {}
    d: Dict[str, int] = {}
    l: Dict[str, int] = {}

    def _init(t: str) -> None:
        if t:
            pts.setdefault(t, 0)
            gf.setdefault(t, 0)
            gc.setdefault(t, 0)
            w.setdefault(t, 0)
            d.setdefault(t, 0)
            l.setdefault(t, 0)

    for home, away, ftr, hg, ag in rows:
        _init(home)
        _init(away)
        gf[home] = gf.get(home, 0) + hg
        gc[home] = gc.get(home, 0) + ag
        gf[away] = gf.get(away, 0) + ag
        gc[away] = gc.get(away, 0) + hg
        if ftr == "H":
            pts[home] += 3
            w[home] += 1
            l[away] += 1
        elif ftr == "A":
            pts[away] += 3
            w[away] += 1
            l[home] += 1
        else:
            pts[home] += 1
            pts[away] += 1
            d[home] += 1
            d[away] += 1

    sorted_teams = sorted(
        pts.items(),
        key=lambda x: (-x[1], -(gf.get(x[0], 0) - gc.get(x[0], 0))),
    )
    return [
        {
            "team_name": team,
            "points": p,
            "wins": w.get(team, 0),
            "draws": d.get(team, 0),
            "losses": l.get(team, 0),
        }
        for team, p in sorted_teams
    ]


def get_standings_from_master_checked(
    league_id: str, season: int
) -> List[Dict[str, Any]]:
    """Clasificación desde master_table_checked o standings_override (para agentes IA)."""
    override = _get_standings_override(league_id, season)
    if override:
        return override
    return _compute_standings_from_master_checked(league_id, season)


def promote_to_master_checked(
    fixture_ids: Optional[List[int]] = None,
    league_id: Optional[str] = None,
    season: Optional[int] = None,
    verified_by: Optional[int] = None,
) -> int:
    """
    Promueve registros de master_table a master_table_checked cuando verificado_1=1 y verificado_2=1.
    Si fixture_ids se pasa, solo esos; si no, filtra por league_id/season. Devuelve número promovidos.
    """
    where = ["verificado_1 = 1", "verificado_2 = 1"]
    params: List[Any] = []
    if fixture_ids:
        placeholders = ",".join("?" * len(fixture_ids))
        where.append(f"fixture_id IN ({placeholders})")
        params.extend(fixture_ids)
    if league_id:
        where.append("league_id = ?")
        params.append(league_id)
    if season is not None:
        where.append("season = ?")
        params.append(season)
    where_clause = " AND ".join(where)
    now = datetime.utcnow().isoformat()

    with get_connection() as conn:
        c = conn.cursor()
        c.execute(
            f"""INSERT OR REPLACE INTO master_table_checked (
                fixture_id, date, league_id, home_team_id, away_team_id, home_team_name, away_team_name,
                home_goals, away_goals, status, season, home_xg, away_xg, api_sports_fixture_id,
                kickoff_time, ftr, hthg, htag, htr, attendance, referee,
                home_shots, away_shots, home_shots_target, away_shots_target,
                home_corners, away_corners, home_fouls, away_fouls,
                home_yellow, away_yellow, home_red, away_red,
                home_offsides, away_offsides, checked_at, checked_by
            ) SELECT
                fixture_id, date, league_id, home_team_id, away_team_id, home_team_name, away_team_name,
                home_goals, away_goals, status, season, home_xg, away_xg, api_sports_fixture_id,
                kickoff_time, ftr, hthg, htag, htr, attendance, referee,
                home_shots, away_shots, home_shots_target, away_shots_target,
                home_corners, away_corners, home_fouls, away_fouls,
                home_yellow, away_yellow, home_red, away_red,
                home_offsides, away_offsides, ?, ?
            FROM master_table WHERE {where_clause}""",
            [now, verified_by] + params,
        )
        return c.rowcount


# ---------- master_table: listado para comparación con API-Sports ----------


def get_master_table_fixtures_for_comparison(
    league_id: Optional[str] = None,
    season: Optional[int] = None,
    limit: int = 500,
) -> List[Dict[str, Any]]:
    """
    Partidos de master_table para comparar con API-Sports.
    Columnas: fixture_id, date, league_id, home_team_name, away_team_name,
    home_goals, away_goals, ftr, hthg, htag, htr, api_sports_fixture_id.
    """
    where = ["status = 'FT'"]
    params: List[Any] = []
    if league_id:
        where.append("league_id = ?")
        params.append(league_id)
    if season is not None:
        where.append("season = ?")
        params.append(season)
    where_clause = " AND ".join(where)
    params.append(limit)
    with get_connection() as conn:
        c = conn.cursor()
        c.execute(
            f"""SELECT fixture_id, date, league_id, home_team_name, away_team_name,
                   home_goals, away_goals, ftr, hthg, htag, htr, api_sports_fixture_id
                FROM master_table WHERE {where_clause} ORDER BY date DESC LIMIT ?""",
            params,
        )
        rows = c.fetchall()
    return [
        {
            "fixture_id": r[0],
            "date": r[1],
            "league_id": r[2],
            "home_team_name": r[3],
            "away_team_name": r[4],
            "home_goals": r[5],
            "away_goals": r[6],
            "ftr": r[7],
            "hthg": r[8],
            "htag": r[9],
            "htr": r[10],
            "api_sports_fixture_id": r[11],
        }
        for r in rows
    ]


# ---------- data_discrepancies ----------


def insert_discrepancy(
    entity_type: str,
    entity_id: str,
    field: str,
    value_source_a: str,
    value_source_b: str,
    source_a: str,
    source_b: str,
    league_id: Optional[str] = None,
    season: Optional[int] = None,
) -> int:
    """Inserta una discrepancia pendiente. Devuelve id insertado."""
    now = datetime.utcnow().isoformat()
    with get_connection() as conn:
        c = conn.cursor()
        c.execute(
            """INSERT INTO data_discrepancies (entity_type, entity_id, field, value_source_a, value_source_b,
               source_a, source_b, status, created_at, league_id, season)
               VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?)""",
            (entity_type, entity_id, field, value_source_a, value_source_b, source_a, source_b, now, league_id, season),
        )
        return c.lastrowid or 0


def get_existing_discrepancy(
    entity_type: str,
    entity_id: str,
    field: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Devuelve discrepancia pendiente si existe. Si field se pasa, filtra por ese campo."""
    with get_connection() as conn:
        c = conn.cursor()
        if field:
            c.execute(
                """SELECT id, entity_type, entity_id, field, value_source_a, value_source_b, source_a, source_b,
                   league_id, season FROM data_discrepancies
                   WHERE entity_type = ? AND entity_id = ? AND field = ? AND status = 'pending' LIMIT 1""",
                (entity_type, entity_id, field),
            )
        else:
            c.execute(
                """SELECT id, entity_type, entity_id, field, value_source_a, value_source_b, source_a, source_b,
                   league_id, season FROM data_discrepancies
                   WHERE entity_type = ? AND entity_id = ? AND status = 'pending' LIMIT 1""",
                (entity_type, entity_id),
            )
        row = c.fetchone()
    if not row:
        return None
    return {
        "id": row[0],
        "entity_type": row[1],
        "entity_id": row[2],
        "field": row[3],
        "value_source_a": row[4],
        "value_source_b": row[5],
        "source_a": row[6],
        "source_b": row[7],
        "league_id": row[8],
        "season": row[9],
    }


def get_pending_discrepancies(
    league_id: Optional[str] = None,
    season: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Lista discrepancias pendientes. Opcional filtro por liga/season."""
    where = ["status = 'pending'"]
    params: List[Any] = []
    if league_id:
        where.append("league_id = ?")
        params.append(league_id)
    if season is not None:
        where.append("season = ?")
        params.append(season)
    where_clause = " AND ".join(where)
    with get_connection() as conn:
        c = conn.cursor()
        c.execute(
            f"""SELECT id, entity_type, entity_id, field, value_source_a, value_source_b, source_a, source_b,
                league_id, season FROM data_discrepancies WHERE {where_clause} ORDER BY league_id, season""",
            params,
        )
        rows = c.fetchall()
    return [
        {
            "id": r[0],
            "entity_type": r[1],
            "entity_id": r[2],
            "field": r[3],
            "value_source_a": r[4],
            "value_source_b": r[5],
            "source_a": r[6],
            "source_b": r[7],
            "league_id": r[8],
            "season": r[9],
        }
        for r in rows
    ]


def resolve_discrepancy(
    discrepancy_id: int,
    choice: str,
    manual_value: Optional[str] = None,
    manual_points: Optional[int] = None,
    resolved_by: Optional[int] = None,
) -> Tuple[bool, str]:
    """
    Resuelve una discrepancia. choice: 'master_table' | 'api_sports' | 'manual'.
    Si manual: manual_value (ej. nombre campeón, normalizado internamente) y opcional manual_points.
    Al resolver: inserta standings_override, marca verificado_1 y verificado_2 en master_table,
    promueve a master_table_checked. Devuelve (ok, mensaje).
    """
    with get_connection() as conn:
        c = conn.cursor()
        c.execute(
            """SELECT id, entity_type, entity_id, field, value_source_a, value_source_b, source_a, source_b,
               league_id, season FROM data_discrepancies WHERE id = ? AND status = 'pending'""",
            (discrepancy_id,),
        )
        row = c.fetchone()
    if not row:
        return False, "Discrepancia no encontrada o ya resuelta"

    did, etype, eid, field, val_a, val_b, src_a, src_b, lid, seas = row
    league_id = lid or ""
    season = seas or 0
    now = datetime.utcnow().isoformat()

    # ---------- Discrepancias de fixture (partido a partido) ----------
    if etype == "fixture":
        _ALLOWED_FIXTURE_FIELDS = frozenset(
            {"home_goals", "away_goals", "home_team_name", "away_team_name", "ftr", "hthg", "htag", "htr"}
        )
        if field not in _ALLOWED_FIXTURE_FIELDS:
            return False, f"Campo no permitido para fixture: {field}"
        resolved_val = ""
        if choice == "api_sports":
            resolved_val = (val_b or "").replace("(vacío)", "").strip()
        elif choice == "master_table":
            resolved_val = (val_a or "").replace("(vacío)", "").strip()
        elif choice == "manual" and manual_value:
            resolved_val = normalize_team_name(manual_value.strip(), league_id) if "team_name" in field else manual_value.strip()
        else:
            return False, "Debes elegir master_table, api_sports o manual (con valor)"
        try:
            # Conversión numérica si aplica
            if field in ("home_goals", "away_goals", "hthg", "htag"):
                resolved_val = str(int(float(resolved_val or 0)))
            elif field == "ftr":
                resolved_val = str(resolved_val or "D")[:1].upper()
                if resolved_val not in ("H", "D", "A"):
                    resolved_val = "D"
        except (ValueError, TypeError):
            resolved_val = "0" if field in ("home_goals", "away_goals", "hthg", "htag") else (resolved_val or "")
        fixture_id_int = int(eid)
        with get_connection() as conn:
            c = conn.cursor()
            c.execute(
                f"""UPDATE master_table SET {field} = ? WHERE fixture_id = ?""",
                (resolved_val, fixture_id_int),
            )
            c.execute(
                """UPDATE master_table SET verificado_1 = 1, verificado_2 = 1, verificado_1_at = ?, verificado_2_at = ?, verificado_2_by = ?
                   WHERE fixture_id = ?""",
                (now, now, resolved_by, fixture_id_int),
            )
            c.execute(
                """UPDATE data_discrepancies SET status = 'resolved', resolved_value = ?, resolved_at = ?, resolved_by = ?
                   WHERE id = ?""",
                (resolved_val, now, resolved_by, discrepancy_id),
            )
        n = promote_to_master_checked(fixture_ids=[fixture_id_int], verified_by=resolved_by)
        return True, f"Fixture {eid} campo {field} actualizado. Doble check aplicado. {n} partido(s) en master_table_checked."

    # ---------- Discrepancias de standings_champion ----------
    if etype != "standings_champion" or field != "champion":
        return False, "Solo se soportan standings_champion o fixture"

    resolved_team = ""
    resolved_pts = None
    if choice == "api_sports":
        resolved_team = val_b.split("(")[0].strip() if val_b else ""
        if val_b and "(" in val_b:
            try:
                resolved_pts = int(val_b.split("(")[1].replace(" pts)", "").strip())
            except (ValueError, IndexError):
                pass
    elif choice == "master_table":
        resolved_team = val_a.split("(")[0].strip() if val_a else ""
        if val_a and "(" in val_a:
            try:
                resolved_pts = int(val_a.split("(")[1].replace(" pts)", "").strip())
            except (ValueError, IndexError):
                pass
    elif choice == "manual" and manual_value:
        resolved_team = normalize_team_name(manual_value.strip(), league_id if league_id else None)
        resolved_pts = manual_points
    else:
        return False, "Debes elegir master_table, api_sports o manual (con valor)"

    if not resolved_team:
        return False, "No se pudo extraer el nombre del campeón"

    if resolved_pts is None:
        resolved_pts = 0

    with get_connection() as conn:
        c = conn.cursor()
        c.execute(
            """INSERT OR REPLACE INTO standings_override (league_id, season, rank, team_name, points, wins, draws, losses)
               VALUES (?, ?, 1, ?, ?, NULL, NULL, NULL)""",
            (league_id, season, resolved_team, resolved_pts),
        )
        c.execute(
            """UPDATE data_discrepancies SET status = 'resolved', resolved_value = ?, resolved_at = ?, resolved_by = ?
               WHERE id = ?""",
            (resolved_team, now, resolved_by, discrepancy_id),
        )
        c.execute(
            """UPDATE master_table SET verificado_1 = 1, verificado_2 = 1, verificado_1_at = ?, verificado_2_at = ?, verificado_2_by = ?
               WHERE league_id = ? AND season = ?""",
            (now, now, resolved_by, league_id, season),
        )
    n = promote_to_master_checked(league_id=league_id, season=season, verified_by=resolved_by)
    return True, f"Resuelto: {resolved_team}. {n} partidos promovidos a master_table_checked"


def get_team_yellow_cards_ranking(
    league_id: str, season: int, limit: int = 20
) -> List[Dict[str, Any]]:
    """
    Ranking de equipos por total de tarjetas amarillas en una temporada (desde historical_matches).
    Suma home_yellow como local y away_yellow como visitante. Orden: más amarillas primero.
    """
    with get_connection() as conn:
        c = conn.cursor()
        c.execute(
            """
            SELECT team_name, SUM(yellows) AS total_yellows FROM (
                SELECT home_team_name AS team_name, COALESCE(home_yellow, 0) AS yellows
                FROM historical_matches WHERE league_id = ? AND season = ?
                UNION ALL
                SELECT away_team_name, COALESCE(away_yellow, 0)
                FROM historical_matches WHERE league_id = ? AND season = ?
            ) GROUP BY team_name ORDER BY total_yellows DESC LIMIT ?
            """,
            (league_id, season, league_id, season, limit),
        )
        rows = c.fetchall()
    return [{"team_name": r[0], "yellow_cards": int(r[1])} for r in rows]

