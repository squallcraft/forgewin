"""
Descarga inicial desde football-data.co.uk (CSV) y actualización semanal desde API-Sports (delta).
Para historial rolling: Top 10 ligas europeas + Champions League + Europa League.
Idempotente: se puede ejecutar varias veces sin duplicar datos (upsert por fixture_id).
"""

import hashlib
import logging
import os
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

from db import get_connection, upsert_historical_match, normalize_team_name

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent / ".env")
except ImportError:
    pass

logger = logging.getLogger(__name__)

# football-data.co.uk: base URL y mapeo liga -> código de archivo (Premier=E0, LaLiga=SP1, etc.)
FD_BASE = "https://www.football-data.co.uk/mmz4281"
# Código ForgeWin (config.LEAGUES) -> código archivo CSV (solo ligas con CSV disponible)
LEAGUE_TO_CSV_CODE: Dict[str, str] = {
    "PL": "E0",   # Premier League
    "PD": "SP1",  # La Liga
    "SA": "I1",   # Serie A
    "BL1": "D1",  # Bundesliga
    "FL1": "F1",  # Ligue 1
    "DED": "N1",  # Eredivisie
    "PPL": "P1",  # Primeira Liga
    "ELC": "E1",  # Championship
    "EL1": "E2",  # League One
}
# CL y EL no tienen CSV en football-data.co.uk con el mismo formato; se usan solo vía API-Sports.

# Rango de fixture_id para partidos cargados desde CSV (evitar colisión con API-Sports 9xx)
CSV_FIXTURE_ID_OFFSET = 800_000_000

# football-data.co.uk: primer año de temporada disponible (sitio: "back to 1993/94")
# Último: temporada actual (_current_season_year()). No hay límite de peticiones documentado; usamos pausa.
FD_FIRST_AVAILABLE_SEASON = 1994


def _season_year_to_fd_str(season_year: int) -> str:
    """Convierte año de temporada (ej. 2024) a string football-data 2324 (temporada 2023/24)."""
    # Temporada 1989/90 -> 8990, 2023/24 -> 2324 (solo 2 dígitos por año)
    return f"{(season_year - 1) % 100:02d}{season_year % 100:02d}"


def _current_season_year() -> int:
    """Año de la temporada actual (europea: jul-jun). Ej: en feb 2026 -> 2025 (temporada 2024/25)."""
    today = date.today()
    return today.year if today.month >= 7 else today.year - 1


def _csv_fixture_id(date_str: str, home: str, away: str, league_id: str) -> int:
    """Genera un fixture_id estable para filas CSV (idempotente por partido)."""
    raw = f"{date_str}|{home}|{away}|{league_id}"
    h = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:10]
    return CSV_FIXTURE_ID_OFFSET + (int(h, 16) % 99_999_999)


def _parse_csv_date(d: str) -> Optional[str]:
    """Convierte fecha DD/MM/YYYY a YYYY-MM-DD.
    Años de 2 dígitos: 00-29 → 2000-2029, 30-99 → 1930-1999.
    """
    if not d or len(d) < 8:
        return None
    parts = d.split("/")
    if len(parts) != 3:
        return None
    try:
        day, month, year = int(parts[0]), int(parts[1]), int(parts[2])
        if year < 100:
            year += 2000 if year < 30 else 1900
        return f"{year:04d}-{month:02d}-{day:02d}"
    except (ValueError, TypeError):
        return None


# Pausa entre peticiones (football-data.co.uk no documenta límite; evitar saturar / cuotas)
_DOWNLOAD_DELAY_SECONDS = float(os.getenv("FOOTBALL_DATA_DOWNLOAD_DELAY", "1.0"))


def download_historical_csvs(
    seasons: Optional[List[int]] = None,
    league_codes: Optional[List[str]] = None,
    save_dir: Optional[Path] = None,
) -> List[Dict[str, Any]]:
    """
    Descarga CSVs desde football-data.co.uk para las temporadas y ligas indicadas.
    URL: https://www.football-data.co.uk/mmz4281/<fd_season>/<csv_code>.csv
    fd_season: 2 dígitos año inicio + 2 año fin (ej. 2324 = 2023/24). Datos desde 1993/94 (season_year >= 1994).
    seasons: lista de años de temporada (ej. [2022, 2023, 2024, 2025]).
    league_codes: códigos ForgeWin (PL, PD, ...). Por defecto todas las que tienen CSV.
    save_dir: carpeta donde guardar CSVs; si es None no se guardan a disco (solo parse en memoria).
    Devuelve lista de filas listas para upsert en historical_matches.
    """
    import time
    current = _current_season_year()
    season_list = seasons or [current - 3, current - 2, current - 1, current]
    leagues = league_codes or list(LEAGUE_TO_CSV_CODE.keys())
    rows: List[Dict[str, Any]] = []
    if save_dir:
        save_dir = Path(save_dir)
        save_dir.mkdir(parents=True, exist_ok=True)

    for league_code in leagues:
        csv_code = LEAGUE_TO_CSV_CODE.get(league_code)
        if not csv_code:
            logger.debug("Saltando liga %s (sin CSV en football-data.co.uk)", league_code)
            continue
        for season_year in season_list:
            fd_season = _season_year_to_fd_str(season_year)
            url = f"{FD_BASE}/{fd_season}/{csv_code}.csv"
            try:
                time.sleep(_DOWNLOAD_DELAY_SECONDS)
                logger.info("Descargando %s temporada %s: %s", league_code, season_year, url)
                r = requests.get(url, timeout=30)
                r.raise_for_status()
                text = r.text
                if save_dir:
                    (save_dir / f"{league_code}_{fd_season}.csv").write_text(text, encoding="utf-8", errors="replace")
                # Parse CSV (cabecera en primera línea)
                lines = text.strip().split("\n")
                if len(lines) < 2:
                    continue
                header = [h.strip() for h in lines[0].split(",")]
                def _col(name: str, *alt: str) -> int:
                    for n in (name,) + alt:
                        if n in header:
                            return header.index(n)
                    return -1
                date_col = _col("Date")
                if date_col < 0:
                    date_col = 0
                home_col = _col("HomeTeam", "Home")
                away_col = _col("AwayTeam", "Away")
                fthg_col = _col("FTHG")
                ftag_col = _col("FTAG")
                if fthg_col < 0:
                    fthg_col = None
                if ftag_col < 0:
                    ftag_col = None
                if home_col < 0 or away_col < 0:
                    logger.warning("Cabecera no esperada en %s %s: %s", league_code, fd_season, header[:5])
                    continue
                # Columnas opcionales: resultado ampliado y estadísticas (football-data.co.uk)
                opt_cols = {
                    "Time": "kickoff_time", "FTR": "ftr", "HTHG": "hthg", "HTAG": "htag", "HTR": "htr",
                    "Attendance": "attendance", "Referee": "referee",
                    "HS": "home_shots", "AS": "away_shots", "HST": "home_shots_target", "AST": "away_shots_target",
                    "HC": "home_corners", "AC": "away_corners", "HF": "home_fouls", "AF": "away_fouls",
                    "HY": "home_yellow", "AY": "away_yellow", "HR": "home_red", "AR": "away_red",
                    "HO": "home_offsides", "AO": "away_offsides",
                }
                opt_indices = {csv_name: _col(csv_name) for csv_name in opt_cols}
                for line in lines[1:]:
                    parts = [p.strip().strip('"') for p in line.split(",")]
                    if max(date_col, home_col, away_col) >= len(parts):
                        continue
                    date_iso = _parse_csv_date(parts[date_col])
                    if not date_iso:
                        continue
                    home, away = parts[home_col], parts[away_col]
                    try:
                        hg = int(parts[fthg_col]) if fthg_col is not None and fthg_col < len(parts) else 0
                        ag = int(parts[ftag_col]) if ftag_col is not None and ftag_col < len(parts) else 0
                    except (ValueError, TypeError):
                        continue
                    fid = _csv_fixture_id(date_iso, home, away, league_code)
                    row = {
                        "fixture_id": fid,
                        "date": date_iso,
                        "league_id": league_code,
                        "home_team_id": None,
                        "away_team_id": None,
                        "home_team_name": home,
                        "away_team_name": away,
                        "home_goals": hg,
                        "away_goals": ag,
                        "status": "FT",
                        "season": season_year,
                        "home_xg": None,
                        "away_xg": None,
                    }
                    for csv_name, key in opt_cols.items():
                        idx = opt_indices.get(csv_name, -1)
                        if idx < 0 or idx >= len(parts):
                            continue
                        raw = parts[idx].strip() if parts[idx] else ""
                        if not raw:
                            continue
                        if key in ("kickoff_time", "ftr", "htr", "referee"):
                            row[key] = raw
                        else:
                            try:
                                row[key] = int(raw)
                            except (ValueError, TypeError):
                                pass
                    rows.append(row)
            except requests.RequestException as e:
                logger.warning("Error descargando %s %s: %s", league_code, fd_season, e)
            except Exception as e:
                logger.exception("Error procesando CSV %s %s: %s", league_code, fd_season, e)

    return rows


# Claves opcionales de estadísticas que upsert_historical_match acepta como kwargs
_HISTORICAL_EXTRA_KEYS = (
    "kickoff_time", "ftr", "hthg", "htag", "htr", "attendance", "referee",
    "home_shots", "away_shots", "home_shots_target", "away_shots_target",
    "home_corners", "away_corners", "home_fouls", "away_fouls",
    "home_yellow", "away_yellow", "home_red", "away_red",
    "home_offsides", "away_offsides",
)


def load_csv_rows_into_db(rows: List[Dict[str, Any]]) -> int:
    """Inserta/actualiza en historical_matches las filas devueltas por download_historical_csvs. Idempotente."""
    from db import update_historical_cards_bulk

    count = 0
    for r in rows:
        try:
            extra = {k: r[k] for k in _HISTORICAL_EXTRA_KEYS if k in r}
            upsert_historical_match(
                fixture_id=r["fixture_id"],
                date=r["date"],
                league_id=r["league_id"],
                home_goals=r["home_goals"],
                away_goals=r["away_goals"],
                season=r["season"],
                status=r.get("status", "FT"),
                home_team_id=r.get("home_team_id"),
                away_team_id=r.get("away_team_id"),
                home_xg=r.get("home_xg"),
                away_xg=r.get("away_xg"),
                home_team_name=r.get("home_team_name"),
                away_team_name=r.get("away_team_name"),
                **extra,
            )
            count += 1
        except Exception as e:
            logger.warning("Error upsert historical row %s: %s", r.get("fixture_id"), e)
    # Actualizar tarjetas en bulk (el upsert puede no actualizar columnas extra si hay fallback)
    cards_updated = update_historical_cards_bulk([r for r in rows if any(r.get(k) is not None for k in ("home_yellow", "away_yellow", "home_red", "away_red"))])
    if cards_updated:
        logger.info("Tarjetas actualizadas: %d filas", cards_updated)
    return count


def _parse_saved_csv_file(path: Path, league_code: str, season_year: int) -> List[Dict[str, Any]]:
    """Parsea un CSV guardado (formato football-data.co.uk) y devuelve filas para historical_matches."""
    rows: List[Dict[str, Any]] = []
    text = path.read_text(encoding="utf-8", errors="replace")
    lines = text.strip().split("\n")
    if len(lines) < 2:
        return rows
    header = [h.strip() for h in lines[0].split(",")]

    def _col(name: str, *alt: str) -> int:
        for n in (name,) + alt:
            if n in header:
                return header.index(n)
        return -1

    date_col = _col("Date")
    if date_col < 0:
        date_col = 0
    home_col = _col("HomeTeam", "Home")
    away_col = _col("AwayTeam", "Away")
    fthg_col = _col("FTHG")
    ftag_col = _col("FTAG")
    if fthg_col < 0:
        fthg_col = None
    if ftag_col < 0:
        ftag_col = None
    if home_col < 0 or away_col < 0:
        return rows
    opt_cols = {
        "Time": "kickoff_time", "FTR": "ftr", "HTHG": "hthg", "HTAG": "htag", "HTR": "htr",
        "Attendance": "attendance", "Referee": "referee",
        "HS": "home_shots", "AS": "away_shots", "HST": "home_shots_target", "AST": "away_shots_target",
        "HC": "home_corners", "AC": "away_corners", "HF": "home_fouls", "AF": "away_fouls",
        "HY": "home_yellow", "AY": "away_yellow", "HR": "home_red", "AR": "away_red",
        "HO": "home_offsides", "AO": "away_offsides",
    }
    opt_indices = {csv_name: _col(csv_name) for csv_name in opt_cols}
    for line in lines[1:]:
        parts = [p.strip().strip('"') for p in line.split(",")]
        if max(date_col, home_col, away_col) >= len(parts):
            continue
        date_iso = _parse_csv_date(parts[date_col])
        if not date_iso:
            continue
        home, away = parts[home_col], parts[away_col]
        try:
            hg = int(parts[fthg_col]) if fthg_col is not None and fthg_col < len(parts) else 0
            ag = int(parts[ftag_col]) if ftag_col is not None and ftag_col < len(parts) else 0
        except (ValueError, TypeError):
            continue
        fid = _csv_fixture_id(date_iso, home, away, league_code)
        row = {
            "fixture_id": fid,
            "date": date_iso,
            "league_id": league_code,
            "home_team_id": None,
            "away_team_id": None,
            "home_team_name": home,
            "away_team_name": away,
            "home_goals": hg,
            "away_goals": ag,
            "status": "FT",
            "season": season_year,
            "home_xg": None,
            "away_xg": None,
        }
        for csv_name, key in opt_cols.items():
            idx = opt_indices.get(csv_name, -1)
            if idx < 0 or idx >= len(parts):
                continue
            raw = parts[idx].strip() if parts[idx] else ""
            if not raw:
                continue
            if key in ("kickoff_time", "ftr", "htr", "referee"):
                row[key] = raw
            else:
                try:
                    row[key] = int(raw)
                except (ValueError, TypeError):
                    pass
        rows.append(row)
    return rows


def load_from_csv_backup(csv_dir: Path) -> int:
    """
    Carga en historical_matches desde CSVs guardados en csv_dir (fallback cuando API-Sports falla).
    Espera nombres {league_code}_{fd_season}.csv (ej. PL_2324.csv). Orden: más reciente primero.
    Devuelve número de registros insertados/actualizados.
    """
    csv_dir = Path(csv_dir)
    if not csv_dir.exists():
        logger.warning("load_from_csv_backup: directorio no existe %s", csv_dir)
        return 0
    files = sorted(csv_dir.glob("*.csv"), key=lambda p: p.name, reverse=True)
    if not files:
        logger.warning("load_from_csv_backup: no hay CSVs en %s", csv_dir)
        return 0
    all_rows: List[Dict[str, Any]] = []
    for path in files:
        # Nombre: PL_2324.csv -> league_code=PL, fd_season=2324 -> season_year=2024
        stem = path.stem
        if "_" not in stem:
            continue
        league_code, fd_season = stem.split("_", 1)
        if league_code not in LEAGUE_TO_CSV_CODE:
            continue
        try:
            # fd_season 2324 = temporada 2023/24 -> season_year 2024
            year_suffix = int(fd_season) % 100
            season_year = 2000 + year_suffix if year_suffix < 50 else 1900 + year_suffix
        except ValueError:
            continue
        rows = _parse_saved_csv_file(path, league_code, season_year)
        all_rows.extend(rows)
        logger.info("Backup CSV: %s -> %d filas", path.name, len(rows))
    return load_csv_rows_into_db(all_rows)


def update_from_apisports(
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    league_codes: Optional[List[str]] = None,
) -> Tuple[int, Optional[str]]:
    """
    Trae partidos nuevos (finalizados) de la semana desde API-Sports e inserta en historical_matches.
    date_from/date_to: YYYY-MM-DD. Por defecto últimos 7 días.
    league_codes: ligas a actualizar; por defecto Top 10 + CL + EL.
    Devuelve (número de partidos insertados/actualizados, None o mensaje de error).
    Si API-Sports falla, devuelve (0, "mensaje") para que el llamador use respaldo CSV.
    """
    from config import TOP_10_LEAGUE_CODES
    from api_sports_fetcher import get_finished_fixtures_for_historical, API_KEY

    if not API_KEY:
        return 0, "API_FOOTBALL_KEY no configurada"

    end = date_to or date.today().isoformat()
    start = date_from or (date.today() - timedelta(days=7)).isoformat()
    leagues = league_codes or TOP_10_LEAGUE_CODES
    total = 0
    for code in leagues:
        try:
            rows = get_finished_fixtures_for_historical(code, start, end)
            for r in rows:
                try:
                    league_id = r["league_id"]
                    home_name = normalize_team_name(r.get("home_team_name"), league_id) or r.get("home_team_name")
                    away_name = normalize_team_name(r.get("away_team_name"), league_id) or r.get("away_team_name")
                    hg, ag = r["home_goals"], r["away_goals"]
                    ftr = "H" if hg > ag else ("A" if ag > hg else "D")
                    upsert_historical_match(
                        fixture_id=r["fixture_id"],
                        date=r["date"],
                        league_id=league_id,
                        home_goals=hg,
                        away_goals=ag,
                        season=r["season"],
                        status=r.get("status", "FT"),
                        home_team_id=r.get("home_team_id"),
                        away_team_id=r.get("away_team_id"),
                        home_xg=r.get("home_xg"),
                        away_xg=r.get("away_xg"),
                        home_team_name=home_name,
                        away_team_name=away_name,
                        api_sports_fixture_id=r.get("api_sports_fixture_id"),
                        attendance=r.get("attendance"),
                        referee=r.get("referee"),
                        ftr=ftr,
                    )
                    total += 1
                except Exception as e:
                    logger.warning("Error upsert API-Sports row %s: %s", r.get("fixture_id"), e)
        except Exception as e:
            logger.exception("Error obteniendo partidos API-Sports %s: %s", code, e)
            return total, str(e)
    return total, None


# Champions League y Europa League: no hay CSV en football-data.co.uk; se rellenan desde API-Sports.
# Primera temporada con datos típicamente disponible en API-Sports (ajustar si el plan da más historial).
CL_EL_FIRST_AVAILABLE_SEASON = 2010


def fetch_and_load_historical_cl_el(
    season_from: Optional[int] = None,
    season_to: Optional[int] = None,
    pause_between_seasons_seconds: float = 1.0,
) -> Tuple[int, Optional[str]]:
    """
    Descarga historial de Champions League (CL) y Europa League (EL) desde API-Sports
    temporada a temporada y lo carga en historical_matches.
    season_from/season_to: años de temporada (ej. 2024 = 2023/24). Por defecto 2010 a actual.
    Respetar cuotas: pausa entre cada petición (api_sports_fetcher) + pause_between_seasons_seconds.
    """
    import time
    from api_sports_fetcher import get_finished_fixtures_for_historical, API_KEY

    if not API_KEY:
        return 0, "API_FOOTBALL_KEY no configurada"

    current = _current_season_year()
    first = season_from if season_from is not None else CL_EL_FIRST_AVAILABLE_SEASON
    last = season_to if season_to is not None else current
    total = 0
    for league_code in ("CL", "EL"):
        for season in range(first, last + 1):
            date_from = f"{season - 1}-07-01"
            date_to = f"{season}-06-30"
            try:
                rows = get_finished_fixtures_for_historical(league_code, date_from, date_to)
                for r in rows:
                    try:
                        league_id = r["league_id"]
                        home_name = normalize_team_name(r.get("home_team_name"), league_id) or r.get("home_team_name")
                        away_name = normalize_team_name(r.get("away_team_name"), league_id) or r.get("away_team_name")
                        # Homologar: guardamos season = año fin (ej. 2024 = 2023/24), igual que PL/SA/PD
                        upsert_historical_match(
                            fixture_id=r["fixture_id"],
                            date=r["date"],
                            league_id=league_id,
                            home_goals=r["home_goals"],
                            away_goals=r["away_goals"],
                            season=season,
                            status=r.get("status", "FT"),
                            home_team_id=r.get("home_team_id"),
                            away_team_id=r.get("away_team_id"),
                            home_xg=r.get("home_xg"),
                            away_xg=r.get("away_xg"),
                            home_team_name=home_name,
                            away_team_name=away_name,
                            api_sports_fixture_id=r.get("api_sports_fixture_id"),
                            attendance=r.get("attendance"),
                            referee=r.get("referee"),
                        )
                        total += 1
                    except Exception as e:
                        logger.warning("Error upsert API-Sports row %s: %s", r.get("fixture_id"), e)
                if rows:
                    logger.info("CL/EL: %s temporada %s -> %d partidos", league_code, season, len(rows))
            except Exception as e:
                logger.exception("Error API-Sports %s temporada %s: %s", league_code, season, e)
                return total, str(e)
            time.sleep(pause_between_seasons_seconds)
    return total, None


def get_full_historical_season_range() -> Tuple[int, int]:
    """Devuelve (primer_año, último_año) de temporada según football-data.co.uk (desde 1993/94)."""
    return (FD_FIRST_AVAILABLE_SEASON, _current_season_year())


def download_full_historical(
    save_dir: Optional[Path] = None,
    league_codes: Optional[List[str]] = None,
    batch_seasons: Optional[int] = None,
    pause_between_batches_seconds: float = 20.0,
) -> List[Dict[str, Any]]:
    """
    Descarga historial desde el primer año disponible (1993/94) hasta la temporada actual.
    Si batch_seasons está definido, descarga en bloques de N temporadas y hace una pausa
    pause_between_batches_seconds entre bloques para no exceder cuotas.
    """
    import time
    first, last = get_full_historical_season_range()
    all_seasons = list(range(first, last + 1))
    leagues = league_codes or list(LEAGUE_TO_CSV_CODE.keys())
    logger.info(
        "Historial completo: temporadas %s a %s (%s años), %s ligas, pausa %.1f s entre peticiones",
        first, last, len(all_seasons), len(leagues), _DOWNLOAD_DELAY_SECONDS,
    )
    if not batch_seasons or batch_seasons >= len(all_seasons):
        return download_historical_csvs(seasons=all_seasons, league_codes=leagues, save_dir=save_dir)
    rows = []
    for i in range(0, len(all_seasons), batch_seasons):
        chunk = all_seasons[i : i + batch_seasons]
        logger.info("Lote %s: temporadas %s a %s (%s temporadas)", (i // batch_seasons) + 1, chunk[0], chunk[-1], len(chunk))
        rows.extend(download_historical_csvs(seasons=chunk, league_codes=leagues, save_dir=save_dir))
        if i + batch_seasons < len(all_seasons):
            logger.info("Pausa %.0f s antes del siguiente lote (evitar cuotas).", pause_between_batches_seconds)
            time.sleep(pause_between_batches_seconds)
    return rows


def run_full_initial_download(
    seasons: Optional[List[int]] = None,
    save_dir: Optional[Path] = None,
) -> int:
    """
    Descarga inicial masiva: CSVs de football-data.co.uk + carga en BD.
    Luego opcionalmente se puede llamar update_from_apisports para CL/EL y datos recientes.
    Devuelve número de registros insertados/actualizados desde CSV.
    """
    rows = download_historical_csvs(seasons=seasons, save_dir=save_dir)
    logger.info("Descarga inicial: %d filas desde CSV", len(rows))
    n = load_csv_rows_into_db(rows)
    logger.info("Cargados en BD: %d registros historical_matches", n)
    return n
