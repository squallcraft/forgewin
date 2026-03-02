"""
Generación de PDF con propuesta de análisis: portada, partidos incluidos y
análisis/recomendación (Alfred/Reginald). Fuente Helvetica, márgenes 1.5 cm.
"""

import os
import tempfile
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    from fpdf import FPDF
except ImportError:
    raise ImportError(
        "Se necesita el paquete fpdf2. Instálalo con: pip install fpdf2"
    ) from None

# --- Constantes de layout ---
MARGIN_MM = 15
LOGO_SIZE_MM = 8
PORTADA_HEIGHT_MM = 42
FORGEWIN_LOGO_WIDTH_MM = 22
FORGEWIN_LOGO_HEIGHT_MM = 12
EXTREME_PROB_THRESHOLD = 0.70
HIGH_PROB_THRESHOLD = 0.5
CS_HIGH_THRESHOLD = 0.4
GAUGE_MAX_GOALS = 4.0
GAUGE_LOW, GAUGE_MID = 1.5, 2.5

# Colores RGB (0-1)
COLORS = {
    "header_bg": (0.12, 0.25, 0.45),
    "local": (0.2, 0.45, 0.85),
    "draw": (0.45, 0.45, 0.5),
    "away": (0.85, 0.25, 0.2),
    "low_risk_bg": (0.2, 0.6, 0.35),
    "mod_risk_bg": (0.75, 0.55, 0.1),
    "high_risk_bg": (0.75, 0.2, 0.15),
    "row_high": (0.88, 0.96, 0.88),
    "badge_alto": (0.4, 0.75, 0.4),
    "badge_bajo": (0.9, 0.4, 0.35),
    "text": (0.15, 0.15, 0.2),
    "line": (0.75, 0.75, 0.78),
}


def _rgb(rgb_tuple: Tuple[float, float, float]) -> Tuple[int, int, int]:
    """Convierte RGB 0-1 a enteros 0-255."""
    return (int(rgb_tuple[0] * 255), int(rgb_tuple[1] * 255), int(rgb_tuple[2] * 255))


class ProposalPDF(FPDF):
    """FPDF con footer fijo (disclaimer y fecha de generación)."""

    def __init__(self, generated_at: str = "", **kwargs):
        super().__init__(**kwargs)
        self._generated_at = generated_at or datetime.now().strftime("%Y-%m-%d %H:%M")

    def footer(self):
        """Pie de página desactivado; se dibuja solo en la última página con _draw_footer_on_last_page."""
        pass


def _draw_footer_on_last_page(pdf: FPDF) -> None:
    """Dibuja el disclaimer y fecha solo al pie de la última página."""
    pdf.set_y(-MARGIN_MM - 10)
    pdf.set_font("Helvetica", "", 9)
    pdf.set_text_color(80, 80, 85)
    disclaimer = (
        "Análisis basado en datos históricos y modelo Poisson. "
        "Generado por ForgeWin el "
    )
    pdf.cell(0, 5, disclaimer + getattr(pdf, "_generated_at", ""), align="C", ln=True)


# ---------------------------------------------------------------------------
# Helpers de dibujo
# ---------------------------------------------------------------------------

def _fetch_league_logo_path(league_code: Optional[str]) -> Optional[str]:
    """Descarga el escudo de la liga a un archivo temporal. Si falla, usa logo de balón de fútbol. Devuelve ruta o None."""
    from config import get_league_emblem_url, FALLBACK_LOGO_URL
    url = None
    if league_code:
        try:
            url = get_league_emblem_url(league_code)
            req = urllib.request.Request(url, headers={"User-Agent": "ForgeWin/1.0"})
            with urllib.request.urlopen(req, timeout=5) as resp:
                data = resp.read()
            if data:
                fd, path = tempfile.mkstemp(suffix=".png")
                os.write(fd, data)
                os.close(fd)
                return path
        except Exception:
            pass
    # Fallback: logo de balón de fútbol
    try:
        req = urllib.request.Request(FALLBACK_LOGO_URL, headers={"User-Agent": "ForgeWin/1.0"})
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = resp.read()
        if data:
            fd, path = tempfile.mkstemp(suffix=".png")
            os.write(fd, data)
            os.close(fd)
            return path
    except Exception:
        pass
    return None


def _get_forgewin_logo_path() -> Optional[str]:
    """Ruta al logo ForgeWin (static/forgewin_logo.png o static/favicon.png)."""
    base = Path(__file__).resolve().parent
    for name in ("forgewin_logo.png", "favicon.png"):
        p = base / "static" / name
        if p.is_file():
            return str(p)
    return None


def _draw_forgewin_logo_on_cover(pdf: FPDF, y_start: float = 6) -> float:
    """Dibuja el logo ForgeWin centrado en la portada. Devuelve la Y tras el logo para seguir escribiendo."""
    logo_path = _get_forgewin_logo_path()
    if not logo_path:
        return y_start
    try:
        x = (pdf.w - FORGEWIN_LOGO_WIDTH_MM) / 2
        pdf.image(logo_path, x=x, y=y_start, w=FORGEWIN_LOGO_WIDTH_MM, h=FORGEWIN_LOGO_HEIGHT_MM)
        return y_start + FORGEWIN_LOGO_HEIGHT_MM + 3
    except Exception:
        return y_start


def _draw_separator(pdf: FPDF, y: float, color: Tuple[float, float, float] = COLORS["line"]) -> None:
    r, g, b = _rgb(color)
    pdf.set_draw_color(r, g, b)
    pdf.set_line_width(0.4)
    pdf.line(MARGIN_MM, y, pdf.w - MARGIN_MM, y)


def _draw_section_header(pdf: FPDF, title: str, font_size: int = 18) -> float:
    pdf.ln(6)
    pdf.set_font("Helvetica", "B", font_size)
    pdf.set_text_color(*_rgb(COLORS["text"]))
    pdf.cell(0, 10, title, ln=True)
    y = pdf.get_y()
    _draw_separator(pdf, y + 2)
    pdf.set_y(y + 6)
    return pdf.get_y()


# Colores de acento por tipo de sección
_ACCENT_ALFRED   = (0.20, 0.45, 0.85)   # azul
_ACCENT_REGINALD = (0.42, 0.18, 0.72)   # violeta
_ACCENT_CONSENSUS = (0.62, 0.42, 0.08)  # ámbar dorado
_ACCENT_NEUTRAL  = (0.28, 0.42, 0.55)   # pizarra


def _draw_section_header_accented(
    pdf: FPDF,
    title: str,
    font_size: int = 14,
    accent: Tuple[float, float, float] = _ACCENT_NEUTRAL,
) -> float:
    """Section header con barra de acento de color a la izquierda."""
    pdf.ln(5)
    bar_y = pdf.get_y()
    line_h = max(9, font_size * 0.72)
    # Barra lateral coloreada
    r, g, b = _rgb(accent)
    pdf.set_fill_color(r, g, b)
    pdf.rect(MARGIN_MM, bar_y, 3.5, line_h, style="F")
    # Fondo muy suave del header
    pdf.set_fill_color(
        int(r + (255 - r) * 0.92),
        int(g + (255 - g) * 0.92),
        int(b + (255 - b) * 0.92),
    )
    pdf.rect(MARGIN_MM + 3.5, bar_y, pdf.w - 2 * MARGIN_MM - 3.5, line_h, style="F")
    # Texto
    pdf.set_xy(MARGIN_MM + 6, bar_y + (line_h - font_size * 0.35) / 2)
    pdf.set_font("Helvetica", "B", font_size)
    pdf.set_text_color(r, g, b)
    pdf.cell(0, font_size * 0.35, _sanitize_for_helvetica(title), ln=True)
    pdf.set_text_color(*_rgb(COLORS["text"]))
    pdf.set_y(bar_y + line_h + 3)
    return pdf.get_y()


def _draw_top_picks_summary(
    pdf: FPDF,
    match_list: List[Dict[str, Any]],
    stats_by_fixture: Dict[str, Any],
) -> None:
    """Caja resaltada con los picks de mayor confianza del análisis."""
    picks: List[Tuple[float, str, str, str]] = []
    for m in match_list:
        fid = m.get("fixture_id")
        s = (stats_by_fixture.get(fid)
             or stats_by_fixture.get(str(fid) if fid is not None else "")
             or {})
        vb = str(s.get("value_bet") or "")
        if vb not in ("1", "X", "2"):
            continue
        probs = [_norm_prob(s.get(k)) for k in ("prob_home_win", "prob_draw", "prob_away_win")]
        valid = [p for p in probs if p is not None]
        if not valid:
            continue
        max_prob = max(valid)
        if max_prob < 0.48:
            continue
        home = m.get("home_team") or m.get("home") or "Local"
        away = m.get("away_team") or m.get("away") or "Visitante"
        picks.append((max_prob, vb, home, away))

    if not picks:
        return

    picks.sort(reverse=True)
    row_h = 6.5
    box_h = 9 + len(picks) * row_h + 3
    box_x, box_y = MARGIN_MM, pdf.get_y()
    box_w = pdf.w - 2 * MARGIN_MM

    # Fondo amarillo claro
    pdf.set_fill_color(255, 252, 220)
    pdf.rect(box_x, box_y, box_w, box_h, style="F")
    # Borde ámbar
    r, g, b = _rgb(_ACCENT_CONSENSUS)
    pdf.set_draw_color(r, g, b)
    pdf.set_line_width(0.6)
    pdf.rect(box_x, box_y, box_w, box_h, style="D")
    # Barra izquierda ámbar
    pdf.set_fill_color(r, g, b)
    pdf.rect(box_x, box_y, 4, box_h, style="F")
    pdf.set_line_width(0.4)

    # Título de la caja
    pdf.set_xy(box_x + 7, box_y + 2)
    pdf.set_font("Helvetica", "B", 9)
    pdf.set_text_color(r, g, b)
    n_total = len(match_list)
    pdf.cell(0, 5, _sanitize_for_helvetica(f"PICKS DESTACADOS - {len(picks)} de {n_total} partidos con mayor confianza estadistica"), ln=True)

    # Una línea por pick
    _result_labels = {"1": "Victoria local", "X": "Empate", "2": "Victoria visitante"}
    _confidence_dots = {True: "High", False: "Mid"}
    for prob, vb, home, away in picks:
        pdf.set_x(box_x + 7)
        high = prob >= 0.62
        dot_color = _ACCENT_ALFRED if vb == "1" else (_ACCENT_CONSENSUS if vb == "X" else (0.78, 0.22, 0.18))
        pdf.set_fill_color(*_rgb(dot_color))
        pdf.set_text_color(255, 255, 255)
        pdf.set_font("Helvetica", "B", 7)
        label = _result_labels.get(vb, vb)
        pdf.cell(22, row_h - 1, label, border=0, fill=True, align="C")
        pdf.set_text_color(*_rgb(COLORS["text"]))
        pdf.set_font("Helvetica", "", 8)
        conf_str = "Alta" if high else "Media"
        match_text = _sanitize_for_helvetica(
            f"  {home} vs {away}  ({prob:.0%} - confianza {conf_str})"
        )
        pdf.cell(0, row_h - 1, match_text[:70], ln=True)

    pdf.set_draw_color(*_rgb(COLORS["line"]))
    pdf.set_y(box_y + box_h + 5)


def _draw_1x2_bars(
    pdf: FPDF, x: float, y: float, w: float, h: float,
    p1: Optional[float], px: Optional[float], p2: Optional[float],
) -> None:
    """Barras horizontales 1-X-2 (azul / gris / rojo)."""
    if p1 is None or px is None or p2 is None:
        return
    total = p1 + px + p2 or 1
    p1, px, p2 = p1 / total, px / total, p2 / total
    acc = 0.0
    for prob, color_key in [(p1, "local"), (px, "draw"), (p2, "away")]:
        w_seg = w * prob
        pdf.set_fill_color(*_rgb(COLORS[color_key]))
        pdf.rect(x + acc, y, w_seg, h, style="F")
        acc += w_seg
    pdf.set_text_color(*_rgb(COLORS["text"]))


def _draw_gauge(
    pdf: FPDF, x: float, y: float, w: float, h: float,
    value: float, max_val: float = GAUGE_MAX_GOALS,
) -> None:
    """Barra de nivel para goles esperados (verde/amarillo/rojo)."""
    value = value or 0
    fill_w = min(1.0, max(0, value / max_val)) * w
    pdf.set_draw_color(200, 200, 200)
    pdf.rect(x, y, w, h, style="D")
    if fill_w <= 0:
        pdf.set_text_color(*_rgb(COLORS["text"]))
        return
    if value < GAUGE_LOW:
        pdf.set_fill_color(80, 180, 80)
    elif value < GAUGE_MID:
        pdf.set_fill_color(220, 200, 60)
    else:
        pdf.set_fill_color(220, 80, 60)
    pdf.rect(x, y, fill_w, h, style="F")
    pdf.set_text_color(*_rgb(COLORS["text"]))


def _resolve_league_code(m: Dict[str, Any]) -> Optional[str]:
    """Obtiene league_code del partido (campo o desde nombre)."""
    code = m.get("league_code") or m.get("league_id")
    if code:
        return code
    name = m.get("league_name") or m.get("league")
    if not name:
        return None
    try:
        from config import get_league_code_from_name
        return get_league_code_from_name(str(name))
    except Exception:
        return None


def _format_cell_value(val: Any, as_pct: bool, as_float: bool) -> str:
    """Formatea valor para celda (solo ASCII para Helvetica)."""
    if val is None or val == "":
        return "-"
    if as_float and isinstance(val, (int, float)):
        return f"{float(val):.1f}"
    if as_pct and isinstance(val, (int, float)):
        return f"{val:.0%}"
    return str(val)[:10]


def _draw_stats_table(
    pdf: FPDF,
    left_x: float, col_w: float,
    p1: Any, px: Any, p2: Any, xg: Any,
    cs_h: Any, cs_a: Any, btts: Any, over25: Any,
) -> float:
    """Dibuja tabla Metrica | Local | Empate | Visit. | Total. Devuelve y final."""
    widths = (col_w * 0.28, col_w * 0.20, col_w * 0.18, col_w * 0.18, col_w * 0.16)
    w0, w1, w2, w3, w4 = widths
    pdf.set_font("Helvetica", "B", 8)
    pdf.set_fill_color(220, 220, 220)
    pdf.cell(w0, 5, "Metrica", border=1, fill=True)
    pdf.cell(w1, 5, "Local", border=1, fill=True)
    pdf.cell(w2, 5, "Empate", border=1, fill=True)
    pdf.cell(w3, 5, "Visit.", border=1, fill=True)
    pdf.cell(w4, 5, "Total", border=1, fill=True, ln=True)
    pdf.set_font("Helvetica", "", 8)
    row_high_rgb = _rgb(COLORS["row_high"])

    def should_fill(col: int, v1: Any, v2: Any, v3: Any, v4: Any, hi: Optional[int]) -> bool:
        if hi is not None and col == hi:
            return True
        val = (v1, v2, v3, v4)[col] if col < 4 else v4
        return val is not None and isinstance(val, (int, float)) and float(val) >= HIGH_PROB_THRESHOLD

    rows = [
        ("1X2", p1, px, p2, None, None, False),
        ("Goles esp.", None, None, None, xg, None, True),
        ("CS local", cs_h, None, None, None, 1, False),
        ("CS visit.", None, None, cs_a, None, 3, False),
        ("BTTS", None, None, None, btts, 4, False),
        ("Over 2.5", None, None, None, over25, 4, False),
    ]
    for lbl, v1, v2, v3, v4, high_idx, total_float in rows:
        pdf.set_x(left_x)
        t1 = _format_cell_value(v1, as_pct=True, as_float=False)
        t2 = _format_cell_value(v2, as_pct=True, as_float=False)
        t3 = _format_cell_value(v3, as_pct=True, as_float=False)
        t4 = _format_cell_value(v4, as_pct=not total_float, as_float=total_float)
        for i, (w, t) in enumerate([(w0, lbl), (w1, t1), (w2, t2), (w3, t3), (w4, t4)]):
            fill = should_fill(i, v1, v2, v3, v4, high_idx)
            if fill:
                pdf.set_fill_color(*row_high_rgb)
            pdf.cell(w, 5, t[:10], border=1, fill=fill)
            if fill:
                pdf.set_fill_color(255, 255, 255)
        pdf.ln()
    return pdf.get_y()


def _draw_badges(
    pdf: FPDF, right_x: float,
    cs_h: Any, cs_a: Any, btts: Any, over25: Any,
) -> None:
    """Badges OK/BAJO para CS, BTTS, Over 2.5."""
    pdf.set_x(right_x)
    for name, val, umbral in [
        ("CS L", cs_h, CS_HIGH_THRESHOLD),
        ("CS V", cs_a, CS_HIGH_THRESHOLD),
        ("BTTS", btts, HIGH_PROB_THRESHOLD),
        ("O2.5", over25, HIGH_PROB_THRESHOLD),
    ]:
        if val is None:
            continue
        alto = val >= umbral
        pdf.set_font("Helvetica", "B", 8)
        c = COLORS["badge_alto"] if alto else COLORS["badge_bajo"]
        pdf.set_fill_color(*_rgb(c))
        pdf.set_text_color(255, 255, 255)
        pdf.cell(8, 4, "OK" if alto else "BAJO", border=0, fill=True, align="C")
        pdf.set_text_color(*_rgb(COLORS["text"]))
        pdf.set_font("Helvetica", "", 8)
        pdf.cell(2, 4, "")
        pdf.cell(6, 4, f"{name}:{val:.0%}")
    pdf.ln(5)


def _draw_match_header(
    pdf: FPDF, m: Dict[str, Any], match_label: str,
    league_code: Optional[str], logo_path: Optional[str],
) -> None:
    """Logo, título del partido, liga/fecha y alerta de probabilidad extrema."""
    home = m.get("home_team") or m.get("home") or "Local"
    away = m.get("away_team") or m.get("away") or "Visitante"
    y_line = pdf.get_y()
    if logo_path:
        try:
            pdf.image(logo_path, x=MARGIN_MM, y=y_line, w=LOGO_SIZE_MM, h=LOGO_SIZE_MM)
            pdf.set_x(MARGIN_MM + LOGO_SIZE_MM + 2)
        except Exception:
            pdf.set_x(MARGIN_MM)
        try:
            os.unlink(logo_path)
        except Exception:
            pass
    else:
        pdf.set_x(MARGIN_MM)
    pdf.set_font("Helvetica", "B", 14)
    pdf.set_text_color(*_rgb(COLORS["text"]))
    pdf.cell(0, 8, f"{match_label}  {home}  vs  {away}", ln=True)
    p1, px, p2 = m.get("prob_home_win"), m.get("prob_draw"), m.get("prob_away_win")
    extreme = None
    if px is not None and px >= EXTREME_PROB_THRESHOLD:
        extreme = f" Atencion: empate muy alto ({px:.0%})."
    elif p1 is not None and p1 >= EXTREME_PROB_THRESHOLD:
        extreme = f" Atencion: victoria local muy alta ({p1:.0%})."
    elif p2 is not None and p2 >= EXTREME_PROB_THRESHOLD:
        extreme = f" Atencion: victoria visitante muy alta ({p2:.0%})."
    pdf.set_font("Helvetica", "", 10)
    league = m.get("league_name") or m.get("league") or "-"
    date_str = m.get("date") or "-"
    pdf.cell(0, 5, f"Liga: {league}  |  Fecha: {date_str}", ln=True)
    if extreme:
        pdf.set_text_color(200, 140, 0)
        pdf.set_font("Helvetica", "I", 9)
        pdf.cell(0, 5, extreme, ln=True)
        pdf.set_text_color(*_rgb(COLORS["text"]))
        pdf.set_font("Helvetica", "", 10)
    pdf.ln(3)


def _draw_match_block(pdf: FPDF, m: Dict[str, Any], match_label: str) -> None:
    """Bloque modular por partido: cabecera, tabla stats, barras 1X2, gauge y badges."""
    league_code = _resolve_league_code(m)
    logo_path = _fetch_league_logo_path(league_code) if league_code else None
    _draw_match_header(pdf, m, match_label, league_code, logo_path)
    y_start = pdf.get_y()
    col_w = (pdf.w - 2 * MARGIN_MM) / 2
    gap = 4
    left_x = MARGIN_MM
    right_x = MARGIN_MM + col_w + gap
    p1, px, p2 = m.get("prob_home_win"), m.get("prob_draw"), m.get("prob_away_win")
    xg = m.get("expected_goals")
    cs_h = m.get("clean_sheet_home")
    cs_a = m.get("clean_sheet_away")
    btts = m.get("prob_btts")
    over25 = m.get("prob_over25")
    y_after_table = _draw_stats_table(pdf, left_x, col_w, p1, px, p2, xg, cs_h, cs_a, btts, over25)
    # Columna derecha: 1X2, gauge, badges
    pdf.set_font("Helvetica", "", 8)
    pdf.set_xy(right_x, y_start)
    pdf.cell(20, 4, "1-X-2 (Azul=Local, Gris=Empate, Rojo=Visit.)", ln=True)
    _draw_1x2_bars(pdf, right_x, pdf.get_y(), col_w - 20, 6, p1, px, p2)
    pdf.ln(6)
    pdf.set_x(right_x)
    pdf.cell(20, 4, "Goles esp. (termometro)", ln=True)
    _draw_gauge(pdf, right_x, pdf.get_y(), col_w - 20, 5, float(xg) if xg is not None else 0)
    pdf.ln(6)
    _draw_badges(pdf, right_x, cs_h, cs_a, btts, over25)
    y_right_end = pdf.get_y()
    pdf.set_y(max(y_after_table, y_right_end) + 4)
    _draw_separator(pdf, pdf.get_y())
    pdf.ln(4)


def _sanitize_for_helvetica(text: str) -> str:
    """Reemplaza caracteres Unicode no soportados por Helvetica (Latin-1) por equivalentes seguros."""
    if not text:
        return text
    replacements = {
        "\u2014": "-",   # em dash —
        "\u2013": "-",   # en dash –
        "\u2019": "'",   # right single quote '
        "\u2018": "'",   # left single quote '
        "\u201c": '"',   # left double quote "
        "\u201d": '"',   # right double quote "
        "\u2026": "...", # ellipsis …
        "\u00a0": " ",   # non-breaking space
    }
    result = []
    for c in text:
        if c in replacements:
            result.append(replacements[c])
        elif ord(c) <= 255:
            # Latin-1 (incluye á, ñ, etc.): mantener para español
            result.append(c)
        else:
            result.append(replacements.get(c, "?"))
    return "".join(result)


def _draw_match_list_short(pdf: FPDF, matches: List[Dict[str, Any]]) -> None:
    """Lista completa de partidos incluidos (nombre local vs visitante, liga, fecha)."""
    pdf.set_font("Helvetica", "", 10)
    pdf.set_text_color(*_rgb(COLORS["text"]))
    for i, m in enumerate(matches, 1):
        home = _sanitize_for_helvetica(m.get("home_team") or m.get("home") or "Local")
        away = _sanitize_for_helvetica(m.get("away_team") or m.get("away") or "Visitante")
        league = _sanitize_for_helvetica(m.get("league_name") or m.get("league") or "")
        date_str = _format_date_short(m.get("date")) or str(m.get("date") or "")
        pdf.cell(0, 5, _sanitize_for_helvetica(f"  {i}. {home} vs {away}  |  {league}  |  {date_str}"), ln=True)
    pdf.ln(4)


def _format_date_short(date_val: Any) -> str:
    """Formatea fecha para tabla compacta (ej. 26/02 17:00 — máx 11 chars)."""
    if not date_val:
        return ""
    s = str(date_val).strip()
    try:
        from datetime import datetime
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt.strftime("%d/%m %H:%M")
    except Exception:
        return s[:11] if len(s) > 11 else s


def _norm_prob(val: Any) -> Optional[float]:
    """Convierte probabilidad a 0-1 para formatear con .0%. Acepta 0-1 o 0-100 (el LLM a veces devuelve 32 en vez de 0.32)."""
    if val is None:
        return None
    try:
        v = float(val)
        if v > 1.0:
            return v / 100.0
        return v
    except (TypeError, ValueError):
        return None


def _draw_final_recommendation_table(
    pdf: FPDF,
    match_list: List[Dict[str, Any]],
    stats_by_fixture: Dict[str, Any],
) -> None:
    """Dibuja el cuadro final: una fila por partido con columnas coloreadas."""
    if not match_list:
        return
    col_w = pdf.w - 2 * MARGIN_MM
    # Columna fecha ampliada a 0.11; local/visit. a 0.13; resto igual
    w = [
        col_w * 0.13,  # Local
        col_w * 0.11,  # Visit.
        col_w * 0.11,  # Fecha  (11 chars: "26/02 17:00")
        col_w * 0.055, # V.1
        col_w * 0.045, # X
        col_w * 0.055, # V.2
        col_w * 0.055, # xG
        col_w * 0.055, # CS L
        col_w * 0.055, # CS V
        col_w * 0.055, # BTTS
        col_w * 0.055, # O2.5
        col_w * 0.08,  # Value (coloreado)
    ]
    headers = ["Local", "Visit.", "Fecha", "V.1", "X", "V.2", "xG", "CS L", "CS V", "BTTS", "O2.5", "Value"]
    pdf.set_font("Helvetica", "B", 7)
    pdf.set_fill_color(45, 55, 72)           # header oscuro
    pdf.set_text_color(255, 255, 255)
    for wi, h in zip(w, headers):
        pdf.cell(wi, 5.5, _sanitize_for_helvetica(h), border=1, fill=True, align="C")
    pdf.ln()

    def _stats_for_fixture(fid: Any) -> Dict[str, Any]:
        if fid is None:
            return {}
        s = stats_by_fixture.get(fid)
        if s is not None:
            return s
        try:
            return stats_by_fixture.get(int(fid)) or {}
        except (TypeError, ValueError):
            return stats_by_fixture.get(str(fid)) or {}

    # Value-bet color map
    _VALUE_COLORS = {
        "1": (0.2, 0.45, 0.85),    # azul  (local)
        "X": (0.35, 0.35, 0.40),   # gris  (empate)
        "2": (0.78, 0.22, 0.18),   # rojo  (visitante)
    }
    _row_even = (248, 249, 252)
    _row_odd  = (255, 255, 255)

    for idx, m in enumerate(match_list):
        fid = m.get("fixture_id")
        s = _stats_for_fixture(fid)
        home   = _sanitize_for_helvetica((m.get("home_team") or m.get("home") or "-")[:13])
        away   = _sanitize_for_helvetica((m.get("away_team") or m.get("away") or "-")[:13])
        date_s = _sanitize_for_helvetica((_format_date_short(m.get("date")) or "-")[:11])
        p1, px, p2 = _norm_prob(s.get("prob_home_win")), _norm_prob(s.get("prob_draw")), _norm_prob(s.get("prob_away_win"))
        xg     = s.get("expected_goals")
        cs_h   = _norm_prob(s.get("clean_sheet_home"))
        cs_a   = _norm_prob(s.get("clean_sheet_away"))
        btts   = _norm_prob(s.get("prob_btts"))
        over25 = _norm_prob(s.get("prob_over25"))
        vb     = str(s.get("value_bet") or "-")

        row_bg = _row_even if idx % 2 == 0 else _row_odd
        pdf.set_text_color(*_rgb(COLORS["text"]))

        def _cell(wi: float, text: str, align: str = "L") -> None:
            pdf.set_fill_color(*row_bg)
            pdf.cell(wi, 5, _sanitize_for_helvetica(str(text))[:13], border=1, fill=True, align=align)

        pdf.set_font("Helvetica", "", 7)
        _cell(w[0], home)
        _cell(w[1], away)
        _cell(w[2], date_s, "C")
        _cell(w[3], f"{p1:.0%}" if p1 is not None else "-", "C")
        _cell(w[4], f"{px:.0%}" if px is not None else "-", "C")
        _cell(w[5], f"{p2:.0%}" if p2 is not None else "-", "C")
        _cell(w[6], f"{xg:.1f}" if xg is not None else "-", "C")
        _cell(w[7], f"{cs_h:.0%}" if cs_h is not None else "-", "C")
        _cell(w[8], f"{cs_a:.0%}" if cs_a is not None else "-", "C")
        _cell(w[9], f"{btts:.0%}" if btts is not None else "-", "C")
        _cell(w[10], f"{over25:.0%}" if over25 is not None else "-", "C")

        # Value: celda coloreada según resultado recomendado
        vb_color = _VALUE_COLORS.get(vb)
        if vb_color:
            pdf.set_fill_color(*_rgb(vb_color))
            pdf.set_text_color(255, 255, 255)
            pdf.set_font("Helvetica", "B", 7)
            pdf.cell(w[11], 5, vb, border=1, fill=True, align="C")
            pdf.set_text_color(*_rgb(COLORS["text"]))
        else:
            _cell(w[11], vb, "C")
        pdf.ln()
    pdf.ln(3)


def _draw_tactical_stats_table(
    pdf: FPDF,
    match_list: List[Dict[str, Any]],
    stats_by_fixture: Dict[str, Any],
) -> None:
    """
    Tabla secundaria de métricas tácticas (promedios por partido):
    Local | Visitante | Tarj.L | Tarj.V | Disp.L | Disp.V | Offs.L | Offs.V

    Solo se renderiza cuando al menos un partido tiene datos tácticos.
    """
    def _get_stats(fid: Any) -> Dict[str, Any]:
        if fid is None:
            return {}
        s = stats_by_fixture.get(fid)
        if s is not None:
            return s
        try:
            return stats_by_fixture.get(int(fid)) or {}
        except (TypeError, ValueError):
            return stats_by_fixture.get(str(fid)) or {}

    # Solo renderizar si hay datos tácticos en al menos un partido
    has_data = any(
        _get_stats(m.get("fixture_id")).get("avg_yellow_cards_home") is not None
        or _get_stats(m.get("fixture_id")).get("avg_shots_on_target_home") is not None
        or _get_stats(m.get("fixture_id")).get("avg_offsides_home") is not None
        for m in match_list
    )
    if not has_data:
        return

    col_w = pdf.w - 2 * MARGIN_MM
    # Anchos: Local(0.16), Visit(0.14), Tarj.L(0.09), Tarj.V(0.09), Disp.L(0.11), Disp.V(0.11), Offs.L(0.09), Offs.V(0.09) = ~0.88
    # Gap restante distribuido uniformemente
    w = [
        col_w * 0.165,  # Local
        col_w * 0.145,  # Visitante
        col_w * 0.09,   # Tarj. L
        col_w * 0.09,   # Tarj. V
        col_w * 0.115,  # Disp. L
        col_w * 0.115,  # Disp. V
        col_w * 0.09,   # Offs. L
        col_w * 0.09,   # Offs. V
    ]

    # Fila 1: headers de grupo (span visual)
    pdf.set_font("Helvetica", "B", 7)
    pdf.set_fill_color(60, 72, 88)
    pdf.set_text_color(255, 255, 255)
    w_teams = w[0] + w[1]
    w_cards = w[2] + w[3]
    w_shots = w[4] + w[5]
    w_offs  = w[6] + w[7]
    pdf.cell(w_teams, 5, "Partido", border=1, fill=True, align="C")
    pdf.cell(w_cards, 5, "Tarjetas amarillas / ptdo", border=1, fill=True, align="C")
    pdf.cell(w_shots, 5, "Disparos al arco / ptdo", border=1, fill=True, align="C")
    pdf.cell(w_offs,  5, "Offsides / ptdo", border=1, fill=True, align="C")
    pdf.ln()

    # Fila 2: sub-headers
    pdf.set_fill_color(45, 55, 72)
    sub_headers = ["Local", "Visit.", "L", "V", "L", "V", "L", "V"]
    for wi, h in zip(w, sub_headers):
        pdf.cell(wi, 4.5, h, border=1, fill=True, align="C")
    pdf.ln()

    # Filas de datos
    _row_even = (248, 249, 252)
    _row_odd  = (255, 255, 255)
    pdf.set_text_color(*_rgb(COLORS["text"]))

    for idx, m in enumerate(match_list):
        fid = m.get("fixture_id")
        s = _get_stats(fid)
        home = _sanitize_for_helvetica((m.get("home_team") or m.get("home") or "-")[:14])
        away = _sanitize_for_helvetica((m.get("away_team") or m.get("away") or "-")[:14])

        cards_h  = s.get("avg_yellow_cards_home")
        cards_a  = s.get("avg_yellow_cards_away")
        shots_h  = s.get("avg_shots_on_target_home")
        shots_a  = s.get("avg_shots_on_target_away")
        offside_h = s.get("avg_offsides_home")
        offside_a = s.get("avg_offsides_away")

        row_bg = _row_even if idx % 2 == 0 else _row_odd
        pdf.set_font("Helvetica", "", 7)
        pdf.set_fill_color(*row_bg)

        def _tc(wi: float, text: str, align: str = "C") -> None:
            pdf.cell(wi, 5, _sanitize_for_helvetica(str(text))[:10], border=1, fill=True, align=align)

        _tc(w[0], home, "L")
        _tc(w[1], away, "L")

        # Tarjetas: colorear si > 2.5 por partido (partido potencialmente caliente)
        for val, wi in [(cards_h, w[2]), (cards_a, w[3])]:
            if val is not None:
                if val > 2.5:
                    pdf.set_fill_color(255, 235, 200)  # naranja suave = muchas tarjetas
                pdf.cell(wi, 5, f"{val:.1f}", border=1, fill=True, align="C")
                pdf.set_fill_color(*row_bg)
            else:
                pdf.cell(wi, 5, "-", border=1, fill=True, align="C")

        # Disparos al arco: colorear si > 5 (equipo muy ofensivo)
        for val, wi in [(shots_h, w[4]), (shots_a, w[5])]:
            if val is not None:
                if val > 5.0:
                    pdf.set_fill_color(220, 240, 255)  # azul suave = muy ofensivo
                pdf.cell(wi, 5, f"{val:.1f}", border=1, fill=True, align="C")
                pdf.set_fill_color(*row_bg)
            else:
                pdf.cell(wi, 5, "-", border=1, fill=True, align="C")

        # Offsides: colorear si > 3 (línea muy alta)
        for val, wi in [(offside_h, w[6]), (offside_a, w[7])]:
            if val is not None:
                if val > 3.0:
                    pdf.set_fill_color(230, 255, 230)  # verde suave = línea alta
                pdf.cell(wi, 5, f"{val:.1f}", border=1, fill=True, align="C")
                pdf.set_fill_color(*row_bg)
            else:
                pdf.cell(wi, 5, "-", border=1, fill=True, align="C")

        pdf.ln()

    # Leyenda compacta
    pdf.set_font("Helvetica", "I", 6.5)
    pdf.set_text_color(100, 100, 110)
    pdf.set_x(MARGIN_MM)
    naranja_box = chr(0xA0)  # placeholder, dibujamos manualmente
    pdf.set_fill_color(255, 235, 200)
    pdf.rect(pdf.get_x(), pdf.get_y() + 1, 3, 3, style="F")
    pdf.set_x(pdf.get_x() + 4)
    pdf.cell(30, 4, ">2.5 tarj/ptdo", ln=False)
    pdf.set_fill_color(220, 240, 255)
    pdf.rect(pdf.get_x(), pdf.get_y() + 1, 3, 3, style="F")
    pdf.set_x(pdf.get_x() + 4)
    pdf.cell(30, 4, ">5 disp/ptdo", ln=False)
    pdf.set_fill_color(230, 255, 230)
    pdf.rect(pdf.get_x(), pdf.get_y() + 1, 3, 3, style="F")
    pdf.set_x(pdf.get_x() + 4)
    pdf.cell(0, 4, ">3 offsides/ptdo", ln=True)
    pdf.set_text_color(*_rgb(COLORS["text"]))
    pdf.ln(2)


def _strip_grok_stats_block(text: str) -> str:
    """
    Quita del texto el bloque GROK_STATS_JSON ... END_GROK_STATS (y el JSON en medio)
    para que no aparezca en el PDF. Reginald a veces incluye ese bloque en la respuesta
    y si el parser falla se guarda el raw; así el PDF siempre muestra solo el análisis.
    """
    if not text or not text.strip():
        return text
    start_marker = "GROK_STATS_JSON"
    end_marker = "END_GROK_STATS"
    if start_marker in text and end_marker in text:
        start_i = text.find(start_marker)
        end_i = text.find(end_marker) + len(end_marker)
        text = (text[:start_i].rstrip() + "\n\n" + text[end_i:].lstrip()).strip()
    return text.strip()


def _draw_grok_analysis(pdf: FPDF, grok_analysis: str) -> None:
    """Vierte el análisis (Alfred/Reginald) en el PDF (títulos en negrita, resto normal)."""
    if not (grok_analysis or grok_analysis.strip()):
        return
    text = _strip_grok_stats_block(grok_analysis.strip())
    if not text:
        return
    w = pdf.w - 2 * MARGIN_MM
    line_height_body = 5
    line_height_h3 = 6
    line_height_h2 = 7
    line_height_h1 = 8

    def _ensure_margin():
        pdf.set_x(MARGIN_MM)

    def _maybe_new_page():
        if pdf.get_y() > pdf.h - 30:
            pdf.add_page()
            pdf.set_xy(MARGIN_MM, MARGIN_MM)

    for raw_line in text.split("\n"):
        line = raw_line.strip()
        if not line:
            _ensure_margin()
            pdf.ln(4)
            continue
        line = _sanitize_for_helvetica(line)
        _ensure_margin()
        _maybe_new_page()

        if line.startswith("### "):
            pdf.set_font("Helvetica", "B", 11)
            pdf.set_text_color(*_rgb(COLORS["text"]))
            pdf.ln(3)
            _ensure_margin()
            pdf.multi_cell(w, line_height_h3, line[4:].strip(), align="L")
            pdf.set_font("Helvetica", "", 9)
        elif line.startswith("## "):
            pdf.set_font("Helvetica", "B", 12)
            pdf.set_text_color(*_rgb(COLORS["text"]))
            pdf.ln(4)
            _ensure_margin()
            pdf.multi_cell(w, line_height_h2, line[3:].strip(), align="L")
            pdf.set_font("Helvetica", "", 9)
        elif line.startswith("# "):
            pdf.set_font("Helvetica", "B", 14)
            pdf.set_text_color(*_rgb(COLORS["text"]))
            pdf.ln(4)
            _ensure_margin()
            pdf.multi_cell(w, line_height_h1, line[2:].strip(), align="L")
            pdf.set_font("Helvetica", "", 9)
        else:
            pdf.set_font("Helvetica", "", 9)
            pdf.set_text_color(*_rgb(COLORS["text"]))
            clean = line.replace("**", "").strip()
            pdf.multi_cell(w, line_height_body, clean, align="L")


def generate_proposal_pdf(
    proposal_id: str,
    matches_with_analysis: List[Dict[str, Any]],
    grok_analysis: str,
    output_dir: str = "",
) -> str:
    """
    Genera un PDF con portada, lista de partidos incluidos, y análisis/recomendación (Alfred).
    """
    output_dir = output_dir or str(Path(__file__).resolve().parent / "generated_pdfs")
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, f"propuesta_{proposal_id}.pdf")
    generated_at = datetime.now().strftime("%d/%m/%Y %H:%M")
    pdf = ProposalPDF(generated_at=generated_at)
    pdf.set_auto_page_break(auto=True, margin=MARGIN_MM)
    pdf.set_margins(MARGIN_MM, MARGIN_MM)
    pdf.add_page()
    # Portada
    pdf.set_fill_color(*_rgb(COLORS["header_bg"]))
    pdf.rect(0, 0, pdf.w, PORTADA_HEIGHT_MM, style="F")
    y_after_logo = _draw_forgewin_logo_on_cover(pdf, y_start=6)
    pdf.set_y(y_after_logo)
    pdf.set_font("Helvetica", "B", 24)
    pdf.set_text_color(255, 255, 255)
    pdf.cell(0, 12, "Propuesta de análisis", ln=True, align="C")
    pdf.set_font("Helvetica", "", 14)
    pdf.cell(0, 8, f"ID: {proposal_id}", ln=True, align="C")
    pdf.set_font("Helvetica", "", 12)
    pdf.cell(0, 6, f"Fecha: {generated_at}", ln=True, align="C")
    pdf.set_y(PORTADA_HEIGHT_MM + 5)
    pdf.set_text_color(*_rgb(COLORS["text"]))
    _draw_separator(pdf, pdf.get_y())
    pdf.ln(10)
    # Partidos incluidos
    _draw_section_header(pdf, "Partidos incluidos", font_size=16)
    _draw_match_list_short(pdf, matches_with_analysis)
    # Análisis y recomendación Alfred
    if pdf.get_y() > pdf.h - 50:
        pdf.add_page()
    _draw_section_header(pdf, "Analisis y recomendacion Alfred", font_size=16)
    _draw_grok_analysis(pdf, grok_analysis or "")
    _draw_footer_on_last_page(pdf)
    pdf.output(path)
    return path


def generate_proposal_pdf_three_options(
    full_id: str,
    fixture_to_match: Dict[str, Any],
    selected_fixture_ids: List[int],
    prop_grok: Dict[str, Any],
    prop_gemini: Dict[str, Any],
    consensus: Optional[Dict[str, Any]] = None,
    output_dir: str = "",
) -> str:
    """
    Genera un PDF con tres secciones: Opción 1 (Alfred), Opción 2 (Reginald), Propuesta General 1+2.
    fixture_to_match: dict fixture_id -> match dict (home_team, away_team, date, etc.).
    consensus: opcional, dict con "analysis", "stats_by_fixture", "gemini_opinion", "grok_opinion", "grok_final".
    """
    output_dir = output_dir or str(Path(__file__).resolve().parent / "generated_pdfs")
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, f"propuesta_1_2_3_{full_id}.pdf")
    generated_at = datetime.now().strftime("%d/%m/%Y %H:%M")
    pdf = ProposalPDF(generated_at=generated_at)
    pdf.set_auto_page_break(auto=True, margin=MARGIN_MM)
    pdf.set_margins(MARGIN_MM, MARGIN_MM)
    pdf.add_page()

    pdf.set_fill_color(*_rgb(COLORS["header_bg"]))
    pdf.rect(0, 0, pdf.w, PORTADA_HEIGHT_MM, style="F")
    y_after_logo = _draw_forgewin_logo_on_cover(pdf, y_start=6)
    pdf.set_y(y_after_logo)
    pdf.set_font("Helvetica", "B", 22)
    pdf.set_text_color(255, 255, 255)
    pdf.cell(0, 10, "ForgeWin - Propuesta 1 + 2 + Propuesta General 1+2", ln=True, align="C")
    pdf.set_font("Helvetica", "", 12)
    pdf.cell(0, 6, "Alfred | Reginald | Propuesta General 1+2", ln=True, align="C")
    pdf.set_font("Helvetica", "", 11)
    pdf.cell(0, 6, f"Fecha: {generated_at}", ln=True, align="C")
    pdf.set_y(PORTADA_HEIGHT_MM + 5)
    pdf.set_text_color(*_rgb(COLORS["text"]))
    _draw_separator(pdf, pdf.get_y())
    pdf.ln(8)

    match_list = []
    for fid in selected_fixture_ids:
        m = fixture_to_match.get(fid) or {}
        match_list.append(dict(m, fixture_id=fid))

    # Lista de partidos UNA SOLA VEZ (encabezado del documento)
    _draw_section_header_accented(pdf, "Partidos incluidos", font_size=12, accent=_ACCENT_NEUTRAL)
    _draw_match_list_short(pdf, match_list)
    if pdf.get_y() > pdf.h - 45:
        pdf.add_page()

    # Opción 1: Alfred (azul)
    _draw_section_header_accented(pdf, "Opcion 1 — Alfred", font_size=14, accent=_ACCENT_ALFRED)
    _draw_grok_analysis(pdf, (prop_grok.get("grok_analysis") or "").strip())
    if pdf.get_y() > pdf.h - 45:
        pdf.add_page()

    # Opción 2: Reginald (violeta)
    _draw_section_header_accented(pdf, "Opcion 2 — Reginald", font_size=14, accent=_ACCENT_REGINALD)
    _draw_grok_analysis(pdf, (prop_gemini.get("grok_analysis") or "").strip())
    if pdf.get_y() > pdf.h - 45:
        pdf.add_page()

    # Propuesta General 1+2 (ámbar dorado)
    _draw_section_header_accented(pdf, "Propuesta General 1+2 (consenso final)", font_size=14, accent=_ACCENT_CONSENSUS)
    if consensus and (consensus.get("analysis") or consensus.get("grok_final")):
        # Tabla resumen antes del análisis
        stats_c = consensus.get("stats_by_fixture") or {}
        if stats_c:
            _draw_final_recommendation_table(pdf, match_list, stats_c)
        _draw_grok_analysis(pdf, (consensus.get("analysis") or "").strip())
    else:
        pdf.set_font("Helvetica", "", 10)
        pdf.set_text_color(*_rgb(COLORS["text"]))
        pdf.multi_cell(0, 5, _sanitize_for_helvetica(
            "Genera la Propuesta General 1+2 en la app para incluir aqui el resultado del dialogo Alfred-Reginald."
        ))
    _draw_footer_on_last_page(pdf)
    pdf.output(path)
    return path


def generate_proposal_pdf_final_recommendation(
    full_id: str,
    fixture_to_match: Dict[str, Any],
    selected_fixture_ids: List[int],
    consensus: Dict[str, Any],
    output_dir: str = "",
) -> str:
    """
    Genera un PDF con solo la opinión recomendada (Propuesta General 1+2):
    primero el cuadro de datos por partido, luego el análisis final de Reginald.
    """
    output_dir = output_dir or str(Path(__file__).resolve().parent / "generated_pdfs")
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, f"opinion_recomendada_{full_id}.pdf")
    generated_at = datetime.now().strftime("%d/%m/%Y %H:%M")
    pdf = ProposalPDF(generated_at=generated_at)
    pdf.set_auto_page_break(auto=True, margin=MARGIN_MM)
    pdf.set_margins(MARGIN_MM, MARGIN_MM)
    pdf.add_page()

    pdf.set_fill_color(*_rgb(COLORS["header_bg"]))
    pdf.rect(0, 0, pdf.w, PORTADA_HEIGHT_MM, style="F")
    y_after_logo = _draw_forgewin_logo_on_cover(pdf, y_start=6)
    pdf.set_y(y_after_logo)
    pdf.set_font("Helvetica", "B", 22)
    pdf.set_text_color(255, 255, 255)
    pdf.cell(0, 10, "ForgeWin - Opinion recomendada", ln=True, align="C")
    pdf.set_font("Helvetica", "", 12)
    pdf.cell(0, 6, "Propuesta General 1+2 (recomendacion final)", ln=True, align="C")
    pdf.set_font("Helvetica", "", 11)
    pdf.cell(0, 6, f"Fecha: {generated_at}", ln=True, align="C")
    pdf.set_y(PORTADA_HEIGHT_MM + 5)
    pdf.set_text_color(*_rgb(COLORS["text"]))
    _draw_separator(pdf, pdf.get_y())
    pdf.ln(8)

    match_list = []
    for fid in selected_fixture_ids:
        m = fixture_to_match.get(fid) or {}
        match_list.append(dict(m, fixture_id=fid))

    stats_by_fixture = consensus.get("stats_by_fixture") or {}

    # 1. Lista completa de partidos (siempre primero, todos los partidos)
    _draw_section_header_accented(pdf, f"Partidos incluidos ({len(match_list)})", font_size=12, accent=_ACCENT_NEUTRAL)
    _draw_match_list_short(pdf, match_list)

    # 2. Caja de picks más destacados (filtro visual, complementa la lista)
    _draw_top_picks_summary(pdf, match_list, stats_by_fixture)

    # 3. Tabla estadística de todos los partidos
    _draw_section_header_accented(pdf, "Estadisticas por partido", font_size=13, accent=_ACCENT_CONSENSUS)
    _draw_final_recommendation_table(pdf, match_list, stats_by_fixture)
    _draw_tactical_stats_table(pdf, match_list, fixture_to_match)

    # 4. Análisis completo
    if pdf.get_y() > pdf.h - 50:
        pdf.add_page()
    _draw_section_header_accented(pdf, "Analisis y recomendacion final", font_size=14, accent=_ACCENT_CONSENSUS)
    _draw_grok_analysis(pdf, (consensus.get("analysis") or "").strip())
    _draw_footer_on_last_page(pdf)
    pdf.output(path)
    return path


def generate_proposal_pdf_v2(
    full_id: str,
    fixture_to_match: Dict[str, Any],
    selected_fixture_ids: List[int],
    grok_stats: Dict[str, Any],
    grok_analysis: str,
    gemini_analysis: str,
    output_dir: str = "",
) -> str:
    """
    Genera un PDF ForgeWin V2: tabla de estadísticas (Alfred), análisis Alfred, análisis Reginald.
    El análisis de Reginald es el que se destaca como descargable.
    """
    output_dir = output_dir or str(Path(__file__).resolve().parent / "generated_pdfs")
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, f"forgewin_v2_{full_id}.pdf")
    generated_at = datetime.now().strftime("%d/%m/%Y %H:%M")
    pdf = ProposalPDF(generated_at=generated_at)
    pdf.set_auto_page_break(auto=True, margin=MARGIN_MM)
    pdf.set_margins(MARGIN_MM, MARGIN_MM)
    pdf.add_page()

    pdf.set_fill_color(*_rgb(COLORS["header_bg"]))
    pdf.rect(0, 0, pdf.w, PORTADA_HEIGHT_MM, style="F")
    y_after_logo = _draw_forgewin_logo_on_cover(pdf, y_start=6)
    pdf.set_y(y_after_logo)
    pdf.set_font("Helvetica", "B", 22)
    pdf.set_text_color(255, 255, 255)
    pdf.cell(0, 10, "ForgeWin V2", ln=True, align="C")
    pdf.set_font("Helvetica", "", 12)
    pdf.cell(0, 6, "Tabla Alfred + Analisis Alfred + Analisis Reginald", ln=True, align="C")
    pdf.set_font("Helvetica", "", 11)
    pdf.cell(0, 6, f"Fecha: {generated_at}", ln=True, align="C")
    pdf.set_y(PORTADA_HEIGHT_MM + 5)
    pdf.set_text_color(*_rgb(COLORS["text"]))
    _draw_separator(pdf, pdf.get_y())
    pdf.ln(8)

    match_list = []
    for fid in selected_fixture_ids:
        m = fixture_to_match.get(fid) or {}
        match_list.append(dict(m, fixture_id=fid))

    _draw_section_header_accented(pdf, "Datos por partido (Alfred)", font_size=13, accent=_ACCENT_ALFRED)
    _draw_final_recommendation_table(pdf, match_list, grok_stats)

    if pdf.get_y() > pdf.h - 50:
        pdf.add_page()
    _draw_section_header_accented(pdf, "Analisis de los partidos — Alfred", font_size=14, accent=_ACCENT_ALFRED)
    _draw_grok_analysis(pdf, (grok_analysis or "").strip())

    if pdf.get_y() > pdf.h - 50:
        pdf.add_page()
    _draw_section_header_accented(pdf, "Analisis Reginald (sobre tabla y analisis de Alfred)", font_size=14, accent=_ACCENT_REGINALD)
    _draw_grok_analysis(pdf, (gemini_analysis or "").strip())
    _draw_footer_on_last_page(pdf)
    pdf.output(path)
    return path


_ACCENT_PRESS = (0.05, 0.48, 0.42)   # verde azulado para fichas de prensa


def generate_journalist_pdf(
    match_title: str,
    subtitle: str,
    analysis_text: str,
    output_path: str = "",
) -> str:
    """
    Genera una ficha de prensa periodística (Asistente de Prensa ForgeWin).
    match_title: ej. "PSG vs Chelsea — Champions League 2025/26"
    subtitle: ej. "Fase de eliminación directa"
    analysis_text: texto markdown de Gemini con las 7 secciones.
    output_path: ruta completa del PDF (si vacía, se guarda en generated_pdfs/).
    """
    if not output_path:
        output_dir = str(Path(__file__).resolve().parent / "generated_pdfs")
        os.makedirs(output_dir, exist_ok=True)
        safe = match_title.lower().replace(" ", "_").replace("—", "").replace("/", "-")[:40]
        output_path = os.path.join(output_dir, f"prensa_{safe}.pdf")

    generated_at = datetime.now().strftime("%d/%m/%Y %H:%M")
    pdf = ProposalPDF(generated_at=generated_at)
    pdf.set_auto_page_break(auto=True, margin=MARGIN_MM)
    pdf.set_margins(MARGIN_MM, MARGIN_MM)
    pdf.add_page()

    # Portada
    pdf.set_fill_color(*_rgb(COLORS["header_bg"]))
    pdf.rect(0, 0, pdf.w, PORTADA_HEIGHT_MM + 10, style="F")
    y_after_logo = _draw_forgewin_logo_on_cover(pdf, y_start=5)
    pdf.set_y(y_after_logo)
    pdf.set_font("Helvetica", "B", 9)
    pdf.set_text_color(180, 210, 200)
    pdf.cell(0, 5, "ASISTENTE DE PRENSA", ln=True, align="C")
    pdf.set_font("Helvetica", "B", 18)
    pdf.set_text_color(255, 255, 255)
    pdf.multi_cell(0, 9, _sanitize_for_helvetica(match_title), align="C")
    pdf.set_font("Helvetica", "", 11)
    pdf.set_text_color(200, 225, 220)
    pdf.cell(0, 6, _sanitize_for_helvetica(subtitle), ln=True, align="C")
    pdf.set_font("Helvetica", "", 9)
    pdf.set_text_color(160, 190, 185)
    pdf.cell(0, 5, f"Generado: {generated_at}", ln=True, align="C")
    pdf.set_y(PORTADA_HEIGHT_MM + 15)
    pdf.set_text_color(*_rgb(COLORS["text"]))
    _draw_separator(pdf, pdf.get_y(), color=_ACCENT_PRESS)
    pdf.ln(8)

    # Contenido: el texto de Gemini con secciones
    _draw_grok_analysis(pdf, analysis_text.strip())

    _draw_footer_on_last_page(pdf)
    pdf.output(output_path)
    return output_path


def generate_proposal_pdf_league_combined(
    full_id: str,
    fixture_to_match: Dict[str, Any],
    selected_fixture_ids: List[int],
    v2_grok_stats: Dict[str, Any],
    v2_grok_analysis: str,
    v2_gemini_analysis: str,
    consensus: Dict[str, Any],
    output_dir: str = "",
) -> str:
    """
    Genera un único PDF con: ForgeWin V2 (tabla Alfred + análisis Alfred + análisis Reginald)
    y ForgeWin - Opinión recomendada (Propuesta General 1+2).
    """
    output_dir = output_dir or str(Path(__file__).resolve().parent / "generated_pdfs")
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, f"forgewin_liga_{full_id}.pdf")
    generated_at = datetime.now().strftime("%d/%m/%Y %H:%M")
    pdf = ProposalPDF(generated_at=generated_at)
    pdf.set_auto_page_break(auto=True, margin=MARGIN_MM)
    pdf.set_margins(MARGIN_MM, MARGIN_MM)
    pdf.add_page()

    pdf.set_fill_color(*_rgb(COLORS["header_bg"]))
    pdf.rect(0, 0, pdf.w, PORTADA_HEIGHT_MM, style="F")
    y_after_logo = _draw_forgewin_logo_on_cover(pdf, y_start=6)
    pdf.set_y(y_after_logo)
    pdf.set_font("Helvetica", "B", 22)
    pdf.set_text_color(255, 255, 255)
    pdf.cell(0, 10, "ForgeWin - Analisis por liga", ln=True, align="C")
    pdf.set_font("Helvetica", "", 12)
    pdf.cell(0, 6, "V2 + Opinion recomendada (Propuesta General 1+2)", ln=True, align="C")
    pdf.set_font("Helvetica", "", 11)
    pdf.cell(0, 6, f"Fecha: {generated_at}", ln=True, align="C")
    pdf.set_y(PORTADA_HEIGHT_MM + 5)
    pdf.set_text_color(*_rgb(COLORS["text"]))
    _draw_separator(pdf, pdf.get_y())
    pdf.ln(8)

    match_list = []
    for fid in selected_fixture_ids:
        m = fixture_to_match.get(fid) or {}
        match_list.append(dict(m, fixture_id=fid))

    # --- Sección 1: Alfred ---
    _draw_section_header_accented(pdf, "Datos por partido (Alfred)", font_size=13, accent=_ACCENT_ALFRED)
    _draw_final_recommendation_table(pdf, match_list, v2_grok_stats)
    _draw_tactical_stats_table(pdf, match_list, fixture_to_match)   # tabla táctica con stats de BD
    if pdf.get_y() > pdf.h - 50:
        pdf.add_page()
    _draw_section_header_accented(pdf, "Analisis de los partidos — Alfred", font_size=12, accent=_ACCENT_ALFRED)
    _draw_grok_analysis(pdf, (v2_grok_analysis or "").strip())
    if pdf.get_y() > pdf.h - 50:
        pdf.add_page()
    _draw_section_header_accented(pdf, "Analisis Reginald", font_size=12, accent=_ACCENT_REGINALD)
    _draw_grok_analysis(pdf, (v2_gemini_analysis or "").strip())

    # --- Sección 2: Propuesta General 1+2 (consenso) ---
    if pdf.get_y() > pdf.h - 50:
        pdf.add_page()
    stats_consensus = consensus.get("stats_by_fixture") or {}
    _draw_section_header_accented(pdf, "Opinion recomendada — Propuesta General 1+2", font_size=14, accent=_ACCENT_CONSENSUS)
    _draw_top_picks_summary(pdf, match_list, stats_consensus)
    _draw_final_recommendation_table(pdf, match_list, stats_consensus)
    _draw_tactical_stats_table(pdf, match_list, fixture_to_match)   # tabla táctica repetida en sección consenso
    if pdf.get_y() > pdf.h - 50:
        pdf.add_page()
    _draw_section_header_accented(pdf, "Analisis y recomendacion final", font_size=12, accent=_ACCENT_CONSENSUS)
    _draw_grok_analysis(pdf, (consensus.get("analysis") or "").strip())

    _draw_footer_on_last_page(pdf)
    pdf.output(path)
    return path
