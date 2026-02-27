"""Generador del reporte PDF de analisis de cartera CxC.

Produce un PDF ejecutivo con una seccion por cada hoja del Excel 02_analisis.
Cada seccion contiene:
    - Encabezado con el nombre del analisis.
    - Explicacion de negocio en lenguaje claro (que significa y que accion sugiere).
    - Grafica a color generada con matplotlib.
    - Tabla resumen con los datos clave.

El PDF se genera en orientacion horizontal (landscape) para que las tablas
anchas quepan correctamente.

Uso desde main.py:
    from src.reporte_pdf import generar_reporte_pdf
    generar_reporte_pdf(analisis_dict, output_path)
"""

from __future__ import annotations

import io
import logging
from pathlib import Path
from typing import Any

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_JUSTIFY
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.platypus import (
    Image,
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

logger = logging.getLogger(__name__)

# ======================================================================
# PALETA DE COLORES
# ======================================================================

_AZUL_CORP   = "#2E4B8F"   # Encabezados principales
_VERDE_CORP  = "#548235"   # Barras vigentes / positivas
_ROJO_CORP   = "#C00000"   # Barras vencidas / negativas
_GRIS_CLARO  = "#F2F2F2"   # Fondo alterno de tabla
_AZUL_CLARO  = "#D9E2F3"   # Banda de encabezado de tabla
_NARANJA     = "#E67E22"   # Acento / tercer valor
_PALETA_BARRAS = [
    "#2E4B8F", "#548235", "#C00000", "#E67E22",
    "#8E44AD", "#17A589", "#D4AC0D", "#922B21",
]

_PAGE_W, _PAGE_H = landscape(A4)
_MARGIN = 1.5 * cm

# ======================================================================
# ESTILOS
# ======================================================================

_BASE_STYLES = getSampleStyleSheet()

_STYLE_TITULO = ParagraphStyle(
    "TituloSeccion",
    parent=_BASE_STYLES["Heading1"],
    fontSize=14,
    textColor=colors.HexColor(_AZUL_CORP),
    spaceAfter=4,
    spaceBefore=0,
    leading=18,
)
_STYLE_SUBTITULO = ParagraphStyle(
    "Subtitulo",
    parent=_BASE_STYLES["Normal"],
    fontSize=9,
    textColor=colors.HexColor("#666666"),
    spaceAfter=6,
    italic=True,
)
_STYLE_EXPLICACION = ParagraphStyle(
    "Explicacion",
    parent=_BASE_STYLES["Normal"],
    fontSize=9,
    leading=13,
    textColor=colors.black,
    spaceAfter=8,
    alignment=TA_JUSTIFY,
)
_STYLE_NOTA = ParagraphStyle(
    "Nota",
    parent=_BASE_STYLES["Normal"],
    fontSize=8,
    leading=11,
    textColor=colors.HexColor("#555555"),
    spaceAfter=4,
    italic=True,
)

# ======================================================================
# HELPERS DE GRAFICA
# ======================================================================

_FIG_W_IN  = (_PAGE_W - 2 * _MARGIN) / (72 * 0.393701)  # puntos -> pulgadas
_FIG_H_IN  = 5.5   # pulgadas para graficas grandes  (era 7.5 — excedia el frame de ~484 pts)
_FIG_H_SM  = 4.0   # pulgadas para graficas compactas (era 5.5)


def _fig_a_imagen(fig: Any, alto: float = _FIG_H_IN) -> Image:
    """Convierte una figura matplotlib a un objeto Image de reportlab.

    Args:
        fig: Figura matplotlib ya dibujada.
        alto: Alto deseado de la imagen en pulgadas.

    Returns:
        Objeto Image listo para insertar en el PDF.
    """
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    ancho_pts = _PAGE_W - 2 * _MARGIN
    alto_pts  = alto * 72
    return Image(buf, width=ancho_pts, height=alto_pts)


def _fmt_miles(val: float, _pos: Any = None) -> str:
    """Formateador de eje Y con separador de miles y dos decimales."""
    if abs(val) >= 1_000_000:
        return f"{val/1_000_000:,.1f}M"
    if abs(val) >= 1_000:
        return f"{val/1_000:,.0f}K"
    return f"{val:,.0f}"


def _ax_base(ax: Any, titulo: str = "") -> None:
    """Aplica estilo corporativo minimalista a un eje."""
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_color("#CCCCCC")
    ax.spines["bottom"].set_color("#CCCCCC")
    ax.tick_params(colors="#555555", labelsize=7)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(_fmt_miles))
    if titulo:
        ax.set_title(titulo, fontsize=9, color=_AZUL_CORP, pad=6)


# ======================================================================
# HELPERS DE TABLA
# ======================================================================

_TABLA_ESTILO = TableStyle([
    ("BACKGROUND",    (0, 0), (-1, 0),  colors.HexColor(_AZUL_CLARO)),
    ("TEXTCOLOR",     (0, 0), (-1, 0),  colors.HexColor(_AZUL_CORP)),
    ("FONTNAME",      (0, 0), (-1, 0),  "Helvetica-Bold"),
    ("FONTSIZE",      (0, 0), (-1, 0),  7),
    ("ALIGN",         (0, 0), (-1, 0),  "CENTER"),
    ("BOTTOMPADDING", (0, 0), (-1, 0),  5),
    ("TOPPADDING",    (0, 0), (-1, 0),  5),
    ("FONTNAME",      (0, 1), (-1, -1), "Helvetica"),
    ("FONTSIZE",      (0, 1), (-1, -1), 7),
    ("ALIGN",         (1, 1), (-1, -1), "RIGHT"),
    ("ALIGN",         (0, 1), (0, -1),  "LEFT"),
    ("ROWBACKGROUNDS", (0, 1), (-1, -1),
     [colors.white, colors.HexColor(_GRIS_CLARO)]),
    ("GRID",          (0, 0), (-1, -1), 0.3, colors.HexColor("#CCCCCC")),
    ("TOPPADDING",    (0, 1), (-1, -1), 3),
    ("BOTTOMPADDING", (0, 1), (-1, -1), 3),
])


def _df_a_tabla(
    df: pd.DataFrame,
    max_filas: int = 25,
    col_widths: list[float] | None = None,
) -> Table:
    """Convierte un DataFrame a una tabla reportlab.

    Args:
        df: DataFrame a convertir.
        max_filas: Numero maximo de filas de datos a mostrar.
        col_widths: Anchos de columna en puntos. Si es None se distribuyen
            uniformemente.

    Returns:
        Objeto Table con estilo corporativo aplicado.
    """
    df_show = df.head(max_filas).copy()

    # Formatear numeros
    for col in df_show.columns:
        col_u = str(col).upper()
        if col_u in {
            "IMPORTE_TOTAL", "TOTAL_CARGOS", "TOTAL_ABONOS", "SALDO_PENDIENTE",
            "SALDO_TOTAL", "MONTO_TOTAL", "IMPORTE_PROMEDIO", "IMPORTE_MAX",
            "FACTURAS_PAGADAS", "FACTURAS_VIGENTES", "IMPUESTO_TOTAL",
        } or col_u.startswith("FACTURAS_VENCIDAS"):
            df_show[col] = df_show[col].apply(
                lambda v: f"{float(v):>14,.2f}" if pd.notna(v) and v != "" else ""
            )
        elif col_u == "PCT_DEL_TOTAL":
            df_show[col] = df_show[col].apply(
                lambda v: f"{float(v):.2f}%" if pd.notna(v) and v != "" else ""
            )
        elif col_u in {"NUM_DOCUMENTOS", "NUM_CARGOS", "NUM_ABONOS",
                       "NUM_REGISTROS"}:
            df_show[col] = df_show[col].apply(
                lambda v: f"{int(v):,}" if pd.notna(v) and v != "" else ""
            )

    data = [list(df_show.columns)] + df_show.values.tolist()
    ancho_total = _PAGE_W - 2 * _MARGIN
    if col_widths is None:
        w = ancho_total / len(df_show.columns)
        col_widths = [w] * len(df_show.columns)

    tbl = Table(data, colWidths=col_widths, repeatRows=1)
    tbl.setStyle(_TABLA_ESTILO)

    if len(df) > max_filas:
        tbl._nota_truncada = True

    return tbl


# ======================================================================
# GENERADORES DE PAGINA POR SECCION
# ======================================================================


def _seccion_cartera_vencida(
    df: pd.DataFrame,
    story: list,
) -> None:
    """Pagina: Cartera Vencida vs Vigente."""
    story.append(Paragraph("Cartera Vencida vs Vigente", _STYLE_TITULO))
    story.append(Paragraph(
        "Analisis de la cartera abierta · Fuente: movimientos_abiertos_cxc",
        _STYLE_SUBTITULO,
    ))
    story.append(Paragraph(
        "Esta tabla y grafica muestran la division de la cartera activa en dos categorias: "
        "las facturas <b>VIGENTES</b> que aun no han superado su fecha de vencimiento "
        "y representan riesgo futuro bajo, y las <b>VENCIDAS</b> que ya superaron el "
        "plazo de pago y requieren accion inmediata de cobranza. "
        "El porcentaje de cartera vencida sobre el total es el indicador de salud de "
        "cobro mas directo: valores por encima del 30% sugieren revisar la politica de "
        "credito y reforzar las gestiones de cobranza preventiva.",
        _STYLE_EXPLICACION,
    ))

    if df.empty:
        story.append(Paragraph("Sin datos disponibles.", _STYLE_NOTA))
        story.append(PageBreak())
        return

    monedas = df["MONEDA"].unique() if "MONEDA" in df.columns else [""]
    n_mon   = len(monedas)
    fig, axes = plt.subplots(1, n_mon, figsize=(_FIG_W_IN, _FIG_H_SM),
                             squeeze=False)
    fig.patch.set_facecolor("white")

    for idx, moneda in enumerate(sorted(monedas)):
        ax  = axes[0][idx]
        sub = df[df["MONEDA"] == moneda] if "MONEDA" in df.columns else df

        etiquetas = sub["ESTATUS_VENCIMIENTO"].tolist() if "ESTATUS_VENCIMIENTO" in sub.columns else []
        saldos    = sub["SALDO_PENDIENTE"].tolist() if "SALDO_PENDIENTE" in sub.columns else []
        pcts      = sub["PCT_DEL_TOTAL"].tolist() if "PCT_DEL_TOTAL" in sub.columns else []

        bar_colors = [
            _VERDE_CORP if "VIGENTE" in str(e) else _ROJO_CORP
            for e in etiquetas
        ]
        bars = ax.bar(etiquetas, saldos, color=bar_colors, width=0.5, zorder=2)
        ax.set_axisbelow(True)
        ax.yaxis.grid(True, linestyle="--", alpha=0.5, color="#DDDDDD")

        for bar, pct in zip(bars, pcts):
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                bar.get_height() * 1.01,
                f"{pct:.1f}%",
                ha="center", va="bottom", fontsize=8, color=_AZUL_CORP,
                fontweight="bold",
            )
        _ax_base(ax, f"Saldo Pendiente — {moneda}")
        ax.set_xlabel("")
        ax.tick_params(axis="x", labelsize=8)

    fig.tight_layout(pad=2.0)
    story.append(_fig_a_imagen(fig, _FIG_H_SM))
    story.append(Spacer(1, 0.3 * cm))

    tbl = _df_a_tabla(df)
    story.append(tbl)
    story.append(PageBreak())


def _seccion_antiguedad_cartera(
    df: pd.DataFrame,
    story: list,
) -> None:
    """Pagina: Antiguedad de Cartera."""
    story.append(Paragraph("Antiguedad de Cartera", _STYLE_TITULO))
    story.append(Paragraph(
        "Distribucion de saldos pendientes por rango de mora · Fuente: movimientos_abiertos_cxc",
        _STYLE_SUBTITULO,
    ))
    story.append(Paragraph(
        "La antiguedad de cartera clasifica los saldos pendientes de cobro segun cuantos "
        "dias llevan vencidos. Las facturas <b>Por vencer</b> (vigentes) son de bajo riesgo; "
        "a medida que avanza el rango la probabilidad de cobro disminuye y los costos de "
        "gestion aumentan. Una cartera sana concentra la mayoria del importe en los rangos "
        "mas cortos. Si el rango <b>Mas de 120 dias</b> representa mas del 15% del total, "
        "es recomendable revisar la politica de castigo o provision de incobrables y "
        "evaluar acciones legales o de recuperacion.",
        _STYLE_EXPLICACION,
    ))

    if df.empty:
        story.append(Paragraph("Sin datos disponibles.", _STYLE_NOTA))
        story.append(PageBreak())
        return

    monedas = sorted(df["MONEDA"].unique()) if "MONEDA" in df.columns else [""]
    n_mon   = len(monedas)
    fig, axes = plt.subplots(1, n_mon, figsize=(_FIG_W_IN, _FIG_H_SM),
                             squeeze=False)
    fig.patch.set_facecolor("white")

    for idx, moneda in enumerate(monedas):
        ax  = axes[0][idx]
        sub = df[df["MONEDA"] == moneda].copy() if "MONEDA" in df.columns else df.copy()

        rangos   = sub["RANGO_ANTIGUEDAD"].tolist()
        importes = sub["IMPORTE_TOTAL"].tolist()
        pcts     = sub["PCT_DEL_TOTAL"].tolist()

        bar_colors = []
        for r in rangos:
            if "VIGENTE" in str(r) or "vencer" in str(r).lower():
                bar_colors.append(_VERDE_CORP)
            elif "+120" in str(r) or "120" in str(r):
                bar_colors.append(_ROJO_CORP)
            else:
                bar_colors.append(_AZUL_CORP)

        y_pos = range(len(rangos))
        bars  = ax.barh(list(y_pos), importes, color=bar_colors, height=0.6, zorder=2)
        ax.set_yticks(list(y_pos))
        ax.set_yticklabels(rangos, fontsize=7)
        ax.set_axisbelow(True)
        ax.xaxis.grid(True, linestyle="--", alpha=0.5, color="#DDDDDD")
        ax.xaxis.set_major_formatter(mticker.FuncFormatter(_fmt_miles))

        for bar, pct in zip(bars, pcts):
            ax.text(
                bar.get_width() * 1.005,
                bar.get_y() + bar.get_height() / 2,
                f"{pct:.1f}%",
                va="center", ha="left", fontsize=7, color=_AZUL_CORP,
            )

        _ax_base(ax, f"Importe Total — {moneda}")
        ax.spines["left"].set_visible(False)
        ax.tick_params(axis="y", length=0)

    fig.tight_layout(pad=2.0)
    story.append(_fig_a_imagen(fig, _FIG_H_SM))
    story.append(Spacer(1, 0.3 * cm))
    story.append(_df_a_tabla(df))
    story.append(PageBreak())


def _seccion_antiguedad_cliente(
    df: pd.DataFrame,
    moneda: str,
    story: list,
) -> None:
    """Pagina: Antiguedad por Cliente (MXN o USD)."""
    story.append(Paragraph(
        f"Antiguedad por Cliente — {moneda}", _STYLE_TITULO,
    ))
    story.append(Paragraph(
        f"Pivot de saldos abiertos y pagados por cliente en {moneda} · "
        "Fuente: movimientos_totales_cxc",
        _STYLE_SUBTITULO,
    ))
    story.append(Paragraph(
        "Esta vista consolida por cliente el comportamiento de pago historico y "
        "la exposicion actual de credito. <b>FACTURAS_PAGADAS</b> refleja el volumen "
        "de negocio historico cobrado; <b>FACTURAS_VIGENTES</b> el riesgo inmediato "
        "aun dentro de terminos; y las columnas <b>FACTURAS_VENCIDAS</b> revelan "
        "la profundidad de la mora por cliente. Clientes con saldo elevado en rangos "
        "altos (61-90 o +120 dias) deben priorizarse en la agenda de cobranza y "
        "considerarse para revision o suspension de linea de credito.",
        _STYLE_EXPLICACION,
    ))

    if df.empty:
        story.append(Paragraph(f"Sin clientes con operaciones en {moneda}.", _STYLE_NOTA))
        story.append(PageBreak())
        return

    # Grafica: top 15 clientes por SALDO_PENDIENTE
    col_saldo = "SALDO_PENDIENTE"
    col_nom   = "NOMBRE_CLIENTE"

    if col_saldo in df.columns and col_nom in df.columns:
        top = (
            df[df[col_saldo] > 0]
            .nlargest(15, col_saldo)
            [[col_nom, col_saldo]]
            .copy()
        )
        top[col_nom] = top[col_nom].str[:28]

        fig, ax = plt.subplots(figsize=(_FIG_W_IN, _FIG_H_SM))
        fig.patch.set_facecolor("white")
        y_pos = range(len(top))
        bars  = ax.barh(
            list(y_pos), top[col_saldo].tolist(),
            color=_AZUL_CORP, height=0.6, zorder=2,
        )
        ax.set_yticks(list(y_pos))
        ax.set_yticklabels(top[col_nom].tolist(), fontsize=7)
        ax.set_axisbelow(True)
        ax.xaxis.grid(True, linestyle="--", alpha=0.5, color="#DDDDDD")
        ax.xaxis.set_major_formatter(mticker.FuncFormatter(_fmt_miles))

        for bar in bars:
            val = bar.get_width()
            ax.text(
                val * 1.005,
                bar.get_y() + bar.get_height() / 2,
                _fmt_miles(val),
                va="center", ha="left", fontsize=7, color=_AZUL_CORP,
            )

        _ax_base(ax, f"Top 15 clientes por Saldo Pendiente — {moneda}")
        ax.spines["left"].set_visible(False)
        ax.tick_params(axis="y", length=0)
        fig.tight_layout(pad=2.0)
        story.append(_fig_a_imagen(fig, _FIG_H_SM))
        story.append(Spacer(1, 0.3 * cm))

    # Tabla: mostrar columnas resumidas para que quepan en la pagina
    cols_tabla = [c for c in [
        "NOMBRE_CLIENTE", "ESTATUS_CLIENTE", "NUM_DOCUMENTOS",
        "FACTURAS_PAGADAS", "FACTURAS_VIGENTES", "TOTAL_CARGOS",
        "TOTAL_ABONOS", "SALDO_PENDIENTE",
    ] if c in df.columns]
    story.append(_df_a_tabla(df[cols_tabla], max_filas=20))
    story.append(Paragraph(
        "Nota: la tabla muestra las primeras 20 filas. "
        "El detalle completo por rango de mora se encuentra en la hoja "
        f"antiguedad_por_cliente_{moneda.lower()} del Excel 02_analisis.",
        _STYLE_NOTA,
    ))
    story.append(PageBreak())


def _seccion_resumen_vendedor(
    df: pd.DataFrame,
    story: list,
) -> None:
    """Pagina: Resumen por Vendedor."""
    story.append(Paragraph("Resumen por Vendedor", _STYLE_TITULO))
    story.append(Paragraph(
        "Actividad de cobranza por ejecutivo de ventas y moneda · "
        "Fuente: movimientos_totales_cxc",
        _STYLE_SUBTITULO,
    ))
    story.append(Paragraph(
        "Este resumen mide la contribucion de cada vendedor en terminos de volumen "
        "facturado (TOTAL_CARGOS), cobros aplicados (TOTAL_ABONOS) y saldo pendiente "
        "de recuperar. Un saldo alto puede indicar una gestion activa con clientes de "
        "volumen elevado, pero tambien puede reflejar problemas de cobranza en la "
        "cartera del ejecutivo. Los registros <b>SIN VENDEDOR ASIGNADO</b> deben "
        "atenderse en el sistema para garantizar la trazabilidad de responsabilidades.",
        _STYLE_EXPLICACION,
    ))

    if df.empty:
        story.append(Paragraph("Sin datos disponibles.", _STYLE_NOTA))
        story.append(PageBreak())
        return

    monedas = sorted(df["MONEDA"].unique()) if "MONEDA" in df.columns else [""]
    n_mon   = len(monedas)
    fig, axes = plt.subplots(1, n_mon, figsize=(_FIG_W_IN, _FIG_H_IN),
                             squeeze=False)
    fig.patch.set_facecolor("white")

    for idx, moneda in enumerate(monedas):
        ax  = axes[0][idx]
        sub = (
            df[df["MONEDA"] == moneda].copy()
            if "MONEDA" in df.columns else df.copy()
        )
        sub = sub.sort_values("TOTAL_CARGOS", ascending=True).tail(15)
        nombres = sub["VENDEDOR"].str[:22].tolist()
        cargos  = sub["TOTAL_CARGOS"].tolist()
        abonos  = sub["TOTAL_ABONOS"].tolist()

        y   = np.arange(len(nombres))
        h   = 0.35
        ax.barh(y + h / 2, cargos, height=h, color=_AZUL_CORP, label="Cargos", zorder=2)
        ax.barh(y - h / 2, abonos, height=h, color=_VERDE_CORP, label="Abonos", zorder=2)
        ax.set_yticks(y)
        ax.set_yticklabels(nombres, fontsize=6)
        ax.set_axisbelow(True)
        ax.xaxis.grid(True, linestyle="--", alpha=0.5, color="#DDDDDD")
        ax.xaxis.set_major_formatter(mticker.FuncFormatter(_fmt_miles))
        ax.legend(fontsize=7, framealpha=0.5)
        _ax_base(ax, f"Cargos vs Abonos — {moneda}")
        ax.spines["left"].set_visible(False)
        ax.tick_params(axis="y", length=0)

    fig.tight_layout(pad=2.0)
    story.append(_fig_a_imagen(fig, _FIG_H_IN))
    story.append(Spacer(1, 0.3 * cm))
    story.append(_df_a_tabla(df, max_filas=20))
    story.append(PageBreak())


def _seccion_resumen_concepto(
    df: pd.DataFrame,
    story: list,
) -> None:
    """Pagina: Resumen por Concepto."""
    story.append(Paragraph("Resumen por Concepto", _STYLE_TITULO))
    story.append(Paragraph(
        "Volumen de operacion por tipo de concepto y moneda · "
        "Fuente: movimientos_totales_cxc (excluye ajustes y cancelados)",
        _STYLE_SUBTITULO,
    ))
    story.append(Paragraph(
        "Agrupa los cargos y abonos segun el concepto de facturacion registrado en "
        "Microsip. Permite identificar cuales lineas de producto o servicio generan "
        "mayor volumen de facturacion y cuales tienen mayor proporcion de abonos "
        "pendientes. Un concepto con muchos cargos pero pocos abonos puede indicar "
        "problemas de cobro especificos en esa linea de negocio.",
        _STYLE_EXPLICACION,
    ))

    if df.empty:
        story.append(Paragraph("Sin datos disponibles.", _STYLE_NOTA))
        story.append(PageBreak())
        return

    monedas = sorted(df["MONEDA"].unique()) if "MONEDA" in df.columns else [""]
    n_mon   = len(monedas)
    fig, axes = plt.subplots(1, n_mon, figsize=(_FIG_W_IN, _FIG_H_SM),
                             squeeze=False)
    fig.patch.set_facecolor("white")

    for idx, moneda in enumerate(monedas):
        ax  = axes[0][idx]
        sub = (
            df[df["MONEDA"] == moneda].copy()
            if "MONEDA" in df.columns else df.copy()
        )
        sub = sub.sort_values("TOTAL_CARGOS", ascending=True).tail(12)
        conceptos = sub["CONCEPTO"].str[:28].tolist()
        cargos    = sub["TOTAL_CARGOS"].tolist()

        y    = range(len(conceptos))
        cols = [_PALETA_BARRAS[i % len(_PALETA_BARRAS)] for i in range(len(conceptos))]
        ax.barh(list(y), cargos, color=cols, height=0.6, zorder=2)
        ax.set_yticks(list(y))
        ax.set_yticklabels(conceptos, fontsize=7)
        ax.set_axisbelow(True)
        ax.xaxis.grid(True, linestyle="--", alpha=0.5, color="#DDDDDD")
        ax.xaxis.set_major_formatter(mticker.FuncFormatter(_fmt_miles))
        _ax_base(ax, f"Total Cargos por Concepto — {moneda}")
        ax.spines["left"].set_visible(False)
        ax.tick_params(axis="y", length=0)

    fig.tight_layout(pad=2.0)
    story.append(_fig_a_imagen(fig, _FIG_H_SM))
    story.append(Spacer(1, 0.3 * cm))
    story.append(_df_a_tabla(df, max_filas=20))
    story.append(PageBreak())


def _seccion_registros_especiales(
    df: pd.DataFrame,
    titulo: str,
    descripcion: str,
    story: list,
) -> None:
    """Pagina generica para ajustes y cancelados."""
    story.append(Paragraph(titulo, _STYLE_TITULO))
    story.append(Paragraph(
        f"Detalle de registros especiales · {descripcion}",
        _STYLE_SUBTITULO,
    ))
    story.append(Paragraph(descripcion, _STYLE_EXPLICACION))

    if df.empty:
        story.append(Paragraph("Sin registros en este periodo.", _STYLE_NOTA))
        story.append(PageBreak())
        return

    monedas = sorted(df["MONEDA"].unique()) if "MONEDA" in df.columns else [""]
    n_mon   = len(monedas)
    fig, axes = plt.subplots(1, n_mon, figsize=(_FIG_W_IN, _FIG_H_SM),
                             squeeze=False)
    fig.patch.set_facecolor("white")

    for idx, moneda in enumerate(monedas):
        ax  = axes[0][idx]
        sub = (
            df[df["MONEDA"] == moneda].copy()
            if "MONEDA" in df.columns else df.copy()
        )
        sub = sub.sort_values("MONTO_TOTAL", ascending=True).tail(12)
        conceptos = sub["CONCEPTO"].str[:28].tolist()
        montos    = sub["MONTO_TOTAL"].tolist()

        y    = range(len(conceptos))
        cols = [_ROJO_CORP if "CANCELADO" in titulo.upper() else _NARANJA
                for _ in conceptos]
        ax.barh(list(y), montos, color=cols, height=0.6, zorder=2)
        ax.set_yticks(list(y))
        ax.set_yticklabels(conceptos, fontsize=7)
        ax.set_axisbelow(True)
        ax.xaxis.grid(True, linestyle="--", alpha=0.5, color="#DDDDDD")
        ax.xaxis.set_major_formatter(mticker.FuncFormatter(_fmt_miles))
        _ax_base(ax, f"Monto Total — {moneda}")
        ax.spines["left"].set_visible(False)
        ax.tick_params(axis="y", length=0)

    fig.tight_layout(pad=2.0)
    story.append(_fig_a_imagen(fig, _FIG_H_SM))
    story.append(Spacer(1, 0.3 * cm))
    story.append(_df_a_tabla(df))
    story.append(PageBreak())


# ======================================================================
# PORTADA
# ======================================================================


def _portada(story: list, timestamp: str) -> None:
    """Genera la pagina de portada del reporte."""
    story.append(Spacer(1, 3 * cm))
    story.append(Paragraph(
        "Reporte de Analisis de Cartera CxC",
        ParagraphStyle(
            "Portada",
            parent=_BASE_STYLES["Title"],
            fontSize=22,
            textColor=colors.HexColor(_AZUL_CORP),
            alignment=TA_CENTER,
            spaceAfter=12,
        ),
    ))
    story.append(Paragraph(
        "Analisis operativo y ejecutivo de Cuentas por Cobrar",
        ParagraphStyle(
            "SubPortada",
            parent=_BASE_STYLES["Normal"],
            fontSize=12,
            textColor=colors.HexColor("#666666"),
            alignment=TA_CENTER,
            spaceAfter=6,
        ),
    ))
    story.append(Spacer(1, 0.5 * cm))
    story.append(Paragraph(
        f"Generado: {timestamp}",
        ParagraphStyle(
            "FechaPortada",
            parent=_BASE_STYLES["Normal"],
            fontSize=9,
            textColor=colors.HexColor("#999999"),
            alignment=TA_CENTER,
        ),
    ))
    story.append(Spacer(1, 1.5 * cm))
    story.append(Paragraph(
        "Contenido: Cartera Vencida vs Vigente · Antiguedad de Cartera · "
        "Antiguedad por Cliente (MXN/USD) · Resumen por Vendedor · "
        "Resumen por Concepto · Ajustes · Cancelados",
        ParagraphStyle(
            "ContenidoPortada",
            parent=_BASE_STYLES["Normal"],
            fontSize=9,
            textColor=colors.HexColor("#555555"),
            alignment=TA_CENTER,
            leading=14,
        ),
    ))
    story.append(PageBreak())


# ======================================================================
# FUNCION PRINCIPAL
# ======================================================================


def generar_reporte_pdf(
    analisis: dict[str, pd.DataFrame],
    output_path: Path,
    timestamp: str,
) -> Path:
    """Genera el PDF ejecutivo de analisis de cartera CxC.

    Cada seccion del PDF corresponde a una hoja del Excel 02_analisis.
    Las secciones aparecen en el mismo orden que las hojas del Excel:
    cartera_vencida_vs_vigente, antiguedad_cartera,
    antiguedad_por_cliente_mxn/usd, resumen_por_vendedor,
    resumen_por_concepto, resumen_ajustes, resumen_cancelados.

    Args:
        analisis: Dict con los DataFrames de analisis, misma estructura
            que devuelve ``Analytics.run_analytics``.
        output_path: Path completo del archivo PDF a generar.
        timestamp: Cadena de fecha/hora para la portada (YYYY-MM-DD HH:MM).

    Returns:
        Path al archivo PDF generado.
    """
    story: list = []

    doc = SimpleDocTemplate(
        str(output_path),
        pagesize=landscape(A4),
        leftMargin=_MARGIN,
        rightMargin=_MARGIN,
        topMargin=_MARGIN + 0.5 * cm,
        bottomMargin=_MARGIN,
        title="Reporte de Analisis CxC",
        author="Pipeline CxC Microsip",
    )

    _portada(story, timestamp)

    _seccion_cartera_vencida(
        analisis.get("cartera_vencida_vs_vigente", pd.DataFrame()), story,
    )
    _seccion_antiguedad_cartera(
        analisis.get("antiguedad_cartera", pd.DataFrame()), story,
    )
    _seccion_antiguedad_cliente(
        analisis.get("antiguedad_por_cliente_mxn", pd.DataFrame()), "MXN", story,
    )
    _seccion_antiguedad_cliente(
        analisis.get("antiguedad_por_cliente_usd", pd.DataFrame()), "USD", story,
    )
    _seccion_resumen_vendedor(
        analisis.get("resumen_por_vendedor", pd.DataFrame()), story,
    )
    _seccion_resumen_concepto(
        analisis.get("resumen_por_concepto", pd.DataFrame()), story,
    )
    _seccion_registros_especiales(
        analisis.get("resumen_ajustes", pd.DataFrame()),
        "Registros por Acreditar (Ajustes)",
        "Los ajustes son anticipos o pagos recibidos que aun no han sido aplicados a "
        "una factura especifica en Microsip. Su presencia puede indicar un retraso en "
        "la conciliacion contable. Deben revisarse periodicamente para asegurar que se "
        "apliquen correctamente y no distorsionen el saldo real del cliente.",
        story,
    )
    _seccion_registros_especiales(
        analisis.get("resumen_cancelados", pd.DataFrame()),
        "Documentos Cancelados",
        "Los documentos cancelados representan facturas o movimientos anulados en el "
        "sistema. Un volumen alto puede indicar errores de captura frecuentes, ajustes "
        "de precio post-emision o disputas con clientes. Es recomendable analizar la "
        "tendencia mensual para detectar patrones inusuales.",
        story,
    )

    doc.build(story)
    logger.info("PDF de analisis generado: %s", output_path)
    return output_path