#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
REPORTE DE CUENTAS POR COBRAR - Generador PDF
================================================
Ejecuta este script para generar el reporte PDF completo.
Todas las secciones se agrupan por página con saltos de página entre ellas.
Orientación horizontal (landscape) para mejor visualización.

Dependencias:
    pip install reportlab matplotlib pandas

Uso:
    python generar_reporte_cxc.py
"""

import os
import io
import tempfile
from datetime import datetime

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

from reportlab.lib.pagesizes import landscape, letter
from reportlab.lib import colors
from reportlab.lib.units import inch, cm
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER, TA_RIGHT, TA_LEFT
from reportlab.platypus import (
    SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer,
    PageBreak, Image as RLImage, KeepTogether
)
from reportlab.pdfgen import canvas


# =============================================================================
# CONFIGURACIÓN GLOBAL
# =============================================================================
ARCHIVO_SALIDA = "Reporte_CXC.pdf"

# Colores corporativos
COLOR_HEADER = colors.HexColor("#003366")
COLOR_HEADER_TEXT = colors.white
COLOR_ROW_EVEN = colors.HexColor("#F2F6FA")
COLOR_ROW_ODD = colors.white
COLOR_BORDER = colors.HexColor("#CCCCCC")
COLOR_ACCENT = colors.HexColor("#0055A4")

# Colores para gráficos (paleta consistente)
CHART_COLORS = ['#003366', '#0055A4', '#4A90D9', '#7FB3E0', '#B0D4F1', '#D6E8F7']
CHART_COLORS_2 = ['#003366', '#C0392B']  # Vigentes vs Vencidas

# Tamaño de página
PAGE_W, PAGE_H = landscape(letter)
MARGIN = 0.6 * inch


# =============================================================================
# DATOS DEL REPORTE (reemplaza con tu fuente de datos real)
# =============================================================================

# --- Sección 1: Resumen por estatus de vencimiento ---
data_resumen_mxn = [
    {"estatus": "FACTURAS_VIGENTES", "num_docs": 212, "importe_total": 4439810.63, "saldo_pendiente": 2941145.94, "pct": 23.52},
    {"estatus": "FACTURAS_VENCIDAS", "num_docs": 323, "importe_total": 11062680.43, "saldo_pendiente": 9564015.74, "pct": 76.48},
]
data_resumen_usd = [
    {"estatus": "FACTURAS_VIGENTES", "num_docs": 3, "importe_total": 758975.88, "saldo_pendiente": 758975.88, "pct": 90.23},
    {"estatus": "FACTURAS_VENCIDAS", "num_docs": 2, "importe_total": 82150.88, "saldo_pendiente": 82150.88, "pct": 9.77},
]

# --- Sección 2: Antigüedad de saldos ---
data_antiguedad_mxn = [
    {"rango": "FACTURAS_VIGENTES", "num_docs": 212, "importe_total": 2941145.94, "pct": 23.52},
    {"rango": "FACTURAS_VENCIDAS (0-30)", "num_docs": 100, "importe_total": 7390552.05, "pct": 59.10},
    {"rango": "FACTURAS_VENCIDAS (31-60)", "num_docs": 27, "importe_total": 101425.70, "pct": 0.81},
    {"rango": "FACTURAS_VENCIDAS (61-90)", "num_docs": 18, "importe_total": 13468.98, "pct": 0.11},
    {"rango": "FACTURAS_VENCIDAS (91-120)", "num_docs": 19, "importe_total": 14225.00, "pct": 0.11},
    {"rango": "FACTURAS_VENCIDAS (+120)", "num_docs": 159, "importe_total": 2044344.01, "pct": 16.35},
]
data_antiguedad_usd = [
    {"rango": "FACTURAS_VIGENTES", "num_docs": 3, "importe_total": 758975.88, "pct": 90.23},
    {"rango": "FACTURAS_VENCIDAS (0-30)", "num_docs": 2, "importe_total": 82150.88, "pct": 9.77},
]

# --- Sección 3: Detalle por cliente MXN (top clientes) ---
data_clientes_mxn = [
    {"cliente": "CFN PRODUCTOS CARNICOS DEL NORTE", "status": "A", "docs": 122, "facturas_pagadas": 88551709.39, "vigentes": 0.00, "vencidas_0_30": 3007353.20, "vencidas_31_60": 0.00, "vencidas_61_90": 0.00, "vencidas_91_120": 0.00, "vencidas_120": 0.00, "total_cargo": 91553062.59, "abono": 88551709.39, "saldo": 3007353.20},
    {"cliente": "SALAZAR CANTU ALIMENTOS, S.A DE C.V", "status": "A", "docs": 40, "facturas_pagadas": 30396417.30, "vigentes": 636452.00, "vencidas_0_30": 2008019.00, "vencidas_31_60": 0.00, "vencidas_61_90": 0.00, "vencidas_91_120": 0.00, "vencidas_120": 0.00, "total_cargo": 33304868.30, "abono": 30396417.30, "saldo": 2906471.00},
    {"cliente": "JAIME ALBINO GUERRERO KONISHI", "status": "A", "docs": 16, "facturas_pagadas": 3142023.51, "vigentes": 0.00, "vencidas_0_30": 0.00, "vencidas_31_60": 0.00, "vencidas_61_90": 0.00, "vencidas_91_120": 0.00, "vencidas_120": 1786104.00, "total_cargo": 5161624.60, "abono": 3375720.26, "saldo": 1786104.34},
    {"cliente": "ALIMENTOS PROCESADOS SAN AGUSTIN SA DE CV", "status": "A", "docs": 5, "facturas_pagadas": 3677058.32, "vigentes": 0.00, "vencidas_0_30": 990144.00, "vencidas_31_60": 0.00, "vencidas_61_90": 0.00, "vencidas_91_120": 0.00, "vencidas_120": 0.00, "total_cargo": 4667202.32, "abono": 3677058.32, "saldo": 990144.00},
    {"cliente": "GM DEL NORTE", "status": "A", "docs": 1, "facturas_pagadas": 0.00, "vigentes": 755880.00, "vencidas_0_30": 0.00, "vencidas_31_60": 0.00, "vencidas_61_90": 0.00, "vencidas_91_120": 0.00, "vencidas_120": 0.00, "total_cargo": 755880.00, "abono": 0.00, "saldo": 755880.00},
    {"cliente": "MARIA DEL SOCORRO MEDINA AREVALO.", "status": "A", "docs": 5, "facturas_pagadas": 3141902.44, "vigentes": 719345.56, "vencidas_0_30": 295719.00, "vencidas_31_60": 0.00, "vencidas_61_90": 0.00, "vencidas_91_120": 0.00, "vencidas_120": 0.00, "total_cargo": 3861248.00, "abono": 3141902.44, "saldo": 719345.56},
    {"cliente": "AL PUBLICO GENERAL RAYON", "status": "A", "docs": 17, "facturas_pagadas": 0.00, "vigentes": 95558.00, "vencidas_0_30": 301145.61, "vencidas_31_60": 0.00, "vencidas_61_90": 0.00, "vencidas_91_120": 400.00, "vencidas_120": 0.00, "total_cargo": 3568860.00, "abono": 3256346.01, "saldo": 312194.59},
    {"cliente": "RICARDO RAMIREZ ROJAS", "status": "A", "docs": 336, "facturas_pagadas": 7422609.61, "vigentes": 44863.50, "vencidas_0_30": 101661.50, "vencidas_31_60": 1490.00, "vencidas_61_90": 0.00, "vencidas_91_120": 0.00, "vencidas_120": 10093.50, "total_cargo": 7619001.11, "abono": 7460671.61, "saldo": 158129.50},
    {"cliente": "SUCURSAL MADERO", "status": "A", "docs": 62, "facturas_pagadas": 5271870.25, "vigentes": 153720.15, "vencidas_0_30": 0.00, "vencidas_31_60": 0.00, "vencidas_61_90": 0.00, "vencidas_91_120": 0.00, "vencidas_120": 0.00, "total_cargo": 1796030.12, "abono": 1680433.98, "saldo": 155597.14},
    {"cliente": "AYALA AMAYA", "status": "A", "docs": 38, "facturas_pagadas": 1680432.98, "vigentes": 38740.25, "vencidas_0_30": 76856.89, "vencidas_31_60": 0.00, "vencidas_61_90": 0.00, "vencidas_91_120": 997.00, "vencidas_120": 0.00, "total_cargo": 12046224.62, "abono": 11900414.48, "saldo": 13736.00},
    {"cliente": "GO GOURMET ALIMENTOS", "status": "A", "docs": 151, "facturas_pagadas": 11894580.62, "vigentes": 44048.00, "vencidas_0_30": 68470.00, "vencidas_31_60": 0.00, "vencidas_61_90": 24466.00, "vencidas_91_120": 0.00, "vencidas_120": 4288.00, "total_cargo": 5416700.41, "abono": 6333623.91, "saldo": 83070.00},
    {"cliente": "MARIA CRUZ HERNANDEZ SALAS", "status": "A", "docs": 270, "facturas_pagadas": 5963014.91, "vigentes": 11204.00, "vencidas_0_30": 43110.00, "vencidas_31_60": 0.00, "vencidas_61_90": 0.00, "vencidas_91_120": 0.00, "vencidas_120": 0.00, "total_cargo": 1689781.47, "abono": 1609196.17, "saldo": 80585.30},
]

# --- Sección 3b: Detalle por cliente USD ---
data_clientes_usd = [
    {"cliente": "MARIA DEL SOCORRO MEDINA AREVALO", "status": "A", "docs": 1, "total_cargo": 719345.56, "abono": 0.00, "saldo": 719345.56},
    {"cliente": "DISTRIBUIDORA DE CARNE LA ORIENTAL", "status": "A", "docs": 14, "total_cargo": 611914.41, "abono": 529763.53, "saldo": 82150.88},
    {"cliente": "AQUA TERRA IMPORTS", "status": "A", "docs": 5, "total_cargo": 166457.30, "abono": 126826.98, "saldo": 39630.32},
]

# --- Sección 4: Resumen por vendedor ---
data_vendedor_mxn = [
    {"vendedor": "PRAC ALIMENTOS", "num_cargos": 7202, "num_abonos": 8386, "total_cargos": 115398941.81, "total_abonos": 110209832.24, "saldo": 5189109.57},
    {"vendedor": "SIN VENDEDOR ASIGNADO", "num_cargos": 1058, "num_abonos": 1338, "total_cargos": 99743670.71, "total_abonos": 96733887.25, "saldo": 3009783.46},
    {"vendedor": "HAIDEE HERNANDEZ", "num_cargos": 1709, "num_abonos": 2028, "total_cargos": 28080685.80, "total_abonos": 25960459.15, "saldo": 2120226.65},
    {"vendedor": "ADRIAN FERNANDO ARRIATA JARAMILLO", "num_cargos": 1038, "num_abonos": 1166, "total_cargos": 86767296.07, "total_abonos": 85485321.84, "saldo": 1281974.23},
    {"vendedor": "JUAN CARLOS BRIONES ZÚÑIGA", "num_cargos": 3440, "num_abonos": 4360, "total_cargos": 61243213.87, "total_abonos": 60381358.09, "saldo": 861855.78},
    {"vendedor": "ARTURO GAUCIN HERNÁNDEZ", "num_cargos": 243, "num_abonos": 319, "total_cargos": 107017307.61, "total_abonos": 106975095.62, "saldo": 42211.99},
]
data_vendedor_usd = [
    {"vendedor": "PRAC ALIMENTOS", "num_cargos": 3, "num_abonos": 2, "total_cargos": 762408.44, "total_abonos": 43062.88, "saldo": 719345.56},
    {"vendedor": "ADRIAN FERNANDO ARRIATA JARAMILLO", "num_cargos": 69, "num_abonos": 67, "total_cargos": 3176777.44, "total_abonos": 3094626.56, "saldo": 82150.88},
    {"vendedor": "HAIDEE HERNANDEZ", "num_cargos": 35, "num_abonos": 41, "total_cargos": 1556952.53, "total_abonos": 1517322.21, "saldo": 39630.32},
    {"vendedor": "ARTURO GAUCIN HERNÁNDEZ", "num_cargos": 6, "num_abonos": 7, "total_cargos": 293462.30, "total_abonos": 293462.30, "saldo": 0.00},
]

# --- Sección 5: Resumen por concepto ---
data_concepto_mxn = [
    {"concepto": "Venta", "num_cargos": 14681, "num_abonos": 0, "total_cargos": 497903154.48, "total_abonos": 0.00},
    {"concepto": "Anticipo", "num_cargos": 9, "num_abonos": 0, "total_cargos": 347961.39, "total_abonos": 0.00},
    {"concepto": "Cobro", "num_cargos": 0, "num_abonos": 15281, "total_cargos": 0.00, "total_abonos": 475446328.71},
    {"concepto": "Ajuste de saldo", "num_cargos": 0, "num_abonos": 1568, "total_cargos": 0.00, "total_abonos": 8004128.34},
    {"concepto": "Nota de Crédito", "num_cargos": 0, "num_abonos": 294, "total_cargos": 0.00, "total_abonos": 1529224.16},
    {"concepto": "Devolución", "num_cargos": 0, "num_abonos": 196, "total_cargos": 0.00, "total_abonos": 420762.23},
    {"concepto": "Nota de crédito", "num_cargos": 0, "num_abonos": 250, "total_cargos": 0.00, "total_abonos": 335627.30},
    {"concepto": "Aplicación de saldo por acreditar", "num_cargos": 0, "num_abonos": 1, "total_cargos": 0.00, "total_abonos": 5000.00},
    {"concepto": "Aplicación de anticipo", "num_cargos": 0, "num_abonos": 7, "total_cargos": 0.00, "total_abonos": 4883.45},
]
data_concepto_usd = [
    {"concepto": "Venta", "num_cargos": 112, "num_abonos": 0, "total_cargos": 5706000.71, "total_abonos": 0.00},
    {"concepto": "Anticipo Dlls", "num_cargos": 1, "num_abonos": 0, "total_cargos": 83600.00, "total_abonos": 0.00},
    {"concepto": "Cobro", "num_cargos": 0, "num_abonos": 104, "total_cargos": 0.00, "total_abonos": 4769003.19},
    {"concepto": "Ajuste de saldo", "num_cargos": 0, "num_abonos": 5, "total_cargos": 0.00, "total_abonos": 93131.80},
    {"concepto": "Aplicación de anticipo", "num_cargos": 0, "num_abonos": 2, "total_cargos": 0.00, "total_abonos": 83600.00},
    {"concepto": "Nota de Crédito", "num_cargos": 0, "num_abonos": 5, "total_cargos": 0.00, "total_abonos": 2441.67},
    {"concepto": "Nota de crédito", "num_cargos": 0, "num_abonos": 1, "total_cargos": 0.00, "total_abonos": 297.29},
]

# --- Sección 6: Ajustes ---
data_ajustes_mxn = [
    {"tipo": "AJUSTE", "concepto": "Cobro", "num_registros": 119, "importe_total": 578535.18, "impuesto_total": 0.00, "monto_total": 578535.18},
]
data_ajustes_usd = [
    {"tipo": "AJUSTE", "concepto": "Cobro", "num_registros": 1, "importe_total": 296.50, "impuesto_total": 0.00, "monto_total": 296.50},
]

# --- Sección 7: Cancelados ---
data_cancelados_mxn = [
    {"tipo": "CANCELADO", "concepto": "Cobro", "num_registros": 236, "importe_total": 5333130.11, "impuesto_total": 0.00, "monto_total": 5333130.11},
    {"tipo": "CANCELADO", "concepto": "Venta", "num_registros": 13, "importe_total": 433619.78, "impuesto_total": 0.00, "monto_total": 433619.78},
    {"tipo": "CANCELADO", "concepto": "Anticipo", "num_registros": 8, "importe_total": 119958.27, "impuesto_total": 0.00, "monto_total": 119958.27},
    {"tipo": "CANCELADO", "concepto": "Ajuste de saldo", "num_registros": 10, "importe_total": 91450.17, "impuesto_total": 0.00, "monto_total": 91450.17},
    {"tipo": "CANCELADO", "concepto": "Nota de crédito", "num_registros": 8, "importe_total": 3701.23, "impuesto_total": 0.00, "monto_total": 3701.23},
    {"tipo": "CANCELADO", "concepto": "Nota de Crédito", "num_registros": 4, "importe_total": 2553.64, "impuesto_total": 0.00, "monto_total": 2553.64},
    {"tipo": "CANCELADO", "concepto": "Aplicación de saldo por acreditar", "num_registros": 1, "importe_total": 240.00, "impuesto_total": 0.00, "monto_total": 240.00},
]
data_cancelados_usd = [
    {"tipo": "CANCELADO", "concepto": "Cobro", "num_registros": 6, "importe_total": 1057670.76, "impuesto_total": 0.00, "monto_total": 1057670.76},
    {"tipo": "CANCELADO", "concepto": "Venta", "num_registros": 1, "importe_total": 881633.97, "impuesto_total": 0.00, "monto_total": 881633.97},
    {"tipo": "CANCELADO", "concepto": "Anticipo", "num_registros": 4, "importe_total": 164496.16, "impuesto_total": 0.00, "monto_total": 164496.16},
    {"tipo": "CANCELADO", "concepto": "Anticipo Dlls", "num_registros": 1, "importe_total": 41124.04, "impuesto_total": 0.00, "monto_total": 41124.04},
    {"tipo": "CANCELADO", "concepto": "Nota de crédito", "num_registros": 1, "importe_total": 297.29, "impuesto_total": 0.00, "monto_total": 297.29},
]


# =============================================================================
# UTILIDADES DE FORMATO
# =============================================================================

def fmt_money(val):
    """Formatea número como moneda sin símbolo."""
    if val is None:
        return ""
    return f"{val:,.2f}"


def fmt_int(val):
    """Formatea número entero con comas."""
    if val is None:
        return ""
    return f"{val:,}"


def fmt_pct(val):
    """Formatea porcentaje."""
    if val is None:
        return ""
    return f"{val:.2f}%"


# =============================================================================
# GENERACIÓN DE GRÁFICOS (con aspecto correcto)
# =============================================================================

def crear_grafico_pastel(labels, sizes, titulo, colores=None, figsize=(5.5, 3.5)):
    """Crea un gráfico de pastel y lo devuelve como bytes PNG."""
    if colores is None:
        colores = CHART_COLORS[:len(labels)]

    fig, ax = plt.subplots(figsize=figsize, dpi=150)

    wedges, texts, autotexts = ax.pie(
        sizes, labels=None, autopct='%1.1f%%',
        colors=colores, startangle=90,
        pctdistance=0.75,
        wedgeprops={'linewidth': 1, 'edgecolor': 'white'}
    )

    for t in autotexts:
        t.set_fontsize(8)
        t.set_fontweight('bold')
        t.set_color('white')

    ax.legend(
        labels, loc='center left', bbox_to_anchor=(1.0, 0.5),
        fontsize=7, frameon=False
    )

    ax.set_title(titulo, fontsize=10, fontweight='bold', color='#003366', pad=10)
    fig.tight_layout()

    buf = io.BytesIO()
    fig.savefig(buf, format='png', bbox_inches='tight', dpi=150)
    plt.close(fig)
    buf.seek(0)
    return buf


def crear_grafico_barras_h(labels, values, titulo, color='#003366', figsize=(5.5, 3.5)):
    """Crea un gráfico de barras horizontales."""
    fig, ax = plt.subplots(figsize=figsize, dpi=150)

    y_pos = range(len(labels))
    bars = ax.barh(y_pos, values, color=color, edgecolor='white', height=0.6)

    ax.set_yticks(y_pos)
    ax.set_yticklabels(labels, fontsize=7)
    ax.invert_yaxis()

    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, p: f'${x:,.0f}'))
    ax.tick_params(axis='x', labelsize=7)

    for bar, val in zip(bars, values):
        ax.text(bar.get_width() + max(values) * 0.01, bar.get_y() + bar.get_height() / 2,
                f'${val:,.0f}', va='center', fontsize=6, color='#333333')

    ax.set_title(titulo, fontsize=10, fontweight='bold', color='#003366', pad=10)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    fig.tight_layout()

    buf = io.BytesIO()
    fig.savefig(buf, format='png', bbox_inches='tight', dpi=150)
    plt.close(fig)
    buf.seek(0)
    return buf


def crear_grafico_barras_agrupadas(labels, vals1, vals2, label1, label2, titulo, figsize=(6, 3.5)):
    """Crea gráfico de barras agrupadas verticales."""
    import numpy as np
    fig, ax = plt.subplots(figsize=figsize, dpi=150)

    x = np.arange(len(labels))
    width = 0.35

    bars1 = ax.bar(x - width / 2, vals1, width, label=label1, color='#003366', edgecolor='white')
    bars2 = ax.bar(x + width / 2, vals2, width, label=label2, color='#4A90D9', edgecolor='white')

    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=7, rotation=25, ha='right')
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, p: f'${x:,.0f}'))
    ax.tick_params(axis='y', labelsize=7)

    ax.legend(fontsize=7, frameon=False)
    ax.set_title(titulo, fontsize=10, fontweight='bold', color='#003366', pad=10)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    fig.tight_layout()

    buf = io.BytesIO()
    fig.savefig(buf, format='png', bbox_inches='tight', dpi=150)
    plt.close(fig)
    buf.seek(0)
    return buf


# =============================================================================
# CONSTRUCCIÓN DE TABLAS CON ESTILO
# =============================================================================

def estilo_tabla_base(num_rows, col_widths=None):
    """Devuelve un estilo de tabla estándar corporativo."""
    style = [
        # Header
        ('BACKGROUND', (0, 0), (-1, 0), COLOR_HEADER),
        ('TEXTCOLOR', (0, 0), (-1, 0), COLOR_HEADER_TEXT),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 7),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 6),
        ('TOPPADDING', (0, 0), (-1, 0), 6),
        ('ALIGN', (0, 0), (-1, 0), 'CENTER'),

        # Body
        ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
        ('FONTSIZE', (0, 1), (-1, -1), 7),
        ('BOTTOMPADDING', (0, 1), (-1, -1), 4),
        ('TOPPADDING', (0, 1), (-1, -1), 4),

        # Grid
        ('GRID', (0, 0), (-1, -1), 0.5, COLOR_BORDER),
        ('LINEBELOW', (0, 0), (-1, 0), 1.5, COLOR_HEADER),

        # Alignment default
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
    ]

    # Zebra striping
    for i in range(1, num_rows + 1):
        if i % 2 == 0:
            style.append(('BACKGROUND', (0, i), (-1, i), COLOR_ROW_EVEN))
        else:
            style.append(('BACKGROUND', (0, i), (-1, i), COLOR_ROW_ODD))

    return style


def crear_seccion_titulo(texto, styles):
    """Crea un título de sección."""
    return Paragraph(
        texto,
        ParagraphStyle(
            'SeccionTitulo',
            parent=styles['Heading1'],
            fontSize=13,
            textColor=COLOR_HEADER,
            spaceAfter=8,
            spaceBefore=4,
            fontName='Helvetica-Bold',
        )
    )


def crear_subtitulo(texto, styles):
    """Crea un subtítulo de moneda."""
    return Paragraph(
        texto,
        ParagraphStyle(
            'SubTitulo',
            parent=styles['Heading2'],
            fontSize=10,
            textColor=COLOR_ACCENT,
            spaceAfter=4,
            spaceBefore=8,
            fontName='Helvetica-Bold',
        )
    )


# =============================================================================
# HEADER Y FOOTER DE CADA PÁGINA
# =============================================================================

class HeaderFooter(canvas.Canvas):
    """Canvas personalizado con header y footer en cada página."""

    def __init__(self, *args, **kwargs):
        canvas.Canvas.__init__(self, *args, **kwargs)
        self._saved_page_states = []

    def showPage(self):
        self._saved_page_states.append(dict(self.__dict__))
        self._startPage()

    def save(self):
        num_pages = len(self._saved_page_states)
        for state in self._saved_page_states:
            self.__dict__.update(state)
            self.draw_page_header_footer(num_pages)
            canvas.Canvas.showPage(self)
        canvas.Canvas.save(self)

    def draw_page_header_footer(self, page_count):
        self.saveState()

        # Header
        self.setFillColor(COLOR_HEADER)
        self.rect(0, PAGE_H - 45, PAGE_W, 45, fill=1, stroke=0)

        self.setFillColor(colors.white)
        self.setFont('Helvetica-Bold', 14)
        self.drawString(MARGIN, PAGE_H - 30, "REPORTE DE CUENTAS POR COBRAR")

        self.setFont('Helvetica', 9)
        fecha_str = datetime.now().strftime("%d/%m/%Y %H:%M")
        self.drawRightString(PAGE_W - MARGIN, PAGE_H - 30, f"Fecha: {fecha_str}")

        # Línea decorativa bajo header
        self.setStrokeColor(COLOR_ACCENT)
        self.setLineWidth(2)
        self.line(0, PAGE_H - 46, PAGE_W, PAGE_H - 46)

        # Footer
        self.setFillColor(colors.HexColor("#666666"))
        self.setFont('Helvetica', 7)
        self.drawString(MARGIN, 20, "Reporte generado automáticamente — Confidencial")
        self.drawRightString(
            PAGE_W - MARGIN, 20,
            f"Página {self._pageNumber} de {page_count}"
        )

        # Línea sobre footer
        self.setStrokeColor(COLOR_BORDER)
        self.setLineWidth(0.5)
        self.line(MARGIN, 35, PAGE_W - MARGIN, 35)

        self.restoreState()


# =============================================================================
# FUNCIÓN PRINCIPAL: GENERAR REPORTE
# =============================================================================

def generar_reporte(archivo_salida=ARCHIVO_SALIDA):
    """Genera el reporte PDF completo."""

    doc = SimpleDocTemplate(
        archivo_salida,
        pagesize=landscape(letter),
        leftMargin=MARGIN,
        rightMargin=MARGIN,
        topMargin=MARGIN + 35,  # Espacio para header
        bottomMargin=MARGIN + 20,  # Espacio para footer
    )

    styles = getSampleStyleSheet()
    story = []

    usable_width = PAGE_W - 2 * MARGIN

    # Estilos de párrafo para celdas
    cell_style_right = ParagraphStyle('CellRight', parent=styles['Normal'], fontSize=7, alignment=TA_RIGHT, fontName='Helvetica')
    cell_style_left = ParagraphStyle('CellLeft', parent=styles['Normal'], fontSize=7, alignment=TA_LEFT, fontName='Helvetica')
    cell_style_center = ParagraphStyle('CellCenter', parent=styles['Normal'], fontSize=7, alignment=TA_CENTER, fontName='Helvetica')
    header_style = ParagraphStyle('HeaderCell', parent=styles['Normal'], fontSize=7, alignment=TA_CENTER, fontName='Helvetica-Bold', textColor=colors.white)

    # =====================================================================
    # PÁGINA 1: Resumen General (Vigentes vs Vencidas) + Gráficos
    # =====================================================================
    story.append(crear_seccion_titulo("1. RESUMEN POR ESTATUS DE VENCIMIENTO", styles))
    story.append(Spacer(1, 6))

    # --- Tabla MXN ---
    story.append(crear_subtitulo("Moneda: MXN", styles))
    headers_resumen = ["MONEDA", "ESTATUS_VENCIMIENTO", "NUM_DOCUMENTOS", "IMPORTE_TOTAL", "SALDO_PENDIENTE", "PCT_DEL_TOTAL"]
    tabla_data = [headers_resumen]
    for r in data_resumen_mxn:
        tabla_data.append(["MXN", r["estatus"], fmt_int(r["num_docs"]), fmt_money(r["importe_total"]), fmt_money(r["saldo_pendiente"]), fmt_pct(r["pct"])])
    # Totales
    total_docs = sum(r["num_docs"] for r in data_resumen_mxn)
    total_importe = sum(r["importe_total"] for r in data_resumen_mxn)
    total_saldo = sum(r["saldo_pendiente"] for r in data_resumen_mxn)
    tabla_data.append(["", "TOTAL", fmt_int(total_docs), fmt_money(total_importe), fmt_money(total_saldo), "100.00%"])

    col_w = [0.8*inch, 2.0*inch, 1.3*inch, 1.5*inch, 1.5*inch, 1.2*inch]
    t = Table(tabla_data, colWidths=col_w)
    s = estilo_tabla_base(len(tabla_data) - 1)
    s.extend([
        ('ALIGN', (2, 1), (-1, -1), 'RIGHT'),
        ('ALIGN', (0, 1), (1, -1), 'LEFT'),
        ('BACKGROUND', (0, -1), (-1, -1), colors.HexColor("#E8EEF4")),
        ('FONTNAME', (0, -1), (-1, -1), 'Helvetica-Bold'),
    ])
    t.setStyle(TableStyle(s))
    story.append(t)
    story.append(Spacer(1, 10))

    # --- Tabla USD ---
    story.append(crear_subtitulo("Moneda: USD", styles))
    tabla_data_usd = [headers_resumen]
    for r in data_resumen_usd:
        tabla_data_usd.append(["USD", r["estatus"], fmt_int(r["num_docs"]), fmt_money(r["importe_total"]), fmt_money(r["saldo_pendiente"]), fmt_pct(r["pct"])])
    total_docs_usd = sum(r["num_docs"] for r in data_resumen_usd)
    total_importe_usd = sum(r["importe_total"] for r in data_resumen_usd)
    total_saldo_usd = sum(r["saldo_pendiente"] for r in data_resumen_usd)
    tabla_data_usd.append(["", "TOTAL", fmt_int(total_docs_usd), fmt_money(total_importe_usd), fmt_money(total_saldo_usd), "100.00%"])

    t2 = Table(tabla_data_usd, colWidths=col_w)
    s2 = estilo_tabla_base(len(tabla_data_usd) - 1)
    s2.extend([
        ('ALIGN', (2, 1), (-1, -1), 'RIGHT'),
        ('ALIGN', (0, 1), (1, -1), 'LEFT'),
        ('BACKGROUND', (0, -1), (-1, -1), colors.HexColor("#E8EEF4")),
        ('FONTNAME', (0, -1), (-1, -1), 'Helvetica-Bold'),
    ])
    t2.setStyle(TableStyle(s2))
    story.append(t2)
    story.append(Spacer(1, 15))

    # --- Gráficos lado a lado ---
    labels_mxn = [r["estatus"].replace("_", " ") for r in data_resumen_mxn]
    sizes_mxn = [r["saldo_pendiente"] for r in data_resumen_mxn]
    chart1_buf = crear_grafico_pastel(labels_mxn, sizes_mxn, "Distribución Saldo MXN", CHART_COLORS_2, figsize=(4.5, 3.0))

    labels_usd = [r["estatus"].replace("_", " ") for r in data_resumen_usd]
    sizes_usd = [r["saldo_pendiente"] for r in data_resumen_usd]
    chart2_buf = crear_grafico_pastel(labels_usd, sizes_usd, "Distribución Saldo USD", CHART_COLORS_2, figsize=(4.5, 3.0))

    # Imagen proporcional (sin deformar)
    img1 = RLImage(chart1_buf, width=4.2*inch, height=2.8*inch, kind='proportional')
    img2 = RLImage(chart2_buf, width=4.2*inch, height=2.8*inch, kind='proportional')

    chart_table = Table([[img1, img2]], colWidths=[usable_width / 2, usable_width / 2])
    chart_table.setStyle(TableStyle([('ALIGN', (0, 0), (-1, -1), 'CENTER'), ('VALIGN', (0, 0), (-1, -1), 'MIDDLE')]))
    story.append(chart_table)

    story.append(PageBreak())

    # =====================================================================
    # PÁGINA 2: Antigüedad de Saldos + Gráficos
    # =====================================================================
    story.append(crear_seccion_titulo("2. ANTIGÜEDAD DE SALDOS", styles))
    story.append(Spacer(1, 6))

    # --- Tabla MXN ---
    story.append(crear_subtitulo("Moneda: MXN", styles))
    headers_antig = ["MONEDA", "RANGO_ANTIGUEDAD", "NUM_DOCUMENTOS", "IMPORTE_TOTAL", "PCT_DEL_TOTAL"]
    tabla_ant = [headers_antig]
    for r in data_antiguedad_mxn:
        tabla_ant.append(["MXN", r["rango"], fmt_int(r["num_docs"]), fmt_money(r["importe_total"]), fmt_pct(r["pct"])])

    col_ant = [0.8*inch, 2.5*inch, 1.3*inch, 1.5*inch, 1.2*inch]
    t_ant = Table(tabla_ant, colWidths=col_ant)
    s_ant = estilo_tabla_base(len(tabla_ant) - 1)
    s_ant.extend([
        ('ALIGN', (2, 1), (-1, -1), 'RIGHT'),
        ('ALIGN', (0, 1), (1, -1), 'LEFT'),
    ])
    t_ant.setStyle(TableStyle(s_ant))
    story.append(t_ant)
    story.append(Spacer(1, 10))

    # --- Tabla USD ---
    story.append(crear_subtitulo("Moneda: USD", styles))
    tabla_ant_usd = [headers_antig]
    for r in data_antiguedad_usd:
        tabla_ant_usd.append(["USD", r["rango"], fmt_int(r["num_docs"]), fmt_money(r["importe_total"]), fmt_pct(r["pct"])])

    t_ant_usd = Table(tabla_ant_usd, colWidths=col_ant)
    s_ant_usd = estilo_tabla_base(len(tabla_ant_usd) - 1)
    s_ant_usd.extend([
        ('ALIGN', (2, 1), (-1, -1), 'RIGHT'),
        ('ALIGN', (0, 1), (1, -1), 'LEFT'),
    ])
    t_ant_usd.setStyle(TableStyle(s_ant_usd))
    story.append(t_ant_usd)
    story.append(Spacer(1, 15))

    # --- Gráfico de antigüedad MXN ---
    labels_ag = [r["rango"].replace("FACTURAS_", "").replace("_", " ") for r in data_antiguedad_mxn]
    vals_ag = [r["importe_total"] for r in data_antiguedad_mxn]
    chart_ag = crear_grafico_barras_h(labels_ag, vals_ag, "Antigüedad de Saldos MXN", color='#003366', figsize=(7, 3.2))
    img_ag = RLImage(chart_ag, width=6.5*inch, height=3.0*inch, kind='proportional')

    chart_ag_table = Table([[img_ag]], colWidths=[usable_width])
    chart_ag_table.setStyle(TableStyle([('ALIGN', (0, 0), (-1, -1), 'CENTER')]))
    story.append(chart_ag_table)

    story.append(PageBreak())

    # =====================================================================
    # PÁGINA 3: Detalle por Cliente MXN
    # =====================================================================
    story.append(crear_seccion_titulo("3. DETALLE POR CLIENTE — MXN", styles))
    story.append(Spacer(1, 6))

    headers_cli = ["CLIENTE", "ST", "DOCS", "FACT. PAGADAS", "VIGENTES",
                   "VENC 0-30", "VENC 31-60", "VENC 61-90", "VENC 91-120",
                   "VENC +120", "TOTAL CARGO", "ABONO", "SALDO PEND."]

    tabla_cli = [headers_cli]
    for c in data_clientes_mxn:
        tabla_cli.append([
            Paragraph(c["cliente"], cell_style_left),
            c["status"],
            fmt_int(c["docs"]),
            fmt_money(c["facturas_pagadas"]),
            fmt_money(c["vigentes"]),
            fmt_money(c["vencidas_0_30"]),
            fmt_money(c["vencidas_31_60"]),
            fmt_money(c["vencidas_61_90"]),
            fmt_money(c["vencidas_91_120"]),
            fmt_money(c["vencidas_120"]),
            fmt_money(c["total_cargo"]),
            fmt_money(c["abono"]),
            fmt_money(c["saldo"]),
        ])

    col_cli = [1.9*inch, 0.3*inch, 0.4*inch, 0.9*inch, 0.7*inch,
               0.8*inch, 0.7*inch, 0.7*inch, 0.7*inch,
               0.7*inch, 0.9*inch, 0.9*inch, 0.9*inch]
    t_cli = Table(tabla_cli, colWidths=col_cli, repeatRows=1)
    s_cli = estilo_tabla_base(len(tabla_cli) - 1)
    s_cli.extend([
        ('ALIGN', (1, 1), (-1, -1), 'RIGHT'),
        ('ALIGN', (0, 1), (0, -1), 'LEFT'),
        ('FONTSIZE', (0, 0), (-1, -1), 6),
        ('FONTSIZE', (0, 0), (-1, 0), 6.5),
    ])
    t_cli.setStyle(TableStyle(s_cli))
    story.append(t_cli)
    story.append(Spacer(1, 12))

    # --- Gráfico top clientes MXN ---
    top_cli = sorted(data_clientes_mxn, key=lambda x: x["saldo"], reverse=True)[:8]
    labels_tc = [c["cliente"][:30] for c in top_cli]
    vals_tc = [c["saldo"] for c in top_cli]
    chart_tc = crear_grafico_barras_h(labels_tc, vals_tc, "Top Clientes por Saldo Pendiente (MXN)", color='#003366', figsize=(8, 3.5))
    img_tc = RLImage(chart_tc, width=7.5*inch, height=3.2*inch, kind='proportional')
    story.append(img_tc)

    story.append(PageBreak())

    # =====================================================================
    # PÁGINA 4: Detalle por Cliente USD
    # =====================================================================
    story.append(crear_seccion_titulo("4. DETALLE POR CLIENTE — USD", styles))
    story.append(Spacer(1, 6))

    headers_cli_usd = ["CLIENTE", "STATUS", "DOCS", "TOTAL_CARGO", "ABONO", "SALDO_PENDIENTE"]
    tabla_cli_usd = [headers_cli_usd]
    for c in data_clientes_usd:
        tabla_cli_usd.append([
            c["cliente"], c["status"], fmt_int(c["docs"]),
            fmt_money(c["total_cargo"]), fmt_money(c["abono"]), fmt_money(c["saldo"])
        ])

    col_cli_usd = [2.8*inch, 0.6*inch, 0.6*inch, 1.3*inch, 1.3*inch, 1.3*inch]
    t_cli_usd = Table(tabla_cli_usd, colWidths=col_cli_usd)
    s_cli_usd = estilo_tabla_base(len(tabla_cli_usd) - 1)
    s_cli_usd.extend([
        ('ALIGN', (2, 1), (-1, -1), 'RIGHT'),
        ('ALIGN', (0, 1), (1, -1), 'LEFT'),
    ])
    t_cli_usd.setStyle(TableStyle(s_cli_usd))
    story.append(t_cli_usd)

    story.append(PageBreak())

    # =====================================================================
    # PÁGINA 5: Resumen por Vendedor
    # =====================================================================
    story.append(crear_seccion_titulo("5. RESUMEN POR VENDEDOR", styles))
    story.append(Spacer(1, 6))

    # --- Tabla MXN ---
    story.append(crear_subtitulo("Moneda: MXN", styles))
    headers_vend = ["MONEDA", "VENDEDOR", "NUM_CARGOS", "NUM_ABONOS", "TOTAL_CARGOS", "TOTAL_ABONOS", "SALDO_PENDIENTE"]
    tabla_vend = [headers_vend]
    for v in data_vendedor_mxn:
        tabla_vend.append(["MXN", v["vendedor"], fmt_int(v["num_cargos"]), fmt_int(v["num_abonos"]),
                           fmt_money(v["total_cargos"]), fmt_money(v["total_abonos"]), fmt_money(v["saldo"])])

    col_vend = [0.6*inch, 2.5*inch, 0.9*inch, 0.9*inch, 1.4*inch, 1.4*inch, 1.3*inch]
    t_vend = Table(tabla_vend, colWidths=col_vend)
    s_vend = estilo_tabla_base(len(tabla_vend) - 1)
    s_vend.extend([
        ('ALIGN', (2, 1), (-1, -1), 'RIGHT'),
        ('ALIGN', (0, 1), (1, -1), 'LEFT'),
    ])
    t_vend.setStyle(TableStyle(s_vend))
    story.append(t_vend)
    story.append(Spacer(1, 10))

    # --- Tabla USD ---
    story.append(crear_subtitulo("Moneda: USD", styles))
    tabla_vend_usd = [headers_vend]
    for v in data_vendedor_usd:
        tabla_vend_usd.append(["USD", v["vendedor"], fmt_int(v["num_cargos"]), fmt_int(v["num_abonos"]),
                               fmt_money(v["total_cargos"]), fmt_money(v["total_abonos"]), fmt_money(v["saldo"])])

    t_vend_usd = Table(tabla_vend_usd, colWidths=col_vend)
    s_vend_usd = estilo_tabla_base(len(tabla_vend_usd) - 1)
    s_vend_usd.extend([
        ('ALIGN', (2, 1), (-1, -1), 'RIGHT'),
        ('ALIGN', (0, 1), (1, -1), 'LEFT'),
    ])
    t_vend_usd.setStyle(TableStyle(s_vend_usd))
    story.append(t_vend_usd)
    story.append(Spacer(1, 15))

    # --- Gráfico vendedor MXN ---
    labels_v = [v["vendedor"][:25] for v in data_vendedor_mxn]
    cargos_v = [v["total_cargos"] for v in data_vendedor_mxn]
    abonos_v = [v["total_abonos"] for v in data_vendedor_mxn]
    chart_v = crear_grafico_barras_agrupadas(
        labels_v, cargos_v, abonos_v,
        "Total Cargos", "Total Abonos",
        "Cargos vs Abonos por Vendedor (MXN)",
        figsize=(8, 3.5)
    )
    img_v = RLImage(chart_v, width=7.5*inch, height=3.2*inch, kind='proportional')
    story.append(img_v)

    story.append(PageBreak())

    # =====================================================================
    # PÁGINA 6: Resumen por Concepto
    # =====================================================================
    story.append(crear_seccion_titulo("6. RESUMEN POR CONCEPTO", styles))
    story.append(Spacer(1, 6))

    # --- Tabla MXN ---
    story.append(crear_subtitulo("Moneda: MXN", styles))
    headers_conc = ["MONEDA", "CONCEPTO", "NUM_CARGOS", "NUM_ABONOS", "TOTAL_CARGOS", "TOTAL_ABONOS"]
    tabla_conc = [headers_conc]
    for c in data_concepto_mxn:
        tabla_conc.append(["MXN", c["concepto"], fmt_int(c["num_cargos"]), fmt_int(c["num_abonos"]),
                           fmt_money(c["total_cargos"]), fmt_money(c["total_abonos"])])

    col_conc = [0.6*inch, 2.5*inch, 1.0*inch, 1.0*inch, 1.5*inch, 1.5*inch]
    t_conc = Table(tabla_conc, colWidths=col_conc)
    s_conc = estilo_tabla_base(len(tabla_conc) - 1)
    s_conc.extend([
        ('ALIGN', (2, 1), (-1, -1), 'RIGHT'),
        ('ALIGN', (0, 1), (1, -1), 'LEFT'),
    ])
    t_conc.setStyle(TableStyle(s_conc))
    story.append(t_conc)
    story.append(Spacer(1, 10))

    # --- Tabla USD ---
    story.append(crear_subtitulo("Moneda: USD", styles))
    tabla_conc_usd = [headers_conc]
    for c in data_concepto_usd:
        tabla_conc_usd.append(["USD", c["concepto"], fmt_int(c["num_cargos"]), fmt_int(c["num_abonos"]),
                               fmt_money(c["total_cargos"]), fmt_money(c["total_abonos"])])

    t_conc_usd = Table(tabla_conc_usd, colWidths=col_conc)
    s_conc_usd = estilo_tabla_base(len(tabla_conc_usd) - 1)
    s_conc_usd.extend([
        ('ALIGN', (2, 1), (-1, -1), 'RIGHT'),
        ('ALIGN', (0, 1), (1, -1), 'LEFT'),
    ])
    t_conc_usd.setStyle(TableStyle(s_conc_usd))
    story.append(t_conc_usd)

    story.append(PageBreak())

    # =====================================================================
    # PÁGINA 7: Ajustes y Cancelados
    # =====================================================================
    story.append(crear_seccion_titulo("7. REGISTROS DE AJUSTE", styles))
    story.append(Spacer(1, 6))

    headers_aj = ["MONEDA", "TIPO_REGISTRO", "CONCEPTO", "NUM_REGISTROS", "IMPORTE_TOTAL", "IMPUESTO_TOTAL", "MONTO_TOTAL"]

    # --- Ajustes MXN ---
    story.append(crear_subtitulo("Moneda: MXN", styles))
    tabla_aj = [headers_aj]
    for a in data_ajustes_mxn:
        tabla_aj.append(["MXN", a["tipo"], a["concepto"], fmt_int(a["num_registros"]),
                         fmt_money(a["importe_total"]), fmt_money(a["impuesto_total"]), fmt_money(a["monto_total"])])

    col_aj = [0.6*inch, 1.2*inch, 1.5*inch, 1.0*inch, 1.3*inch, 1.2*inch, 1.3*inch]
    t_aj = Table(tabla_aj, colWidths=col_aj)
    s_aj = estilo_tabla_base(len(tabla_aj) - 1)
    s_aj.extend([
        ('ALIGN', (3, 1), (-1, -1), 'RIGHT'),
        ('ALIGN', (0, 1), (2, -1), 'LEFT'),
    ])
    t_aj.setStyle(TableStyle(s_aj))
    story.append(t_aj)
    story.append(Spacer(1, 8))

    # --- Ajustes USD ---
    story.append(crear_subtitulo("Moneda: USD", styles))
    tabla_aj_usd = [headers_aj]
    for a in data_ajustes_usd:
        tabla_aj_usd.append(["USD", a["tipo"], a["concepto"], fmt_int(a["num_registros"]),
                             fmt_money(a["importe_total"]), fmt_money(a["impuesto_total"]), fmt_money(a["monto_total"])])

    t_aj_usd = Table(tabla_aj_usd, colWidths=col_aj)
    s_aj_usd = estilo_tabla_base(len(tabla_aj_usd) - 1)
    s_aj_usd.extend([
        ('ALIGN', (3, 1), (-1, -1), 'RIGHT'),
        ('ALIGN', (0, 1), (2, -1), 'LEFT'),
    ])
    t_aj_usd.setStyle(TableStyle(s_aj_usd))
    story.append(t_aj_usd)

    story.append(Spacer(1, 25))

    # =====================================================================
    # Continúa en misma página: Cancelados
    # =====================================================================
    story.append(crear_seccion_titulo("8. REGISTROS CANCELADOS", styles))
    story.append(Spacer(1, 6))

    # --- Cancelados MXN ---
    story.append(crear_subtitulo("Moneda: MXN", styles))
    tabla_canc = [headers_aj]
    for c in data_cancelados_mxn:
        tabla_canc.append(["MXN", c["tipo"], c["concepto"], fmt_int(c["num_registros"]),
                           fmt_money(c["importe_total"]), fmt_money(c["impuesto_total"]), fmt_money(c["monto_total"])])

    t_canc = Table(tabla_canc, colWidths=col_aj)
    s_canc = estilo_tabla_base(len(tabla_canc) - 1)
    s_canc.extend([
        ('ALIGN', (3, 1), (-1, -1), 'RIGHT'),
        ('ALIGN', (0, 1), (2, -1), 'LEFT'),
    ])
    t_canc.setStyle(TableStyle(s_canc))
    story.append(t_canc)
    story.append(Spacer(1, 8))

    # --- Cancelados USD ---
    story.append(crear_subtitulo("Moneda: USD", styles))
    tabla_canc_usd = [headers_aj]
    for c in data_cancelados_usd:
        tabla_canc_usd.append(["USD", c["tipo"], c["concepto"], fmt_int(c["num_registros"]),
                               fmt_money(c["importe_total"]), fmt_money(c["impuesto_total"]), fmt_money(c["monto_total"])])

    t_canc_usd = Table(tabla_canc_usd, colWidths=col_aj)
    s_canc_usd = estilo_tabla_base(len(tabla_canc_usd) - 1)
    s_canc_usd.extend([
        ('ALIGN', (3, 1), (-1, -1), 'RIGHT'),
        ('ALIGN', (0, 1), (2, -1), 'LEFT'),
    ])
    t_canc_usd.setStyle(TableStyle(s_canc_usd))
    story.append(t_canc_usd)

    # =====================================================================
    # CONSTRUIR PDF
    # =====================================================================
    doc.build(story, canvasmaker=HeaderFooter)
    print(f"\n✅ Reporte generado exitosamente: {archivo_salida}")
    print(f"   Tamaño: {os.path.getsize(archivo_salida) / 1024:.1f} KB")


# =============================================================================
# PUNTO DE ENTRADA
# =============================================================================
if __name__ == "__main__":
    generar_reporte()