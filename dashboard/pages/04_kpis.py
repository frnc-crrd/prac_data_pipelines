"""Página 4: KPIs Estratégicos.

Vista profunda de los cinco indicadores clave: DSO, CEI, Índice de
Morosidad, análisis Pareto/ABC y utilización de límite de crédito.
Incluye interpretación textual de cada KPI y gráficas de gauge.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import streamlit as st

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from dashboard.data_loader import cargar_kpis
from config.settings import KPI_PERIODO_DIAS

# ======================================================================
# HEADER
# ======================================================================
st.markdown(
    """
    <div class="main-header">
        <h1>🎯 KPIs Estratégicos de Cobranza</h1>
        <p>Indicadores clave para evaluación estratégica de la gestión de cuentas por cobrar</p>
    </div>
    """,
    unsafe_allow_html=True,
)

# ======================================================================
# CARGA DE DATOS
# ======================================================================
try:
    kpis_data = cargar_kpis()
except Exception as e:
    st.error(f"❌ Error al cargar datos: {e}")
    st.stop()

kpis_resumen      = kpis_data.get("kpis_resumen", pd.DataFrame())
concentracion     = kpis_data.get("kpis_concentracion", pd.DataFrame())
limite_credito    = kpis_data.get("kpis_limite_credito", pd.DataFrame())
morosidad_cliente = kpis_data.get("kpis_morosidad_cliente", pd.DataFrame())

st.caption(f"Período de análisis: últimos {KPI_PERIODO_DIAS} días")
st.divider()


# ======================================================================
# HELPERS
# ======================================================================
def _get_kpi_row(nombre: str) -> dict:
    """Extrae fila de KPI del DataFrame resumen como diccionario."""
    if kpis_resumen.empty:
        return {}
    row = kpis_resumen[kpis_resumen["KPI"].str.contains(nombre, case=False, na=False)]
    return row.iloc[0].to_dict() if not row.empty else {}


def _gauge(valor: float, min_val: float, max_val: float, umbral_ok: float,
           umbral_warn: float, titulo: str, unidad: str,
           invertir: bool = False) -> go.Figure:
    """Crea una gráfica tipo gauge (velocímetro) con Plotly.

    Args:
        valor:       Valor actual del KPI.
        min_val:     Valor mínimo de la escala.
        max_val:     Valor máximo de la escala.
        umbral_ok:   Límite superior de la zona verde.
        umbral_warn: Límite superior de la zona amarilla.
        titulo:      Título del gauge.
        unidad:      Unidad de medida mostrada en el valor.
        invertir:    Si True, mayor valor = mejor (ej: CEI).

    Returns:
        Figura de Plotly.
    """
    if not invertir:
        # Menor es mejor (DSO, Morosidad)
        pasos = [
            {"range": [min_val, umbral_ok],   "color": "#dcfce7"},
            {"range": [umbral_ok, umbral_warn], "color": "#fef9c3"},
            {"range": [umbral_warn, max_val],  "color": "#fee2e2"},
        ]
    else:
        # Mayor es mejor (CEI)
        pasos = [
            {"range": [min_val, umbral_warn],  "color": "#fee2e2"},
            {"range": [umbral_warn, umbral_ok], "color": "#fef9c3"},
            {"range": [umbral_ok, max_val],    "color": "#dcfce7"},
        ]

    fig = go.Figure(go.Indicator(
        mode="gauge+number+delta",
        value=valor,
        number={"suffix": f" {unidad}", "font": {"size": 28, "color": "#1e3a5f"}},
        title={"text": titulo, "font": {"size": 14, "color": "#475569"}},
        gauge={
            "axis": {"range": [min_val, max_val], "tickwidth": 1, "tickcolor": "#94a3b8"},
            "bar":  {"color": "#2d6a9f", "thickness": 0.3},
            "steps": pasos,
            "threshold": {
                "line": {"color": "#1e3a5f", "width": 3},
                "thickness": 0.75,
                "value": valor,
            },
        },
    ))
    fig.update_layout(
        height=220,
        margin=dict(t=40, b=10, l=20, r=20),
        paper_bgcolor="white",
    )
    return fig


# ======================================================================
# SECCIÓN 1: DSO
# ======================================================================
st.subheader("DSO — Days Sales Outstanding")

dso_row = _get_kpi_row("DSO")
dso_val = float(dso_row.get("VALOR", 0))

dso_col1, dso_col2 = st.columns([1, 2])

with dso_col1:
    st.plotly_chart(
        _gauge(dso_val, 0, 120, 45, 70, "DSO", "días", invertir=False),
        use_container_width=True,
    )

with dso_col2:
    st.markdown("#### ¿Qué mide el DSO?")
    st.markdown(
        "Los **días promedio que tarda la empresa en convertir sus ventas a crédito en efectivo**. "
        "Un DSO alto indica lentitud en cobranza o clientes con problemas de pago."
    )

    col_a, col_b, col_c = st.columns(3)
    with col_a:
        st.markdown('<div class="alert-ok">✅ <strong>&lt; 45 días</strong><br>Cobranza eficiente</div>', unsafe_allow_html=True)
    with col_b:
        st.markdown('<div class="alert-warning">⚠️ <strong>45-70 días</strong><br>Requiere atención</div>', unsafe_allow_html=True)
    with col_c:
        st.markdown('<div class="alert-critico">🚨 <strong>&gt; 70 días</strong><br>Problema crítico</div>', unsafe_allow_html=True)

    if dso_row.get("INTERPRETACION"):
        st.info(f"📊 {dso_row['INTERPRETACION']}")

st.divider()

# ======================================================================
# SECCIÓN 2: CEI
# ======================================================================
st.subheader("CEI — Collection Effectiveness Index")

cei_row = _get_kpi_row("CEI")
cei_val = float(cei_row.get("VALOR", 0))

cei_col1, cei_col2 = st.columns([1, 2])

with cei_col1:
    st.plotly_chart(
        _gauge(cei_val, 0, 100, 80, 60, "CEI", "%", invertir=True),
        use_container_width=True,
    )

with cei_col2:
    st.markdown("#### ¿Qué mide el CEI?")
    st.markdown(
        "El **porcentaje del total cobrable que efectivamente se recuperó** en el período. "
        "Complementa al DSO: el DSO dice *qué tan rápido* cobras, el CEI dice *qué tan bien* cobras."
    )

    col_a, col_b, col_c = st.columns(3)
    with col_a:
        st.markdown('<div class="alert-ok">✅ <strong>≥ 80%</strong><br>Eficiencia aceptable</div>', unsafe_allow_html=True)
    with col_b:
        st.markdown('<div class="alert-warning">⚠️ <strong>60-79%</strong><br>Mejorable</div>', unsafe_allow_html=True)
    with col_c:
        st.markdown('<div class="alert-critico">🚨 <strong>&lt; 60%</strong><br>Problemas serios</div>', unsafe_allow_html=True)

    if cei_row.get("INTERPRETACION"):
        st.info(f"📊 {cei_row['INTERPRETACION']}")

st.divider()

# ======================================================================
# SECCIÓN 3: ÍNDICE DE MOROSIDAD
# ======================================================================
st.subheader("Índice de Morosidad")

mor_row = _get_kpi_row("Morosidad")
mor_val = float(mor_row.get("VALOR", 0))

mor_col1, mor_col2 = st.columns([1, 2])

with mor_col1:
    st.plotly_chart(
        _gauge(mor_val, 0, 100, 10, 25, "Morosidad", "%", invertir=False),
        use_container_width=True,
    )

with mor_col2:
    st.markdown("#### ¿Qué mide el Índice de Morosidad?")
    st.markdown(
        "La **proporción de la cartera cuya fecha de vencimiento ya pasó**. "
        "Valores por encima del 25% indican cartera deteriorada que requiere acciones correctivas."
    )

    col_a, col_b, col_c, col_d = st.columns(4)
    with col_a:
        st.markdown('<div class="alert-ok">✅ <strong>&lt; 10%</strong><br>Sana</div>', unsafe_allow_html=True)
    with col_b:
        st.markdown('<div class="alert-warning">⚠️ <strong>10-25%</strong><br>Atención</div>', unsafe_allow_html=True)
    with col_c:
        st.markdown('<div class="alert-critico">🚨 <strong>&gt; 25%</strong><br>Deteriorada</div>', unsafe_allow_html=True)
    with col_d:
        st.markdown('<div class="alert-critico">🆘 <strong>&gt; 50%</strong><br>Crisis</div>', unsafe_allow_html=True)

    if mor_row.get("INTERPRETACION"):
        st.info(f"📊 {mor_row['INTERPRETACION']}")

st.divider()

# ======================================================================
# SECCIÓN 4: ANÁLISIS PARETO / ABC
# ======================================================================
st.subheader("Concentración de Cartera — Análisis Pareto 80/20")

if not concentracion.empty:
    pareto_col1, pareto_col2 = st.columns([1.5, 1])

    with pareto_col1:
        # Gráfica de curva de Pareto
        df_pareto = concentracion.copy()
        df_pareto["RANK"] = range(1, len(df_pareto) + 1)

        fig_pareto = go.Figure()
        # Barras de saldo por cliente (Top 20)
        top20 = df_pareto.head(20)
        fig_pareto.add_trace(go.Bar(
            x=top20["NOMBRE_CLIENTE"],
            y=top20["SALDO"],
            name="Saldo",
            marker_color="#3b82f6",
            opacity=0.8,
            yaxis="y1",
        ))
        # Línea de % acumulado
        fig_pareto.add_trace(go.Scatter(
            x=top20["NOMBRE_CLIENTE"],
            y=top20["PCT_ACUMULADO"],
            name="% Acumulado",
            line=dict(color="#ef4444", width=2),
            mode="lines+markers",
            yaxis="y2",
        ))
        # Línea de referencia 80%
        fig_pareto.add_hline(
            y=80, line_dash="dash", line_color="#22c55e",
            annotation_text="80%", yref="y2",
        )

        fig_pareto.update_layout(
            xaxis=dict(tickangle=-45, showgrid=False),
            yaxis=dict(title="Saldo ($)", showgrid=True, gridcolor="#f1f5f9"),
            yaxis2=dict(
                title="% Acumulado",
                overlaying="y", side="right",
                range=[0, 110],
                showgrid=False,
            ),
            legend=dict(orientation="h", y=1.1),
            plot_bgcolor="white",
            paper_bgcolor="white",
            margin=dict(t=40, b=100, l=10, r=60),
            height=380,
        )
        st.plotly_chart(fig_pareto, use_container_width=True)

    with pareto_col2:
        # Resumen ABC
        if "CLASIFICACION" in concentracion.columns:
            abc = concentracion.groupby("CLASIFICACION").agg(
                CLIENTES=("NOMBRE_CLIENTE", "count"),
                SALDO=("SALDO", "sum"),
            ).reset_index()

            total_saldo = concentracion["SALDO"].sum()
            total_clientes = len(concentracion)

            for _, row in abc.iterrows():
                clase = row["CLASIFICACION"]
                saldo = row["SALDO"]
                clientes = row["CLIENTES"]
                pct_s = saldo / total_saldo * 100 if total_saldo > 0 else 0
                pct_c = clientes / total_clientes * 100 if total_clientes > 0 else 0

                desc = {
                    "A": ("🔴", "Top 80% del saldo — máxima prioridad"),
                    "B": ("🟡", "Siguiente 15% — seguimiento regular"),
                    "C": ("🟢", "Último 5% — gestión estándar"),
                }.get(clase, ("⚪", ""))

                icono, descripcion = desc
                st.markdown(
                    f"""
                    **{icono} Clase {clase}**
                    - {clientes} clientes ({pct_c:.1f}%)
                    - ${saldo:,.2f} ({pct_s:.1f}% del total)
                    - *{descripcion}*
                    """,
                )
                st.divider()

    # Tabla completa de concentración
    with st.expander("Ver tabla completa de concentración"):
        display_conc = concentracion.copy()
        if "SALDO" in display_conc.columns:
            display_conc["SALDO"] = display_conc["SALDO"].apply(lambda x: f"${x:,.2f}")
        if "PCT_DEL_TOTAL" in display_conc.columns:
            display_conc["PCT_DEL_TOTAL"] = display_conc["PCT_DEL_TOTAL"].apply(lambda x: f"{x:.2f}%")
        if "PCT_ACUMULADO" in display_conc.columns:
            display_conc["PCT_ACUMULADO"] = display_conc["PCT_ACUMULADO"].apply(lambda x: f"{x:.2f}%")

        st.dataframe(display_conc, use_container_width=True, hide_index=True)

else:
    st.info("Sin datos de concentración disponibles.")

st.divider()

# ======================================================================
# SECCIÓN 5: MOROSIDAD POR CLIENTE — TOP RIESGOS
# ======================================================================
st.subheader("Top 10 Clientes por Saldo Vencido")

if not morosidad_cliente.empty and "SALDO_VENCIDO" in morosidad_cliente.columns:
    top_riesgo = morosidad_cliente[morosidad_cliente["SALDO_VENCIDO"] > 0].nlargest(10, "SALDO_VENCIDO")

    if not top_riesgo.empty:
        fig_riesgo = px.bar(
            top_riesgo,
            x="SALDO_VENCIDO",
            y="NOMBRE_CLIENTE",
            orientation="h",
            color="DIAS_VENCIDO_MAX",
            color_continuous_scale=["#22c55e", "#f59e0b", "#ef4444"],
            labels={
                "SALDO_VENCIDO": "Saldo Vencido ($)",
                "NOMBRE_CLIENTE": "",
                "DIAS_VENCIDO_MAX": "Días vencido",
            },
            text="SALDO_VENCIDO",
        )
        fig_riesgo.update_traces(texttemplate="$%{text:,.0f}", textposition="outside")
        fig_riesgo.update_layout(
            plot_bgcolor="white",
            paper_bgcolor="white",
            yaxis=dict(autorange="reversed"),
            margin=dict(t=20, b=20, l=150, r=120),
            coloraxis_colorbar=dict(title="Días<br>vencido"),
            height=400,
        )
        st.plotly_chart(fig_riesgo, use_container_width=True)
    else:
        st.success("✅ No hay clientes con saldo vencido.")
else:
    st.info("Sin datos de morosidad por cliente disponibles.")