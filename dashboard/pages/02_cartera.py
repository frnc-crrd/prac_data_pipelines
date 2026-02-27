"""Página 2: Cartera & Antigüedad.

Análisis detallado de la composición de la cartera por rangos de
antigüedad, tabla pivote por cliente y comparativa de cartera
vencida vs vigente con métricas de días.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from dashboard.data_loader import cargar_analytics

# ======================================================================
# HEADER
# ======================================================================
st.markdown(
    """
    <div class="main-header">
        <h1>🗂️ Cartera & Antigüedad</h1>
        <p>Distribución de la cartera por rangos de tiempo y análisis de vencimientos</p>
    </div>
    """,
    unsafe_allow_html=True,
)

# ======================================================================
# CARGA DE DATOS
# ======================================================================
try:
    analytics = cargar_analytics()
except Exception as e:
    st.error(f"❌ Error al cargar datos: {e}")
    st.stop()

antiguedad      = analytics.get("antiguedad_cartera", pd.DataFrame())
por_cliente     = analytics.get("antiguedad_por_cliente", pd.DataFrame())
vencida_vigente = analytics.get("cartera_vencida_vs_vigente", pd.DataFrame())
resumen_cliente = analytics.get("resumen_por_cliente", pd.DataFrame())

# ======================================================================
# SECCIÓN 1: MÉTRICAS DE ANTIGÜEDAD
# ======================================================================
st.subheader("Resumen Global de Antigüedad")

if not antiguedad.empty:
    total_cartera = antiguedad["IMPORTE_TOTAL"].sum()
    n_documentos  = int(antiguedad["NUM_DOCUMENTOS"].sum())

    m1, m2, m3, m4 = st.columns(4)
    with m1:
        st.metric("Total Cartera", f"${total_cartera:,.2f}")
    with m2:
        st.metric("Total Documentos", f"{n_documentos:,}")
    with m3:
        # Cartera vencida = todo excepto "Vigente"
        vencida = antiguedad[antiguedad["RANGO_ANTIGUEDAD"] != "Vigente"]["IMPORTE_TOTAL"].sum()
        st.metric("Total Vencido", f"${vencida:,.2f}")
    with m4:
        pct_vencido = (vencida / total_cartera * 100) if total_cartera > 0 else 0
        st.metric("% Vencido", f"{pct_vencido:.1f}%")

    st.divider()

# ======================================================================
# SECCIÓN 2: GRÁFICAS DE ANTIGÜEDAD
# ======================================================================
graf_col1, graf_col2 = st.columns(2)

with graf_col1:
    st.subheader("Importe por Rango de Antigüedad")
    if not antiguedad.empty:
        colores = {
            "Vigente":          "#22c55e",
            "0-30 días":        "#3b82f6",
            "31-60 días":       "#f59e0b",
            "61-90 días":       "#f97316",
            "91-120 días":      "#ef4444",
            "Más de 120 días":  "#7f1d1d",
            "Sin fecha":        "#94a3b8",
        }
        fig = px.bar(
            antiguedad,
            x="RANGO_ANTIGUEDAD",
            y="IMPORTE_TOTAL",
            color="RANGO_ANTIGUEDAD",
            color_discrete_map=colores,
            text="PCT_DEL_TOTAL",
            labels={"IMPORTE_TOTAL": "Importe ($)", "RANGO_ANTIGUEDAD": "Rango"},
        )
        fig.update_traces(
            texttemplate="%{text:.1f}%",
            textposition="outside",
        )
        fig.update_layout(
            showlegend=False,
            plot_bgcolor="white",
            paper_bgcolor="white",
            margin=dict(t=20, b=40, l=10, r=10),
            xaxis=dict(showgrid=False, title=""),
            yaxis=dict(showgrid=True, gridcolor="#f1f5f9", title="Importe ($)"),
        )
        st.plotly_chart(fig, use_container_width=True)

with graf_col2:
    st.subheader("Número de Documentos por Rango")
    if not antiguedad.empty:
        fig2 = px.pie(
            antiguedad,
            names="RANGO_ANTIGUEDAD",
            values="NUM_DOCUMENTOS",
            hole=0.4,
            color="RANGO_ANTIGUEDAD",
            color_discrete_map=colores,
        )
        fig2.update_layout(
            margin=dict(t=20, b=40, l=10, r=10),
            paper_bgcolor="white",
            legend=dict(orientation="v", x=1.0, y=0.5),
        )
        fig2.update_traces(textinfo="percent+label", textfont_size=11)
        st.plotly_chart(fig2, use_container_width=True)

st.divider()

# ======================================================================
# SECCIÓN 3: TABLA DE ANTIGÜEDAD DETALLADA
# ======================================================================
st.subheader("Detalle por Rango de Antigüedad")

if not antiguedad.empty:
    display_ant = antiguedad.copy()

    for col in ["IMPORTE_TOTAL", "IMPORTE_PROMEDIO", "IMPORTE_MAX"]:
        if col in display_ant.columns:
            display_ant[col] = display_ant[col].apply(lambda x: f"${x:,.2f}")

    if "PCT_DEL_TOTAL" in display_ant.columns:
        display_ant["PCT_DEL_TOTAL"] = display_ant["PCT_DEL_TOTAL"].apply(lambda x: f"{x:.2f}%")

    st.dataframe(
        display_ant,
        use_container_width=True,
        hide_index=True,
        column_config={
            "RANGO_ANTIGUEDAD":  st.column_config.TextColumn("Rango"),
            "NUM_DOCUMENTOS":    st.column_config.NumberColumn("Documentos", format="%d"),
            "IMPORTE_TOTAL":     st.column_config.TextColumn("Importe Total"),
            "IMPORTE_PROMEDIO":  st.column_config.TextColumn("Promedio"),
            "IMPORTE_MAX":       st.column_config.TextColumn("Máximo"),
            "PCT_DEL_TOTAL":     st.column_config.TextColumn("% del Total"),
        },
    )

st.divider()

# ======================================================================
# SECCIÓN 4: VENCIDA VS VIGENTE
# ======================================================================
st.subheader("Cartera Vencida vs Vigente")

if not vencida_vigente.empty:
    vv_col1, vv_col2 = st.columns([1, 1.5])

    with vv_col1:
        for _, row in vencida_vigente.iterrows():
            estatus = row.get("ESTATUS_VENCIMIENTO", "")
            importe = row.get("IMPORTE_TOTAL", 0)
            pct     = row.get("PCT_DEL_TOTAL", 0)
            ndocs   = row.get("NUM_DOCUMENTOS", 0)
            dias_p  = row.get("DIAS_VENCIDO_PROMEDIO", 0)

            if estatus == "VENCIDO":
                css = "alert-critico"
                icono = "🚨"
            else:
                css = "alert-ok"
                icono = "✅"

            st.markdown(
                f'<div class="{css}">'
                f'{icono} <strong>{estatus}</strong><br>'
                f'Importe: <strong>${importe:,.2f}</strong> ({pct:.1f}%)<br>'
                f'Documentos: {int(ndocs):,} | '
                f'Días vencido prom.: {dias_p:.0f}'
                f'</div>',
                unsafe_allow_html=True,
            )

    with vv_col2:
        fig_vv = px.bar(
            vencida_vigente,
            x="ESTATUS_VENCIMIENTO",
            y="IMPORTE_TOTAL",
            color="ESTATUS_VENCIMIENTO",
            color_discrete_map={"VENCIDO": "#ef4444", "VIGENTE": "#22c55e"},
            text="IMPORTE_TOTAL",
            labels={"IMPORTE_TOTAL": "Importe ($)", "ESTATUS_VENCIMIENTO": ""},
        )
        fig_vv.update_traces(
            texttemplate="$%{text:,.0f}",
            textposition="outside",
        )
        fig_vv.update_layout(
            showlegend=False,
            plot_bgcolor="white",
            paper_bgcolor="white",
            margin=dict(t=40, b=20, l=10, r=10),
            yaxis=dict(showgrid=True, gridcolor="#f1f5f9"),
            xaxis=dict(showgrid=False),
        )
        st.plotly_chart(fig_vv, use_container_width=True)

st.divider()

# ======================================================================
# SECCIÓN 5: PIVOTE POR CLIENTE
# ======================================================================
st.subheader("Antigüedad Desglosada por Cliente")

if not por_cliente.empty:
    # Filtro de búsqueda
    busqueda = st.text_input("🔍 Buscar cliente", placeholder="Escribe parte del nombre...")

    df_pivote = por_cliente.copy()
    if busqueda:
        df_pivote = df_pivote[
            df_pivote["NOMBRE_CLIENTE"].str.contains(busqueda, case=False, na=False)
        ]

    st.dataframe(
        df_pivote,
        use_container_width=True,
        hide_index=True,
    )
    st.caption(f"Mostrando {len(df_pivote):,} de {len(por_cliente):,} clientes")
else:
    st.info("Sin datos de antigüedad por cliente disponibles.")