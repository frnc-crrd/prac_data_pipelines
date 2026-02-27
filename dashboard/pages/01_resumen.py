"""Página 1: Resumen Ejecutivo.

Vista de alto nivel pensada para dirección: KPIs principales en tarjetas
grandes, semáforo de alertas, gráfica de composición de cartera y
tabla de los 10 clientes con mayor saldo pendiente.
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

from dashboard.data_loader import cargar_analytics, cargar_kpis, cargar_reporte

# ======================================================================
# HEADER
# ======================================================================
st.markdown(
    """
    <div class="main-header">
        <h1>📈 Resumen Ejecutivo — Cuentas por Cobrar</h1>
        <p>Visión global del estado de la cartera y principales indicadores de cobranza</p>
    </div>
    """,
    unsafe_allow_html=True,
)

# ======================================================================
# CARGA DE DATOS
# ======================================================================
try:
    kpis_data     = cargar_kpis()
    analytics     = cargar_analytics()
    reporte_data  = cargar_reporte()
except Exception as e:
    st.error(f"❌ Error al cargar datos: {e}")
    st.stop()

kpis_resumen   = kpis_data.get("kpis_resumen", pd.DataFrame())
concentracion  = kpis_data.get("kpis_concentracion", pd.DataFrame())
antiguedad     = analytics.get("antiguedad_cartera", pd.DataFrame())
vencida_vig    = analytics.get("cartera_vencida_vs_vigente", pd.DataFrame())
facturas_vivas = reporte_data.get("facturas_vivas", pd.DataFrame())

# ======================================================================
# SECCIÓN 1: KPIs PRINCIPALES (tarjetas)
# ======================================================================
st.subheader("Indicadores Clave")

def _get_kpi(df: pd.DataFrame, nombre: str) -> tuple[float, str]:
    """Extrae valor y unidad de un KPI del DataFrame resumen."""
    if df.empty:
        return 0.0, ""
    row = df[df["KPI"].str.contains(nombre, case=False, na=False)]
    if row.empty:
        return 0.0, ""
    return float(row.iloc[0]["VALOR"]), str(row.iloc[0]["UNIDAD"])


dso_val,  dso_unit  = _get_kpi(kpis_resumen, "DSO")
cei_val,  cei_unit  = _get_kpi(kpis_resumen, "CEI")
mor_val,  mor_unit  = _get_kpi(kpis_resumen, "Morosidad")

# Saldo total pendiente
saldo_total: float = 0.0
if not concentracion.empty and "SALDO" in concentracion.columns:
    saldo_total = float(concentracion["SALDO"].sum())

col1, col2, col3, col4 = st.columns(4)

with col1:
    delta_dso = "🟢 Bueno" if dso_val < 45 else ("🟡 Atención" if dso_val < 70 else "🔴 Crítico")
    st.metric(
        label="DSO — Días Promedio de Cobro",
        value=f"{dso_val:.1f} días",
        delta=delta_dso,
        delta_color="off",
    )

with col2:
    delta_cei = "🟢 Bueno" if cei_val >= 80 else ("🟡 Atención" if cei_val >= 60 else "🔴 Crítico")
    st.metric(
        label="CEI — Efectividad de Cobro",
        value=f"{cei_val:.1f}%",
        delta=delta_cei,
        delta_color="off",
    )

with col3:
    delta_mor = "🟢 Sana" if mor_val < 10 else ("🟡 Atención" if mor_val < 25 else "🔴 Deteriorada")
    st.metric(
        label="Índice de Morosidad",
        value=f"{mor_val:.1f}%",
        delta=delta_mor,
        delta_color="off",
    )

with col4:
    st.metric(
        label="Saldo Total Pendiente",
        value=f"${saldo_total:,.2f}",
        delta=f"{len(concentracion)} clientes activos" if not concentracion.empty else "",
        delta_color="off",
    )

st.divider()

# ======================================================================
# SECCIÓN 2: SEMÁFORO DE ALERTAS
# ======================================================================
st.subheader("Semáforo de Alertas")

alertas_col1, alertas_col2, alertas_col3 = st.columns(3)

with alertas_col1:
    # DSO
    if dso_val < 45:
        st.markdown('<div class="alert-ok">✅ <strong>DSO en zona segura</strong><br>Cobro promedio dentro de parámetros aceptables (&lt;45 días)</div>', unsafe_allow_html=True)
    elif dso_val < 70:
        st.markdown(f'<div class="alert-warning">⚠️ <strong>DSO elevado: {dso_val:.0f} días</strong><br>Revisar clientes con mayor antigüedad</div>', unsafe_allow_html=True)
    else:
        st.markdown(f'<div class="alert-critico">🚨 <strong>DSO crítico: {dso_val:.0f} días</strong><br>Requiere acción inmediata en cobranza</div>', unsafe_allow_html=True)

with alertas_col2:
    # Morosidad
    if mor_val < 10:
        st.markdown(f'<div class="alert-ok">✅ <strong>Cartera sana: {mor_val:.1f}% vencida</strong><br>Nivel de morosidad bajo control</div>', unsafe_allow_html=True)
    elif mor_val < 25:
        st.markdown(f'<div class="alert-warning">⚠️ <strong>Morosidad: {mor_val:.1f}%</strong><br>Monitorear clientes vencidos</div>', unsafe_allow_html=True)
    else:
        st.markdown(f'<div class="alert-critico">🚨 <strong>Cartera deteriorada: {mor_val:.1f}%</strong><br>Acciones urgentes de cobranza requeridas</div>', unsafe_allow_html=True)

with alertas_col3:
    # Concentración
    if not concentracion.empty and "CLASIFICACION" in concentracion.columns:
        n_clase_a = int((concentracion["CLASIFICACION"] == "A").sum())
        total_clientes = len(concentracion)
        pct_concentracion = round(n_clase_a / total_clientes * 100, 1) if total_clientes else 0

        if n_clase_a <= 3:
            st.markdown(f'<div class="alert-critico">🚨 <strong>Alta concentración: {n_clase_a} clientes = 80% del saldo</strong><br>Riesgo alto de liquidez si alguno falla</div>', unsafe_allow_html=True)
        elif pct_concentracion <= 30:
            st.markdown(f'<div class="alert-ok">✅ <strong>Concentración saludable</strong><br>{n_clase_a} clientes acumulan el 80% del saldo</div>', unsafe_allow_html=True)
        else:
            st.markdown(f'<div class="alert-warning">⚠️ <strong>Concentración moderada</strong><br>{n_clase_a} de {total_clientes} clientes = 80% del saldo</div>', unsafe_allow_html=True)

st.divider()

# ======================================================================
# SECCIÓN 3: GRÁFICAS
# ======================================================================
graf_col1, graf_col2 = st.columns([1.2, 1])

with graf_col1:
    st.subheader("Composición de Cartera por Antigüedad")
    if not antiguedad.empty and "RANGO_ANTIGUEDAD" in antiguedad.columns:
        fig_ant = px.bar(
            antiguedad,
            x="RANGO_ANTIGUEDAD",
            y="IMPORTE_TOTAL",
            color="RANGO_ANTIGUEDAD",
            color_discrete_sequence=["#22c55e", "#3b82f6", "#f59e0b", "#f97316", "#ef4444", "#7f1d1d"],
            text_auto=".2s",
            labels={"RANGO_ANTIGUEDAD": "Rango", "IMPORTE_TOTAL": "Importe ($)"},
        )
        fig_ant.update_layout(
            showlegend=False,
            plot_bgcolor="white",
            paper_bgcolor="white",
            margin=dict(t=20, b=40, l=10, r=10),
            xaxis=dict(showgrid=False),
            yaxis=dict(showgrid=True, gridcolor="#f1f5f9"),
        )
        fig_ant.update_traces(textfont_size=11, textposition="outside")
        st.plotly_chart(fig_ant, use_container_width=True)
    else:
        st.info("Sin datos de antigüedad disponibles.")

with graf_col2:
    st.subheader("Vencida vs Vigente")
    if not vencida_vig.empty and "IMPORTE_TOTAL" in vencida_vig.columns:
        fig_donut = px.pie(
            vencida_vig,
            names="ESTATUS_VENCIMIENTO",
            values="IMPORTE_TOTAL",
            hole=0.55,
            color="ESTATUS_VENCIMIENTO",
            color_discrete_map={"VENCIDO": "#ef4444", "VIGENTE": "#22c55e"},
        )
        fig_donut.update_layout(
            showlegend=True,
            legend=dict(orientation="h", yanchor="bottom", y=-0.2, xanchor="center", x=0.5),
            margin=dict(t=20, b=60, l=10, r=10),
            paper_bgcolor="white",
        )
        fig_donut.update_traces(
            textposition="inside",
            textinfo="percent+label",
            textfont_size=13,
        )
        st.plotly_chart(fig_donut, use_container_width=True)
    else:
        st.info("Sin datos de vencimiento disponibles.")

st.divider()

# ======================================================================
# SECCIÓN 4: TOP 10 CLIENTES POR SALDO
# ======================================================================
st.subheader("Top 10 Clientes por Saldo Pendiente")

if not concentracion.empty:
    top10 = concentracion.head(10).copy()

    cols_mostrar = ["NOMBRE_CLIENTE", "SALDO", "PCT_DEL_TOTAL", "PCT_ACUMULADO", "CLASIFICACION"]
    cols_disponibles = [c for c in cols_mostrar if c in top10.columns]
    top10_display = top10[cols_disponibles].copy()

    if "SALDO" in top10_display.columns:
        top10_display["SALDO"] = top10_display["SALDO"].apply(lambda x: f"${x:,.2f}")
    if "PCT_DEL_TOTAL" in top10_display.columns:
        top10_display["PCT_DEL_TOTAL"] = top10_display["PCT_DEL_TOTAL"].apply(lambda x: f"{x:.1f}%")
    if "PCT_ACUMULADO" in top10_display.columns:
        top10_display["PCT_ACUMULADO"] = top10_display["PCT_ACUMULADO"].apply(lambda x: f"{x:.1f}%")

    st.dataframe(
        top10_display,
        use_container_width=True,
        hide_index=True,
        column_config={
            "NOMBRE_CLIENTE":   st.column_config.TextColumn("Cliente"),
            "SALDO":            st.column_config.TextColumn("Saldo Pendiente"),
            "PCT_DEL_TOTAL":    st.column_config.TextColumn("% del Total"),
            "PCT_ACUMULADO":    st.column_config.TextColumn("% Acumulado"),
            "CLASIFICACION":    st.column_config.TextColumn("Clase ABC"),
        },
    )
else:
    st.info("Sin datos de concentración disponibles.")