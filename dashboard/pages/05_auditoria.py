"""Página 5: Auditoría de Anomalías.

Resultados de las reglas de negocio aplicadas sobre los datos crudos:
duplicados, importes atípicos, documentos sin cliente, cancelados,
vencimientos críticos y reporte de calidad de datos.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from dashboard.data_loader import cargar_auditoria

# ======================================================================
# HEADER
# ======================================================================
st.markdown(
    """
    <div class="main-header">
        <h1>🔍 Auditoría de Anomalías</h1>
        <p>Detección de inconsistencias, duplicados y problemas de calidad en los datos de CxC</p>
    </div>
    """,
    unsafe_allow_html=True,
)

# ======================================================================
# CARGA DE DATOS
# ======================================================================
try:
    audit = cargar_auditoria()
except Exception as e:
    st.error(f"❌ Error al cargar auditoría: {e}")
    st.stop()

resumen          = audit.resumen
duplicados       = audit.duplicados
atipicos         = audit.importes_atipicos
sin_cliente      = audit.registros_sin_cliente
cancelados       = audit.documentos_cancelados
venc_criticos    = audit.vencimientos_criticos
calidad_datos    = audit.calidad_datos

# ======================================================================
# SECCIÓN 1: RESUMEN EJECUTIVO DE AUDITORÍA
# ======================================================================
st.subheader("Resumen de Hallazgos")

total_hallazgos = resumen.get("total_hallazgos", 0)
total_registros = resumen.get("total_registros", 0)

if total_hallazgos == 0:
    st.success(f"✅ Auditoría limpia — {total_registros:,} registros revisados sin hallazgos críticos.")
else:
    pct_hallazgos = (total_hallazgos / total_registros * 100) if total_registros > 0 else 0
    if pct_hallazgos < 2:
        nivel = "alert-ok"
        icono = "✅"
        texto = "Tasa de anomalías baja"
    elif pct_hallazgos < 5:
        nivel = "alert-warning"
        icono = "⚠️"
        texto = "Anomalías moderadas — revisar"
    else:
        nivel = "alert-critico"
        icono = "🚨"
        texto = "Alta tasa de anomalías — acción requerida"

    st.markdown(
        f'<div class="{nivel}">{icono} <strong>{texto}</strong> — '
        f'{total_hallazgos:,} hallazgos en {total_registros:,} registros '
        f'({pct_hallazgos:.1f}%)</div>',
        unsafe_allow_html=True,
    )

st.write("")

# Tarjetas por tipo de hallazgo
col1, col2, col3, col4, col5 = st.columns(5)

hallazgos_config = [
    (col1, "duplicados",          "🔁 Duplicados",           resumen.get("duplicados", 0)),
    (col2, "importes_atipicos",   "📊 Importes Atípicos",    resumen.get("importes_atipicos", 0)),
    (col3, "sin_cliente",         "👤 Sin Cliente",          resumen.get("sin_cliente", 0)),
    (col4, "cancelados",          "❌ Cancelados",           resumen.get("cancelados", 0)),
    (col5, "vencimientos_criticos","⏰ Venc. Críticos",      resumen.get("vencimientos_criticos", 0)),
]

for col, clave, titulo, cantidad in hallazgos_config:
    with col:
        color = "🔴" if cantidad > 0 else "🟢"
        st.metric(titulo, f"{color} {cantidad:,}")

st.divider()

# ======================================================================
# SECCIÓN 2: GRÁFICA DE DISTRIBUCIÓN DE HALLAZGOS
# ======================================================================
if total_hallazgos > 0:
    st.subheader("Distribución de Hallazgos por Tipo")

    datos_grafica = pd.DataFrame([
        {"Tipo": "Duplicados",        "Cantidad": resumen.get("duplicados", 0)},
        {"Tipo": "Importes Atípicos", "Cantidad": resumen.get("importes_atipicos", 0)},
        {"Tipo": "Sin Cliente",       "Cantidad": resumen.get("sin_cliente", 0)},
        {"Tipo": "Cancelados",        "Cantidad": resumen.get("cancelados", 0)},
        {"Tipo": "Venc. Críticos",    "Cantidad": resumen.get("vencimientos_criticos", 0)},
    ])
    datos_grafica = datos_grafica[datos_grafica["Cantidad"] > 0]

    if not datos_grafica.empty:
        fig = px.bar(
            datos_grafica,
            x="Tipo",
            y="Cantidad",
            color="Tipo",
            color_discrete_sequence=["#ef4444", "#f97316", "#f59e0b", "#94a3b8", "#3b82f6"],
            text="Cantidad",
        )
        fig.update_traces(textposition="outside")
        fig.update_layout(
            showlegend=False,
            plot_bgcolor="white",
            paper_bgcolor="white",
            margin=dict(t=30, b=20, l=10, r=10),
            xaxis=dict(showgrid=False),
            yaxis=dict(showgrid=True, gridcolor="#f1f5f9"),
        )
        st.plotly_chart(fig, use_container_width=True)

    st.divider()

# ======================================================================
# SECCIÓN 3: DETALLE POR TIPO DE HALLAZGO
# ======================================================================

# Tabs para cada tipo de hallazgo
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "🔁 Duplicados",
    "📊 Importes Atípicos",
    "👤 Sin Cliente",
    "❌ Cancelados",
    "⏰ Venc. Críticos",
    "🗂️ Calidad de Datos",
])

# ── TAB 1: DUPLICADOS ──────────────────────────────────────────────────
with tab1:
    st.markdown("#### Registros con posible duplicación")
    st.markdown(
        "Documentos que comparten la misma combinación de `CLIENTE_ID + FOLIO + CONCEPTO`. "
        "Pueden indicar doble captura o error en el sistema."
    )
    if not duplicados.empty:
        cols_mostrar = [c for c in [
            "NOMBRE_CLIENTE", "FOLIO", "CONCEPTO", "FECHA_EMISION",
            "IMPORTE", "MOTIVO",
        ] if c in duplicados.columns]
        st.dataframe(
            duplicados[cols_mostrar],
            use_container_width=True,
            hide_index=True,
        )
        st.caption(f"{len(duplicados):,} registros posiblemente duplicados")
    else:
        st.success("✅ No se detectaron duplicados.")

# ── TAB 2: IMPORTES ATÍPICOS ───────────────────────────────────────────
with tab2:
    st.markdown("#### Importes con Z-score elevado (outliers estadísticos)")
    st.markdown(
        "Un **Z-score ≥ 3** significa que el importe está a más de 3 desviaciones estándar "
        "de la media — evento estadísticamente raro (< 0.3% en distribución normal). "
        "Puede indicar error de captura o una transacción inusualmente grande."
    )
    if not atipicos.empty:
        cols_mostrar = [c for c in [
            "NOMBRE_CLIENTE", "FOLIO", "CONCEPTO", "FECHA_EMISION",
            "IMPORTE", "ZSCORE_IMPORTE", "MOTIVO",
        ] if c in atipicos.columns]
        display_at = atipicos[cols_mostrar].copy()
        if "IMPORTE" in display_at.columns:
            display_at["IMPORTE"] = display_at["IMPORTE"].apply(lambda x: f"${x:,.2f}" if pd.notna(x) else "")
        if "ZSCORE_IMPORTE" in display_at.columns:
            display_at["ZSCORE_IMPORTE"] = display_at["ZSCORE_IMPORTE"].apply(lambda x: f"{x:.2f}" if pd.notna(x) else "")

        st.dataframe(display_at, use_container_width=True, hide_index=True)
        st.caption(f"{len(atipicos):,} importes atípicos detectados")

        # Mini gráfica de distribución
        if "IMPORTE" in atipicos.columns and len(atipicos) > 1:
            fig_dist = px.box(
                atipicos,
                y="IMPORTE",
                title="Distribución de importes atípicos",
                color_discrete_sequence=["#ef4444"],
            )
            fig_dist.update_layout(
                plot_bgcolor="white", paper_bgcolor="white",
                margin=dict(t=40, b=20, l=10, r=10), height=250,
            )
            st.plotly_chart(fig_dist, use_container_width=True)
    else:
        st.success("✅ No se detectaron importes atípicos.")

# ── TAB 3: SIN CLIENTE ─────────────────────────────────────────────────
with tab3:
    st.markdown("#### Documentos con datos de cliente incompletos")
    st.markdown(
        "Registros sin nombre de cliente, sin tipo de cliente o sin vendedor asignado. "
        "Pueden afectar los reportes por cliente y vendedor."
    )
    if not sin_cliente.empty:
        cols_mostrar = [c for c in [
            "FOLIO", "CONCEPTO", "FECHA_EMISION", "IMPORTE",
            "NOMBRE_CLIENTE", "TIPO_CLIENTE", "VENDEDOR", "MOTIVO",
        ] if c in sin_cliente.columns]
        st.dataframe(
            sin_cliente[cols_mostrar],
            use_container_width=True,
            hide_index=True,
        )
        # Resumen por motivo
        if "MOTIVO" in sin_cliente.columns:
            st.caption("Desglose por motivo:")
            for motivo, cnt in sin_cliente["MOTIVO"].value_counts().items():
                st.caption(f"  • {motivo}: {cnt:,}")
    else:
        st.success("✅ Todos los documentos tienen datos de cliente completos.")

# ── TAB 4: CANCELADOS ──────────────────────────────────────────────────
with tab4:
    st.markdown("#### Documentos cancelados en Microsip")
    st.markdown(
        "Estos documentos están marcados como cancelados en el sistema. "
        "El pipeline los excluye de los cálculos, pero se listan aquí para referencia."
    )
    if not cancelados.empty:
        cols_mostrar = [c for c in [
            "NOMBRE_CLIENTE", "FOLIO", "CONCEPTO", "FECHA_EMISION",
            "IMPORTE", "DIAS_HASTA_CANCELACION", "MOTIVO",
        ] if c in cancelados.columns]
        display_can = cancelados[cols_mostrar].copy()
        if "IMPORTE" in display_can.columns:
            display_can["IMPORTE"] = display_can["IMPORTE"].apply(lambda x: f"${x:,.2f}" if pd.notna(x) else "")
        st.dataframe(display_can, use_container_width=True, hide_index=True)
        st.caption(f"{len(cancelados):,} documentos cancelados")
    else:
        st.success("✅ No se encontraron documentos cancelados.")

# ── TAB 5: VENCIMIENTOS CRÍTICOS ───────────────────────────────────────
with tab5:
    st.markdown("#### Cargos vencidos hace ≥ 90 días")
    st.markdown(
        "La probabilidad de cobro cae drásticamente después de 90 días. "
        "Estos documentos requieren acciones urgentes: renegociación, "
        "agencia de cobranza externa o acciones legales."
    )
    if not venc_criticos.empty:
        # Agrupar por cliente para visualización
        if "NOMBRE_CLIENTE" in venc_criticos.columns and "IMPORTE" in venc_criticos.columns:
            resumen_vc = (
                venc_criticos.groupby("NOMBRE_CLIENTE")
                .agg(
                    NUM_DOCS=("IMPORTE", "count"),
                    IMPORTE_TOTAL=("IMPORTE", "sum"),
                    DIAS_MAX=("DIAS_VENCIDO", "max"),
                )
                .reset_index()
                .sort_values("IMPORTE_TOTAL", ascending=False)
            )

            g_col1, g_col2 = st.columns(2)
            with g_col1:
                st.metric("Clientes afectados", f"{resumen_vc['NOMBRE_CLIENTE'].nunique():,}")
            with g_col2:
                st.metric("Monto en riesgo", f"${venc_criticos['IMPORTE'].sum():,.2f}")

            st.write("")

            display_vc = resumen_vc.copy()
            display_vc["IMPORTE_TOTAL"] = display_vc["IMPORTE_TOTAL"].apply(lambda x: f"${x:,.2f}")
            st.dataframe(
                display_vc,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "NOMBRE_CLIENTE": st.column_config.TextColumn("Cliente"),
                    "NUM_DOCS":       st.column_config.NumberColumn("Documentos", format="%d"),
                    "IMPORTE_TOTAL":  st.column_config.TextColumn("Importe en Riesgo"),
                    "DIAS_MAX":       st.column_config.NumberColumn("Días Vencido Máx", format="%d"),
                },
            )

        with st.expander("Ver todos los documentos individuales"):
            cols_mostrar = [c for c in [
                "NOMBRE_CLIENTE", "FOLIO", "CONCEPTO", "FECHA_VENCIMIENTO",
                "IMPORTE", "DIAS_VENCIDO", "MOTIVO",
            ] if c in venc_criticos.columns]
            st.dataframe(venc_criticos[cols_mostrar], use_container_width=True, hide_index=True)
    else:
        st.success("✅ No hay vencimientos críticos (≥ 90 días).")

# ── TAB 6: CALIDAD DE DATOS ────────────────────────────────────────────
with tab6:
    st.markdown("#### Reporte de calidad por columna")
    st.markdown(
        "Estado de completitud de cada columna del dataset. "
        "Columnas con alto porcentaje de nulos pueden indicar configuración "
        "incompleta en Microsip o campos no utilizados."
    )
    if not calidad_datos.empty:
        display_cd = calidad_datos.copy()
        if "PCT_NULOS" in display_cd.columns:
            display_cd["PCT_NULOS"] = display_cd["PCT_NULOS"].apply(lambda x: f"{x:.1f}%")

        st.dataframe(
            display_cd,
            use_container_width=True,
            hide_index=True,
            column_config={
                "COLUMNA":         st.column_config.TextColumn("Columna"),
                "TIPO_DATO":       st.column_config.TextColumn("Tipo"),
                "TOTAL_REGISTROS": st.column_config.NumberColumn("Total", format="%d"),
                "NULOS":           st.column_config.NumberColumn("Nulos", format="%d"),
                "PCT_NULOS":       st.column_config.TextColumn("% Nulos"),
                "VALORES_UNICOS":  st.column_config.NumberColumn("Valores Únicos", format="%d"),
            },
        )

        # Alertar columnas con > 50% nulos
        if "PCT_NULOS" in calidad_datos.columns:
            criticas = calidad_datos[calidad_datos["NULOS"] / calidad_datos["TOTAL_REGISTROS"] > 0.5]
            if not criticas.empty:
                st.warning(
                    f"⚠️ {len(criticas)} columna(s) con más del 50% de valores nulos: "
                    + ", ".join(f"`{c}`" for c in criticas["COLUMNA"].tolist())
                )
    else:
        st.info("Sin datos de calidad disponibles.")