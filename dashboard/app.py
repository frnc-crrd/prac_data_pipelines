"""Punto de entrada del dashboard CxC.

Configura la app de Streamlit con navegación multipágina,
tema corporativo y sidebar con información del sistema.

Ejecución:
    streamlit run dashboard/app.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# ======================================================================
# CONFIGURACIÓN GLOBAL DE LA APP
# ======================================================================
st.set_page_config(
    page_title="Dashboard CxC — Microsip",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        "Get Help": None,
        "Report a bug": None,
        "About": "Dashboard de Cuentas por Cobrar — Microsip v1.0",
    },
)

# ======================================================================
# ESTILOS GLOBALES
# ======================================================================
st.markdown(
    """
    <style>
        /* Tipografía general */
        html, body, [class*="css"] {
            font-family: 'Segoe UI', sans-serif;
        }

        /* Header principal */
        .main-header {
            background: linear-gradient(135deg, #1e3a5f 0%, #2d6a9f 100%);
            padding: 1.5rem 2rem;
            border-radius: 10px;
            margin-bottom: 1.5rem;
        }
        .main-header h1 {
            color: white;
            margin: 0;
            font-size: 1.8rem;
            font-weight: 700;
        }
        .main-header p {
            color: #b8d4f0;
            margin: 0.3rem 0 0 0;
            font-size: 0.95rem;
        }

        /* Tarjetas de métricas */
        [data-testid="metric-container"] {
            background: white;
            border: 1px solid #e2e8f0;
            border-radius: 10px;
            padding: 1rem;
            box-shadow: 0 1px 3px rgba(0,0,0,0.08);
        }
        [data-testid="metric-container"]:hover {
            box-shadow: 0 4px 12px rgba(0,0,0,0.12);
            transform: translateY(-1px);
            transition: all 0.2s ease;
        }

        /* Sidebar */
        [data-testid="stSidebar"] {
            background: #f8fafc;
            border-right: 1px solid #e2e8f0;
        }

        /* Tablas */
        [data-testid="stDataFrame"] {
            border-radius: 8px;
            overflow: hidden;
        }

        /* Botón de refresh */
        .stButton > button {
            border-radius: 8px;
            border: 1px solid #2d6a9f;
            color: #2d6a9f;
            background: white;
            font-weight: 600;
            transition: all 0.2s;
        }
        .stButton > button:hover {
            background: #2d6a9f;
            color: white;
        }

        /* Alertas personalizadas */
        .alert-critico {
            background: #fef2f2;
            border-left: 4px solid #ef4444;
            padding: 0.75rem 1rem;
            border-radius: 0 8px 8px 0;
            margin: 0.5rem 0;
            color: #7f1d1d;
        }
        .alert-warning {
            background: #fffbeb;
            border-left: 4px solid #f59e0b;
            padding: 0.75rem 1rem;
            border-radius: 0 8px 8px 0;
            margin: 0.5rem 0;
            color: #78350f;
        }
        .alert-ok {
            background: #f0fdf4;
            border-left: 4px solid #22c55e;
            padding: 0.75rem 1rem;
            border-radius: 0 8px 8px 0;
            margin: 0.5rem 0;
            color: #14532d;
        }

        /* Ocultar footer de Streamlit */
        footer { visibility: hidden; }
        #MainMenu { visibility: hidden; }
    </style>
    """,
    unsafe_allow_html=True,
)

# ======================================================================
# NAVEGACIÓN MULTIPÁGINA
# ======================================================================
pages = {
    "📈 Resumen Ejecutivo": "pages/01_resumen.py",
    "🗂️ Cartera & Antigüedad": "pages/02_cartera.py",
    "👥 Análisis por Cliente": "pages/03_clientes.py",
    "🎯 KPIs Estratégicos": "pages/04_kpis.py",
    "🔍 Auditoría": "pages/05_auditoria.py",
}

pg = st.navigation(
    [
        st.Page("pages/01_resumen.py",   title="Resumen Ejecutivo",    icon="📈"),
        st.Page("pages/02_cartera.py",   title="Cartera & Antigüedad", icon="🗂️"),
        st.Page("pages/03_clientes.py",  title="Análisis por Cliente", icon="👥"),
        st.Page("pages/04_kpis.py",      title="KPIs Estratégicos",    icon="🎯"),
        st.Page("pages/05_auditoria.py", title="Auditoría",            icon="🔍"),
    ]
)

# ======================================================================
# SIDEBAR — INFORMACIÓN DEL SISTEMA
# ======================================================================
with st.sidebar:
    st.markdown("### ⚙️ Sistema")
    st.markdown("**Base de datos:** Microsip Firebird")

    st.divider()

    if st.button("🔄 Refrescar datos", use_container_width=True):
        st.cache_data.clear()
        st.success("Caché limpiado. Recargando...")
        st.rerun()

    st.divider()
    st.caption("Dashboard CxC v1.0")
    st.caption("Datos con caché de 1 hora")

# ======================================================================
# EJECUTAR PÁGINA ACTIVA
# ======================================================================
pg.run()