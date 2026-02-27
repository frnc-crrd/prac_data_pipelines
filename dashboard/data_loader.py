"""Capa de carga y caché de datos para el dashboard CxC.

Este módulo es el único punto de contacto entre la capa de presentación
(Streamlit) y la lógica de negocio (src/).  Centraliza la conexión a
Firebird y aplica caché con TTL de 1 hora para evitar reconexiones en
cada interacción del usuario.

Si en el futuro migras a Dash, solo reemplazas el decorador
``@st.cache_data`` por la estrategia de caché de Dash (Flask-Caching
o variable global con timestamp).  El resto del archivo no cambia.

Uso desde cualquier página:
    from dashboard.data_loader import cargar_kpis, cargar_analytics
    kpis = cargar_kpis()
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import streamlit as st

# Asegurar que el raíz del proyecto esté en el path
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from config.settings import (
    ANOMALIAS,
    FIREBIRD_CONFIG,
    KPI_PERIODO_DIAS,
    RANGOS_ANTIGUEDAD,
    SQL_FILE,
)
from src.analytics import Analytics
from src.auditor import Auditor
from src.db_connector import FirebirdConnector
from src.kpis import generar_kpis
from src.reporte_cxc import generar_reporte_cxc


# ======================================================================
# DATOS CRUDOS
# ======================================================================

@st.cache_data(ttl=3600, show_spinner="Conectando a Firebird y cargando datos...")
def cargar_datos_crudos() -> pd.DataFrame:
    """Extrae el DataFrame maestro desde Firebird con caché de 1 hora.

    Returns:
        DataFrame crudo con todos los movimientos de CxC.

    Raises:
        Exception: Si no se puede establecer la conexión.
    """
    connector = FirebirdConnector(FIREBIRD_CONFIG)
    return connector.execute_sql_file(SQL_FILE)


# ======================================================================
# MÓDULOS DERIVADOS
# ======================================================================

@st.cache_data(ttl=3600, show_spinner="Calculando KPIs estratégicos...")
def cargar_kpis() -> dict[str, pd.DataFrame]:
    """Devuelve el diccionario completo de KPIs estratégicos.

    Returns:
        Dict con claves: kpis_resumen, kpis_concentracion,
        kpis_limite_credito, kpis_morosidad_cliente.
    """
    df = cargar_datos_crudos()
    return generar_kpis(df, KPI_PERIODO_DIAS)


@st.cache_data(ttl=3600, show_spinner="Analizando cartera...")
def cargar_analytics() -> dict[str, pd.DataFrame]:
    """Devuelve todos los reportes de análisis de cartera.

    Returns:
        Dict con claves: antiguedad_cartera, antiguedad_por_cliente,
        cartera_vencida_vs_vigente, resumen_por_cliente,
        resumen_por_vendedor, resumen_por_concepto, datos_completos.
    """
    df = cargar_datos_crudos()
    analytics = Analytics(RANGOS_ANTIGUEDAD)
    return analytics.run_analytics(df)


@st.cache_data(ttl=3600, show_spinner="Generando reporte operativo...")
def cargar_reporte() -> dict[str, pd.DataFrame]:
    """Devuelve el reporte operativo con saldos y métricas de ciclo.

    Returns:
        Dict con claves: reporte_cxc, por_acreditar, facturas_vivas.
    """
    df = cargar_datos_crudos()
    return generar_reporte_cxc(df)


@st.cache_data(ttl=3600, show_spinner="Ejecutando auditoría de anomalías...")
def cargar_auditoria():
    """Devuelve el AuditResult con todos los hallazgos.

    Returns:
        AuditResult con atributos: resumen, duplicados,
        importes_atipicos, registros_sin_cliente,
        documentos_cancelados, vencimientos_criticos, calidad_datos.
    """
    df = cargar_datos_crudos()
    auditor = Auditor(ANOMALIAS)
    return auditor.run_audit(df)


# ======================================================================
# HELPERS DE FILTRADO
# ======================================================================

def get_clientes(df: pd.DataFrame) -> list[str]:
    """Devuelve lista ordenada de clientes únicos del DataFrame.

    Args:
        df: DataFrame con columna NOMBRE_CLIENTE.

    Returns:
        Lista de nombres de cliente ordenada alfabéticamente.
    """
    if "NOMBRE_CLIENTE" not in df.columns:
        return []
    return sorted(df["NOMBRE_CLIENTE"].dropna().unique().tolist())


def get_vendedores(df: pd.DataFrame) -> list[str]:
    """Devuelve lista ordenada de vendedores únicos del DataFrame.

    Args:
        df: DataFrame con columna VENDEDOR.

    Returns:
        Lista de vendedores ordenada alfabéticamente.
    """
    if "VENDEDOR" not in df.columns:
        return []
    return sorted(df["VENDEDOR"].dropna().unique().tolist())


def filtrar_por_cliente(
    df: pd.DataFrame,
    clientes: list[str],
) -> pd.DataFrame:
    """Filtra el DataFrame por lista de clientes seleccionados.

    Si la lista está vacía devuelve el DataFrame completo.

    Args:
        df:       DataFrame con columna NOMBRE_CLIENTE.
        clientes: Lista de nombres a incluir.

    Returns:
        DataFrame filtrado.
    """
    if not clientes or "NOMBRE_CLIENTE" not in df.columns:
        return df
    return df[df["NOMBRE_CLIENTE"].isin(clientes)]