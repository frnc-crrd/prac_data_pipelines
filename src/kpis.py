"""Módulo de KPIs estratégicos de Cuentas por Cobrar (CxC).

Calcula indicadores clave de desempeño (Key Performance Indicators)
para la evaluación estratégica de la gestión de cobranza, a partir
de los datos transaccionales del query maestro de Microsip.

KPIs implementados
------------------

**DSO (Days Sales Outstanding)**
    Días promedio que la empresa tarda en convertir sus cuentas por
    cobrar en efectivo.  Un DSO alto indica lentitud en cobranza o
    clientes con problemas de pago.  Se compara contra las condiciones
    de pago pactadas: si vendes a 30 días y tu DSO es 55, hay un
    desfase de 25 días que impacta directamente el flujo de caja.

**CEI (Collection Effectiveness Index)**
    Porcentaje del total cobrable que efectivamente se recuperó en un
    periodo dado.  Un CEI del 100% significa cobranza perfecta.  A
    diferencia del DSO (que mide velocidad), el CEI mide *efectividad*
    de recuperación.  Valores debajo de 60% señalan problemas serios.

**Índice de Morosidad**
    Proporción de la cartera cuya fecha de vencimiento ya pasó.
    Valores por encima del 25% indican cartera deteriorada que
    requiere acciones correctivas (renegociación, agencia externa,
    acciones legales).

**Concentración de Cartera (Análisis Pareto / 80-20)**
    Identifica si pocos clientes concentran la mayor parte del saldo
    pendiente.  Alta concentración = alto riesgo: si uno de esos
    clientes deja de pagar, el impacto en liquidez es severo.
    Clasifica clientes en A (top 80%), B (siguiente 15%), C (resto).

**Límite de Crédito vs Saldo**
    Mide la utilización de la línea de crédito por cliente.  Clientes
    sobre el 100% representan riesgo inmediato y se deberían bloquear
    para nuevos pedidos hasta regularizar.

Dependencias de datos
---------------------
- El query SQL debe incluir ``CLIENTES.LIMITE_CREDITO`` para el
  análisis de utilización de crédito.
- Los saldos se calculan como ``IMPORTE + IMPUESTO`` en moneda
  original (sin conversión a MXN).
- Se reutiliza la lógica de saldo por factura: cargo menos la suma
  de abonos vinculados vía ``DOCTO_CC_ACR_ID``.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

_CANCELADO_VALUES: list[Any] = ["S", "SI", "s", "si", 1, True, "1"]


# ======================================================================
# FUNCIÓN PRINCIPAL
# ======================================================================

def generar_kpis(
    df_crudo: pd.DataFrame,
    dias_periodo: int = 90,
) -> dict[str, pd.DataFrame]:
    """Orquesta el cálculo de todos los KPIs estratégicos de CxC.

    Flujo interno:
        1. Normalizar datos (tipos de columna, numéricos, fechas).
        2. Filtrar documentos cancelados y movimientos tipo ``A``.
        3. Calcular saldo por factura (cargo − abonos vinculados).
        4. Derivar cada KPI a partir de los saldos y movimientos.

    Args:
        df_crudo:     DataFrame directo del query SQL maestro de CxC
                      de Microsip, con tipos nativos de Firebird.
        dias_periodo: Ventana retrospectiva en días calendario para el
                      cálculo de DSO y CEI.  Por ejemplo, 90 significa
                      "últimos 90 días desde hoy" (default: 90).

    Returns:
        Diccionario con las siguientes claves (cada una un DataFrame):

        ``"kpis_resumen"``
            Tabla con los tres KPIs principales: DSO, CEI e Índice
            de Morosidad, incluyendo valor, unidad e interpretación
            textual.

        ``"kpis_concentracion"``
            Análisis Pareto por cliente: saldo, porcentaje del total,
            porcentaje acumulado y clasificación ABC.

        ``"kpis_limite_credito"``
            Utilización de línea de crédito por cliente: saldo vs
            límite, porcentaje de utilización y nivel de alerta.
            Vacío si el query no incluye ``LIMITE_CREDITO``.

        ``"kpis_morosidad_cliente"``
            Desglose de cartera vencida y vigente por cliente, con
            número de facturas y máximo de días vencidos.
    """
    hoy = pd.Timestamp(datetime.now().date())
    inicio_periodo = hoy - pd.Timedelta(days=dias_periodo)

    df = _preparar(df_crudo)
    df_activo = _filtrar_activos(df)
    saldos = _saldos_por_factura(df_activo)

    dso = _calcular_dso(df_activo, saldos, hoy, inicio_periodo, dias_periodo)
    cei = _calcular_cei(df_activo, hoy, inicio_periodo)
    morosidad = _calcular_morosidad(saldos, hoy)

    resumen = pd.DataFrame([dso, cei, morosidad])

    logger.info(
        "KPIs calculados — DSO: %.1f días | CEI: %.1f%% | Morosidad: %.1f%%",
        dso["VALOR"], cei["VALOR"], morosidad["VALOR"],
    )

    return {
        "kpis_resumen": resumen,
        "kpis_concentracion": _calcular_concentracion(saldos),
        "kpis_limite_credito": _calcular_limite_credito(df_activo, saldos),
        "kpis_morosidad_cliente": _calcular_morosidad_por_cliente(saldos, hoy),
    }


# ======================================================================
# PREPARACIÓN INTERNA
# ======================================================================

def _preparar(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza columnas y tipos del DataFrame crudo.

    Misma lógica de preparación que los demás módulos: mayúsculas en
    nombres, fechas como ``datetime64``, numéricos con ``fillna(0)``,
    y ``TIPO_IMPTE`` normalizado.  Adicionalmente parsea
    ``LIMITE_CREDITO`` si existe.

    Args:
        df: DataFrame crudo del query maestro.

    Returns:
        DataFrame normalizado.
    """
    df = df.copy()
    df.columns = pd.Index([c.upper().strip() for c in df.columns])

    for col in ["FECHA_EMISION", "FECHA_VENCIMIENTO"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    for col in ["IMPORTE", "IMPUESTO", "CARGOS", "ABONOS"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)  # type: ignore[call-overload]

    if "LIMITE_CREDITO" in df.columns:
        df["LIMITE_CREDITO"] = pd.to_numeric(df["LIMITE_CREDITO"], errors="coerce").fillna(0)  # type: ignore[call-overload]

    if "TIPO_IMPTE" in df.columns:
        df["TIPO_IMPTE"] = df["TIPO_IMPTE"].astype(str).str.strip().str.upper()

    return df


def _filtrar_activos(df: pd.DataFrame) -> pd.DataFrame:
    """Excluye documentos cancelados y movimientos tipo ``A``.

    Args:
        df: DataFrame normalizado.

    Returns:
        DataFrame con solo movimientos activos (C y R).
    """
    df_f = df.copy()

    if "CANCELADO" in df_f.columns:
        df_f = df_f[~df_f["CANCELADO"].isin(_CANCELADO_VALUES)]

    if "TIPO_IMPTE" in df_f.columns:
        df_f = df_f[df_f["TIPO_IMPTE"] != "A"]

    return df_f.reset_index(drop=True)


def _saldos_por_factura(df: pd.DataFrame) -> pd.DataFrame:
    """Calcula el saldo pendiente de cada factura (cargo).

    Genera un DataFrame con **una fila por cargo** donde el saldo
    refleja cuánto queda por cobrar de esa factura en particular.

    Fórmula::

        SALDO = (IMPORTE + IMPUESTO) del cargo
                − Σ (IMPORTE + IMPUESTO) de abonos vinculados
                  vía DOCTO_CC_ACR_ID

    Esta función replica internamente la lógica de
    ``reporte_cxc._calcular_saldo_factura()`` pero devuelve un
    DataFrame optimizado para análisis agregado (no para reporte
    por transacción).

    Args:
        df: DataFrame de movimientos activos (sin cancelados ni tipo A).

    Returns:
        DataFrame con columnas: ``DOCTO_CC_ID``, ``NOMBRE_CLIENTE``,
        ``FECHA_VENCIMIENTO``, ``FECHA_EMISION``, ``MONTO_CARGO``,
        ``MONTO_ABONOS``, ``SALDO``.
    """
    df = df.copy()
    df["_MONTO"] = df["IMPORTE"] + df["IMPUESTO"]

    es_cargo = df["TIPO_IMPTE"] == "C"
    es_abono = df["TIPO_IMPTE"] == "R"

    cols_cargo = ["DOCTO_CC_ID", "NOMBRE_CLIENTE", "FECHA_VENCIMIENTO",
                  "FECHA_EMISION", "_MONTO"]
    cols_disponibles = [c for c in cols_cargo if c in df.columns]
    cargos = df.loc[es_cargo, cols_disponibles].copy()
    cargos = cargos.rename(columns={"_MONTO": "MONTO_CARGO"})

    if "DOCTO_CC_ACR_ID" in df.columns and "DOCTO_CC_ID" in cargos.columns:
        abonos_sum: pd.Series[float] = (
            df.loc[es_abono & df["DOCTO_CC_ACR_ID"].notna()]
            .groupby("DOCTO_CC_ACR_ID")["_MONTO"]
            .sum()
            .rename("MONTO_ABONOS")
        )
        cargos = cargos.merge(
            abonos_sum, left_on="DOCTO_CC_ID", right_index=True, how="left",
        )
        cargos["MONTO_ABONOS"] = cargos["MONTO_ABONOS"].fillna(0)
    else:
        cargos["MONTO_ABONOS"] = 0.0

    cargos["SALDO"] = (cargos["MONTO_CARGO"] - cargos["MONTO_ABONOS"]).round(2)

    logger.info(
        "Saldos por factura: %d cargos, saldo total $%.2f",
        len(cargos), float(cargos["SALDO"].sum()),
    )

    return cargos


# ======================================================================
# DSO (Days Sales Outstanding)
# ======================================================================

def _calcular_dso(
    df: pd.DataFrame,
    saldos: pd.DataFrame,
    hoy: pd.Timestamp,
    inicio_periodo: pd.Timestamp,
    dias_periodo: int,
) -> dict[str, Any]:
    """Calcula el DSO (Days Sales Outstanding).

    El DSO representa cuántos días en promedio tarda la empresa en
    cobrar una venta a crédito.  Es el indicador más usado para medir
    la eficiencia del departamento de cobranza.

    Fórmula::

        DSO = (Saldo total CxC / Ventas a crédito del periodo)
              × días del periodo

    Donde:
        - **Saldo total CxC**: Suma de saldos pendientes de todos los
          cargos activos (``IMPORTE + IMPUESTO − abonos vinculados``).
        - **Ventas a crédito del periodo**: Suma de ``IMPORTE + IMPUESTO``
          de todos los cargos (``TIPO_IMPTE = 'C'``) emitidos dentro de
          la ventana de análisis.

    Interpretación práctica:
        - Comparar el DSO contra las condiciones de pago promedio.
        - Un DSO que sube mes a mes indica deterioro en la cobranza.
        - Benchmark general: DSO < 45 días es aceptable.

    Args:
        df:              DataFrame de movimientos activos.
        saldos:          DataFrame de saldos por factura.
        hoy:             Fecha de referencia para el corte.
        inicio_periodo:  Fecha de inicio de la ventana de análisis.
        dias_periodo:    Longitud de la ventana en días calendario.

    Returns:
        Diccionario ``{KPI, VALOR, UNIDAD, INTERPRETACION}``.
    """
    saldo_total: float = float(saldos["SALDO"].sum())

    cargos_periodo = df[
        (df["TIPO_IMPTE"] == "C")
        & (df["FECHA_EMISION"] >= inicio_periodo)
        & (df["FECHA_EMISION"] <= hoy)
    ]
    ventas_periodo: float = float(
        (cargos_periodo["IMPORTE"] + cargos_periodo["IMPUESTO"]).sum()
    )

    dso = (saldo_total / ventas_periodo) * dias_periodo if ventas_periodo > 0 else 0.0

    return {
        "KPI": "DSO (Days Sales Outstanding)",
        "VALOR": round(dso, 1),
        "UNIDAD": "días",
        "INTERPRETACION": (
            f"La empresa tarda en promedio {dso:.1f} días en cobrar. "
            f"Saldo actual: ${saldo_total:,.2f} vs "
            f"${ventas_periodo:,.2f} facturado en {dias_periodo} días."
        ),
    }


# ======================================================================
# CEI (Collection Effectiveness Index)
# ======================================================================

def _calcular_cei(
    df: pd.DataFrame,
    hoy: pd.Timestamp,
    inicio_periodo: pd.Timestamp,
) -> dict[str, Any]:
    """Calcula el CEI (Collection Effectiveness Index).

    El CEI mide qué porcentaje del dinero que *podía* cobrarse en un
    periodo fue efectivamente cobrado.  Complementa al DSO: el DSO
    dice "qué tan rápido cobras" y el CEI dice "qué tan bien cobras".

    Fórmula::

        CEI = (Cobros del periodo / Total cobrable del periodo) × 100

    Donde:
        - **Cobros del periodo**: Suma de ``IMPORTE + IMPUESTO`` de
          todos los abonos (``TIPO_IMPTE = 'R'``) en la ventana.
        - **Total cobrable**: Saldo al inicio + cargos del periodo.
        - **Saldo al inicio**: Reconstruido retrospectivamente como
          ``saldo_actual − cargos_periodo + cobros_periodo``.

    Interpretación:
        - CEI = 100%  → cobranza perfecta.
        - CEI ≥ 80%   → eficiencia aceptable.
        - CEI < 60%   → problemas serios.

    Args:
        df:             DataFrame de movimientos activos.
        hoy:            Fecha de referencia.
        inicio_periodo: Fecha de inicio de la ventana.

    Returns:
        Diccionario ``{KPI, VALOR, UNIDAD, INTERPRETACION}``.
    """
    df = df.copy()
    df["_MONTO"] = df["IMPORTE"] + df["IMPUESTO"]

    es_cargo = df["TIPO_IMPTE"] == "C"
    es_abono = df["TIPO_IMPTE"] == "R"
    en_periodo = (df["FECHA_EMISION"] >= inicio_periodo) & (df["FECHA_EMISION"] <= hoy)

    cargos_periodo: float = float(df.loc[es_cargo & en_periodo, "_MONTO"].sum())
    cobros_periodo: float = float(df.loc[es_abono & en_periodo, "_MONTO"].sum())

    saldo_actual: float = float(
        df.loc[es_cargo, "_MONTO"].sum() - df.loc[es_abono, "_MONTO"].sum()
    )

    saldo_inicio: float = saldo_actual - cargos_periodo + cobros_periodo
    cobrable: float = saldo_inicio + cargos_periodo

    cei = (cobros_periodo / cobrable) * 100 if cobrable > 0 else 100.0

    return {
        "KPI": "CEI (Collection Effectiveness Index)",
        "VALOR": round(cei, 1),
        "UNIDAD": "%",
        "INTERPRETACION": (
            f"Se recuperó el {cei:.1f}% de lo cobrable en el periodo. "
            f"Cobros: ${cobros_periodo:,.2f} de ${cobrable:,.2f} cobrable "
            f"(saldo inicio: ${saldo_inicio:,.2f} + "
            f"cargos periodo: ${cargos_periodo:,.2f})."
        ),
    }


# ======================================================================
# ÍNDICE DE MOROSIDAD
# ======================================================================

def _calcular_morosidad(
    saldos: pd.DataFrame,
    hoy: pd.Timestamp,
) -> dict[str, Any]:
    """Calcula el Índice de Morosidad global de la cartera.

    Fórmula::

        Morosidad = (Cartera vencida / Cartera total) × 100

    Interpretación:
        - < 10%  → cartera sana.
        - 10-25% → atención requerida.
        - > 25%  → cartera deteriorada, acciones urgentes.
        - > 50%  → crisis de cartera.

    Args:
        saldos: DataFrame de saldos por factura.
        hoy:    Fecha de referencia.

    Returns:
        Diccionario ``{KPI, VALOR, UNIDAD, INTERPRETACION}``.
    """
    cartera_total: float = float(saldos["SALDO"].sum())

    if "FECHA_VENCIMIENTO" in saldos.columns:
        vencidos = saldos[saldos["FECHA_VENCIMIENTO"] < hoy]
        cartera_vencida: float = float(vencidos["SALDO"].sum())
    else:
        cartera_vencida = 0.0

    indice = (cartera_vencida / cartera_total) * 100 if cartera_total > 0 else 0.0

    return {
        "KPI": "Índice de Morosidad",
        "VALOR": round(indice, 1),
        "UNIDAD": "%",
        "INTERPRETACION": (
            f"El {indice:.1f}% de la cartera está vencida. "
            f"Vencida: ${cartera_vencida:,.2f} de "
            f"${cartera_total:,.2f} total."
        ),
    }


# ======================================================================
# CONCENTRACIÓN DE CARTERA (PARETO)
# ======================================================================

def _calcular_concentracion(saldos: pd.DataFrame) -> pd.DataFrame:
    """Análisis de concentración de cartera (Principio de Pareto 80/20).

    Agrupa los saldos pendientes por cliente, ordena de mayor a menor
    deudor, y calcula el porcentaje acumulado para identificar la
    distribución del riesgo.

    Clasificación ABC:
        - **A**: Clientes que acumulan hasta el 80% del saldo total.
        - **B**: Siguientes clientes hasta el 95% acumulado.
        - **C**: Resto de la cartera (último 5%).

    Señales de alerta:
        - Si la clase A tiene solo 1-3 clientes → concentración peligrosa.
        - Un solo cliente >30% del total es riesgo existencial.

    Args:
        saldos: DataFrame de saldos por factura.

    Returns:
        DataFrame con columnas: ``NOMBRE_CLIENTE``, ``SALDO``,
        ``PCT_DEL_TOTAL``, ``PCT_ACUMULADO``, ``CLASIFICACION``.
    """
    if "NOMBRE_CLIENTE" not in saldos.columns:
        return pd.DataFrame()

    por_cliente = (
        saldos.groupby("NOMBRE_CLIENTE")["SALDO"]
        .sum()
        .round(2)
        .reset_index()
        .sort_values("SALDO", ascending=False)
        .reset_index(drop=True)
    )

    total: float = float(por_cliente["SALDO"].sum())
    if total <= 0:
        logger.warning("Saldo total ≤ 0 — no se puede calcular concentración.")
        return pd.DataFrame()

    por_cliente["PCT_DEL_TOTAL"] = (por_cliente["SALDO"] / total * 100).round(2)
    por_cliente["PCT_ACUMULADO"] = por_cliente["PCT_DEL_TOTAL"].cumsum().round(2)

    por_cliente["CLASIFICACION"] = np.where(
        por_cliente["PCT_ACUMULADO"] <= 80, "A",
        np.where(por_cliente["PCT_ACUMULADO"] <= 95, "B", "C"),
    )

    n_a = int((por_cliente["CLASIFICACION"] == "A").sum())
    n_total = len(por_cliente)
    logger.info(
        "Concentración Pareto: %d de %d clientes (%.0f%%) acumulan el 80%% del saldo",
        n_a, n_total, n_a / n_total * 100 if n_total else 0,
    )

    return por_cliente


# ======================================================================
# LÍMITE DE CRÉDITO VS SALDO
# ======================================================================

def _calcular_limite_credito(
    df: pd.DataFrame,
    saldos: pd.DataFrame,
) -> pd.DataFrame:
    """Compara el saldo actual vs el límite de crédito por cliente.

    Niveles de alerta:
        - **SOBRE_LIMITE** (>100%): Bloquear nuevos pedidos.
        - **CRITICO** (90-100%): Monitoreo cercano.
        - **ALTO** (70-90%): Evaluar aumento o restricción.
        - **NORMAL** (<70%): Dentro de parámetros.
        - **SIN_LIMITE**: Sin límite asignado en el sistema.

    Requiere ``CLIENTES.LIMITE_CREDITO`` en el query SQL.

    Args:
        df:     DataFrame de movimientos activos.
        saldos: DataFrame de saldos por factura.

    Returns:
        DataFrame con columnas: ``NOMBRE_CLIENTE``, ``SALDO``,
        ``LIMITE_CREDITO``, ``UTILIZACION_PCT``, ``DISPONIBLE``,
        ``ALERTA``.
    """
    if "LIMITE_CREDITO" not in df.columns or "NOMBRE_CLIENTE" not in df.columns:
        logger.warning(
            "Columna LIMITE_CREDITO no encontrada en el query — "
            "agrega 'CLIENTES.LIMITE_CREDITO' al SELECT del SQL."
        )
        return pd.DataFrame()

    limites = (
        df.groupby("NOMBRE_CLIENTE")["LIMITE_CREDITO"]
        .max()
        .reset_index()
    )

    saldo_cliente = (
        saldos.groupby("NOMBRE_CLIENTE")["SALDO"]
        .sum()
        .round(2)
        .reset_index()
    )

    resultado = saldo_cliente.merge(limites, on="NOMBRE_CLIENTE", how="left")
    resultado["LIMITE_CREDITO"] = resultado["LIMITE_CREDITO"].fillna(0)

    resultado["UTILIZACION_PCT"] = np.where(
        resultado["LIMITE_CREDITO"] > 0,
        (resultado["SALDO"] / resultado["LIMITE_CREDITO"] * 100).round(1),
        np.nan,
    )

    resultado["DISPONIBLE"] = (
        resultado["LIMITE_CREDITO"] - resultado["SALDO"]
    ).round(2)

    condiciones = [
        resultado["LIMITE_CREDITO"] == 0,
        resultado["UTILIZACION_PCT"] > 100,
        resultado["UTILIZACION_PCT"] >= 90,
        resultado["UTILIZACION_PCT"] >= 70,
    ]
    opciones = ["SIN_LIMITE", "SOBRE_LIMITE", "CRITICO", "ALTO"]
    resultado["ALERTA"] = np.select(condiciones, opciones, default="NORMAL")

    resultado = resultado.sort_values("SALDO", ascending=False).reset_index(drop=True)

    sobre = int((resultado["ALERTA"] == "SOBRE_LIMITE").sum())
    if sobre > 0:
        logger.warning("%d clientes sobre su límite de crédito.", sobre)

    return resultado


# ======================================================================
# MOROSIDAD POR CLIENTE
# ======================================================================

def _calcular_morosidad_por_cliente(
    saldos: pd.DataFrame,
    hoy: pd.Timestamp,
) -> pd.DataFrame:
    """Desglosa la cartera vencida y vigente por cada cliente.

    Columnas del resultado:
        - ``SALDO_TOTAL``: Deuda total del cliente.
        - ``SALDO_VIGENTE``: Facturas que aún no vencen.
        - ``SALDO_VENCIDO``: Facturas cuya fecha de vencimiento ya pasó.
        - ``PCT_VENCIDO``: Porcentaje de la deuda vencida.
        - ``NUM_FACTURAS``: Total de facturas activas.
        - ``NUM_VENCIDAS``: Cuántas están vencidas.
        - ``DIAS_VENCIDO_MAX``: Mayor número de días vencidos
          (> 90 = zona crítica).

    Args:
        saldos: DataFrame de saldos por factura.
        hoy:    Fecha de referencia.

    Returns:
        DataFrame ordenado por ``SALDO_VENCIDO`` descendente.
    """
    if "NOMBRE_CLIENTE" not in saldos.columns or "FECHA_VENCIMIENTO" not in saldos.columns:
        return pd.DataFrame()

    df = saldos.copy()
    df["DIAS_VENCIDO"] = (hoy - df["FECHA_VENCIMIENTO"]).dt.days.fillna(0).astype(int)
    df["_VENCIDO"] = df["DIAS_VENCIDO"] > 0
    df["_SALDO_VENCIDO"] = np.where(df["_VENCIDO"], df["SALDO"], 0.0)
    df["_SALDO_VIGENTE"] = np.where(~df["_VENCIDO"], df["SALDO"], 0.0)

    por_cliente = df.groupby("NOMBRE_CLIENTE").agg(
        SALDO_TOTAL=("SALDO", "sum"),
        SALDO_VIGENTE=("_SALDO_VIGENTE", "sum"),
        SALDO_VENCIDO=("_SALDO_VENCIDO", "sum"),
        NUM_FACTURAS=("SALDO", "count"),
        NUM_VENCIDAS=("_VENCIDO", "sum"),
        DIAS_VENCIDO_MAX=("DIAS_VENCIDO", "max"),
    ).reset_index()

    por_cliente["SALDO_TOTAL"] = por_cliente["SALDO_TOTAL"].round(2)
    por_cliente["SALDO_VIGENTE"] = por_cliente["SALDO_VIGENTE"].round(2)
    por_cliente["SALDO_VENCIDO"] = por_cliente["SALDO_VENCIDO"].round(2)
    por_cliente["NUM_VENCIDAS"] = por_cliente["NUM_VENCIDAS"].astype(int)

    por_cliente["PCT_VENCIDO"] = np.where(
        por_cliente["SALDO_TOTAL"] > 0,
        (por_cliente["SALDO_VENCIDO"] / por_cliente["SALDO_TOTAL"] * 100).round(1),
        0.0,
    )

    cols = [
        "NOMBRE_CLIENTE", "SALDO_TOTAL", "SALDO_VIGENTE", "SALDO_VENCIDO",
        "PCT_VENCIDO", "NUM_FACTURAS", "NUM_VENCIDAS", "DIAS_VENCIDO_MAX",
    ]
    por_cliente = por_cliente[[c for c in cols if c in por_cliente.columns]]

    return por_cliente.sort_values("SALDO_VENCIDO", ascending=False).reset_index(drop=True)