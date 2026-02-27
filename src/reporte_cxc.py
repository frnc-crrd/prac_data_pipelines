"""Generador de reporte operativo de Cuentas por Cobrar (CxC).

Toma los datos crudos del query maestro de Microsip y genera:

- ``reporte_cxc``: Movimientos filtrados (sin cancelados, sin tipo A)
  con saldo por factura, saldo acumulado por cliente y metricas
  de ciclo de cobranza.
- ``por_acreditar``: Movimientos con ``TIPO_IMPTE = 'A'`` que representan
  anticipos o pagos pendientes de aplicar.
- ``facturas_abiertas``: Cargos con saldo pendiente (SALDO_FACTURA > 0)
  mas sus abonos parciales. Agrupa cada factura con sus cobros
  aplicados mediante bandas alternas de color.
- ``facturas_cerradas``: Cargos completamente cobrados (SALDO_FACTURA == 0)
  mas todos sus abonos vinculados. Incluye DELTA_RECAUDO y
  CATEGORIA_RECAUDO para analizar el comportamiento de pago.
- ``movimientos_reales_totales``: Union de facturas abiertas y cerradas
  con todas las columnas del reporte operativo, incluyendo
  SALDO_CLIENTE, ambas categorias y ambos deltas.

Logica de saldos (moneda original, sin conversion a MXN):
    ``SALDO_FACTURA``  = (IMPORTE + IMPUESTO) del cargo menos la suma de
                         (IMPORTE + IMPUESTO) de abonos vinculados por
                         ``DOCTO_CC_ACR_ID``. Solo se calcula para
                         movimientos tipo ``C``; el resto queda como NULL.
    ``SALDO_CLIENTE``  = suma acumulada de movimientos por cliente
                         (``C`` suma, ``R`` resta).

Metricas de ciclo de cobranza (solo en filas de cargo):
    ``DELTA_TERMINOS`` = FECHA_VENCIMIENTO menos FECHA_EMISION (dias).
    ``DIAS_PACTADOS``  = dias extraidos de la columna CONDICIONES.
    ``TERMINOS_OK``    = True si DELTA_TERMINOS == DIAS_PACTADOS.
    ``DELTA_RECAUDO``  = Fecha del ultimo abono menos FECHA_VENCIMIENTO.
                         Solo para facturas pagadas (SALDO = 0).
                         Negativo = pago anticipado, positivo = dias
                         de retraso real.
    ``CATEGORIA_RECAUDO`` = Clasificacion del comportamiento de pago segun
                         DELTA_RECAUDO: Pago anticipado, Pago puntual,
                         Retraso leve, moderado, alto o critico.
    ``DELTA_MORA``     = HOY menos FECHA_VENCIMIENTO.
                         Solo para facturas abiertas (SALDO > 0).
    ``CATEGORIA_MORA`` = Clasificacion del riesgo segun DELTA_MORA.
"""

from __future__ import annotations

import logging
from typing import Any

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ======================================================================
# COLUMNAS DE CADA VISTA
# ======================================================================

COLUMNAS_REPORTE: list[str] = [
    "NOMBRE_CLIENTE",
    "MONEDA",
    "CONDICIONES",
    "ESTATUS_CLIENTE",
    "CONCEPTO",
    "FOLIO",
    "FECHA_EMISION",
    "FECHA_VENCIMIENTO",
    "DESCRIPCION",
    "TIPO_IMPTE",
    "CARGOS",
    "ABONOS",
    "IMPORTE",
    "IMPUESTO",
    "SALDO_FACTURA",
    "SALDO_CLIENTE",
    "DELTA_RECAUDO",
    "CATEGORIA_RECAUDO",
    "DELTA_MORA",
    "CATEGORIA_MORA",
]

COLUMNAS_POR_ACREDITAR: list[str] = [
    c for c in COLUMNAS_REPORTE
    if c not in (
        "CONDICIONES", "FECHA_VENCIMIENTO", "CARGOS", "IMPUESTO",
        "SALDO_FACTURA", "SALDO_CLIENTE",
        "DELTA_RECAUDO", "CATEGORIA_RECAUDO",
        "DELTA_MORA", "CATEGORIA_MORA",
    )
]

# Facturas abiertas: sin DELTA_RECAUDO ni CATEGORIA_RECAUDO (no aplica
# a facturas con saldo pendiente) y sin TIPO_IMPTE ni SALDO_CLIENTE.
COLUMNAS_FACTURAS_ABIERTAS: list[str] = [
    "NOMBRE_CLIENTE",
    "MONEDA",
    "CONDICIONES",
    "ESTATUS_CLIENTE",
    "CONCEPTO",
    "FOLIO",
    "FECHA_EMISION",
    "FECHA_VENCIMIENTO",
    "DESCRIPCION",
    "CARGOS",
    "ABONOS",
    "IMPORTE",
    "IMPUESTO",
    "SALDO_FACTURA",
    "DELTA_MORA",
    "CATEGORIA_MORA",
]

# Facturas cerradas: sin DELTA_MORA ni CATEGORIA_MORA (no aplica a
# facturas completamente cobradas) y sin TIPO_IMPTE ni SALDO_CLIENTE.
COLUMNAS_FACTURAS_CERRADAS: list[str] = [
    "NOMBRE_CLIENTE",
    "MONEDA",
    "CONDICIONES",
    "ESTATUS_CLIENTE",
    "CONCEPTO",
    "FOLIO",
    "FECHA_EMISION",
    "FECHA_VENCIMIENTO",
    "DESCRIPCION",
    "CARGOS",
    "ABONOS",
    "IMPORTE",
    "IMPUESTO",
    "SALDO_FACTURA",
    "DELTA_RECAUDO",
    "CATEGORIA_RECAUDO",
]

# Movimientos totales: todas las columnas del reporte operativo completo,
# incluyendo ambos deltas y ambas categorias.
COLUMNAS_MOVIMIENTOS_TOTALES: list[str] = [
    "NOMBRE_CLIENTE",
    "MONEDA",
    "CONDICIONES",
    "ESTATUS_CLIENTE",
    "CONCEPTO",
    "FOLIO",
    "FECHA_EMISION",
    "FECHA_VENCIMIENTO",
    "DESCRIPCION",
    "TIPO_IMPTE",
    "CARGOS",
    "ABONOS",
    "IMPORTE",
    "IMPUESTO",
    "SALDO_FACTURA",
    "SALDO_CLIENTE",
    "DELTA_RECAUDO",
    "CATEGORIA_RECAUDO",
    "DELTA_MORA",
    "CATEGORIA_MORA",
]


# ======================================================================
# FUNCION PRINCIPAL
# ======================================================================


def generar_reporte_cxc(df_crudo: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Genera el reporte operativo completo de CxC.

    Flujo interno:
        1. Extraer movimientos ``TIPO_IMPTE = 'A'`` (por acreditar).
        2. Filtrar cancelados y tipo A del dataset principal.
        3. Calcular ``SALDO_FACTURA`` (cargo menos abonos vinculados).
        4. Calcular metricas de ciclo: ``DELTA_RECAUDO``,
           ``CATEGORIA_RECAUDO``, ``DELTA_MORA`` y ``CATEGORIA_MORA``.
        5. Calcular ``SALDO_CLIENTE`` (acumulado por nombre).
        6. Extraer ``facturas_abiertas`` (saldo > 0 con abonos parciales).
        7. Extraer ``facturas_cerradas`` (saldo == 0 con todos sus abonos).
        8. Construir ``movimientos_totales_cxc`` con todas las columnas
           del dataset mas Z-scores intercalados.
        9. Seleccionar columnas y aplicar bandas de grupo.

    Las fechas se mantienen como ``datetime64`` para que Excel las
    reconozca nativamente y permita agrupar por anio/mes/dia.

    Args:
        df_crudo: DataFrame directo del query SQL maestro de CxC.

    Returns:
        Diccionario con las siguientes claves:

        ``"reporte_cxc"``
            Reporte operativo completo con todas las columnas y metricas.

        ``"por_acreditar"``
            Movimientos tipo A (anticipos sin aplicar).

        ``"movimientos_abiertos_cxc"``
            Cargos con saldo pendiente mas sus abonos parciales,
            con bandas de grupo y metricas de mora.

        ``"movimientos_cerrados_cxc"``
            Cargos completamente cobrados mas sus abonos, con bandas
            de grupo y metricas de recaudo.

        ``"movimientos_totales_cxc"``
            Todas las columnas del dataset filtrado mas Z-scores de
            IMPORTE, DELTA_RECAUDO y DELTA_MORA intercalados junto a
            su columna fuente, con flag ATIPICO_* por cada uno.
    """
    df = _preparar(df_crudo)

    por_acreditar = _obtener_por_acreditar(df)
    df_filtrado = _filtrar_movimientos(df)
    df_filtrado = _calcular_saldo_factura(df_filtrado)
    df_filtrado = _calcular_metricas_ciclo(df_filtrado)
    df_filtrado = _calcular_saldo_cliente(df_filtrado)

    # Extraer vistas antes de aplicar bandas al dataset completo,
    # ya que cada vista aplica su propia llamada a agregar_bandas_grupo.
    facturas_abiertas_raw = _extraer_facturas_abiertas(df_filtrado)
    facturas_cerradas_raw = _extraer_facturas_cerradas(df_filtrado)

    facturas_abiertas = _seleccionar_columnas(
        facturas_abiertas_raw, COLUMNAS_FACTURAS_ABIERTAS,
    )
    if "_BAND_GROUP" in facturas_abiertas_raw.columns:
        facturas_abiertas["_BAND_GROUP"] = (
            facturas_abiertas_raw["_BAND_GROUP"].values
        )

    facturas_cerradas = _seleccionar_columnas(
        facturas_cerradas_raw, COLUMNAS_FACTURAS_CERRADAS,
    )
    if "_BAND_GROUP" in facturas_cerradas_raw.columns:
        facturas_cerradas["_BAND_GROUP"] = (
            facturas_cerradas_raw["_BAND_GROUP"].values
        )

    # Aplicar bandas al dataset completo para reporte y movimientos totales.
    df_filtrado = agregar_bandas_grupo(df_filtrado)

    reporte = _seleccionar_columnas(df_filtrado, COLUMNAS_REPORTE)
    if "_BAND_GROUP" in df_filtrado.columns:
        reporte["_BAND_GROUP"] = df_filtrado["_BAND_GROUP"].values

    # movimientos_totales_cxc: todas las columnas del dataset filtrado
    # (captura las 39+ del query sin lista fija) mas Z-scores intercalados
    # al lado de su columna fuente para identificacion inmediata de atipicos.
    movimientos_totales = _agregar_zscores(df_filtrado.copy())

    por_acreditar = _seleccionar_columnas(
        por_acreditar, COLUMNAS_POR_ACREDITAR,
    )

    n_clientes = (
        reporte["NOMBRE_CLIENTE"].nunique()
        if "NOMBRE_CLIENTE" in reporte.columns else 0
    )
    logger.info(
        "Reporte generado: %d filas, %d clientes", len(reporte), n_clientes,
    )

    return {
        "reporte_cxc":             reporte,
        "por_acreditar":           por_acreditar,
        "movimientos_abiertos_cxc":  facturas_abiertas,
        "movimientos_cerrados_cxc":  facturas_cerradas,
        "movimientos_totales_cxc":   movimientos_totales,
    }



# ======================================================================
# FUNCIONES INTERNAS — ENRIQUECIMIENTO CON Z-SCORES
# ======================================================================


def _insertar_columna_despues(
    df: pd.DataFrame,
    referencia: str,
    nombre: str,
    valores: "pd.Series",
) -> pd.DataFrame:
    """Inserta una columna nueva inmediatamente a la derecha de otra.

    Si la columna de referencia no existe, agrega la nueva al final.

    Args:
        df: DataFrame destino.
        referencia: Nombre de la columna tras la cual insertar.
        nombre: Nombre de la nueva columna.
        valores: Serie con los valores a insertar.

    Returns:
        DataFrame con la nueva columna en la posicion correcta.
    """
    if referencia in df.columns:
        pos = df.columns.get_loc(referencia) + 1
    else:
        pos = len(df.columns)
    df.insert(pos, nombre, valores)
    return df


def _agregar_zscores(
    df: pd.DataFrame,
    umbral: float = 3.0,
) -> pd.DataFrame:
    """Agrega Z-scores e indicadores de atipico al dataset de movimientos.

    Calcula el Z-score de tres indicadores clave de cobranza y lo inserta
    inmediatamente a la derecha de su columna fuente, seguido de una
    columna flag ``ATIPICO_*`` que marca True cuando el Z-score supera
    el umbral configurado.

    Columnas calculadas y su posicion de insercion:

    **Despues de IMPORTE** (solo cargos/debe, TIPO_IMPTE == 'C')::

        ZSCORE_IMPORTE      Z-score del importe de venta respecto a la
                            distribucion de todos los cargos.
        ATIPICO_IMPORTE     True si ZSCORE_IMPORTE >= umbral.
                            None en filas de abono (haber).

    **Despues de DELTA_RECAUDO** (solo facturas cerradas, SALDO == 0)::

        ZSCORE_DELTA_RECAUDO  Z-score de los dias reales de recaudo.
        ATIPICO_DELTA_RECAUDO True si ZSCORE_DELTA_RECAUDO >= umbral.

    **Despues de DELTA_MORA** (solo facturas abiertas, SALDO > 0)::

        ZSCORE_DELTA_MORA   Z-score de los dias de mora acumulados.
        ATIPICO_DELTA_MORA  True si ZSCORE_DELTA_MORA >= umbral.

    Los Z-scores son siempre el valor absoluto para que la comparacion
    con el umbral sea simetrica (tanto valores extremos altos como bajos
    se marcan como atipicos).

    Las filas que no califican para cada calculo (p.ej. abonos para el
    Z-score de IMPORTE) reciben NaN en la columna de Z-score y False
    en la columna flag.

    Args:
        df: DataFrame con las columnas del reporte mas las calculadas
            (SALDO_FACTURA, DELTA_RECAUDO, DELTA_MORA, TIPO_IMPTE).
        umbral: Valor de corte de Z-score para marcar un registro como
            atipico. Por defecto 3.0 (3 desviaciones estandar).

    Returns:
        DataFrame con seis columnas adicionales intercaladas en las
        posiciones descritas arriba.
    """
    df = df.copy()

    # Mover SALDO_CLIENTE antes de DELTA_RECAUDO para que en Excel
    # aparezca junto a SALDO_FACTURA antes del bloque de deltas.
    if "SALDO_CLIENTE" in df.columns and "DELTA_RECAUDO" in df.columns:
        saldo_cliente = df.pop("SALDO_CLIENTE")
        pos = df.columns.get_loc("DELTA_RECAUDO")
        df.insert(pos, "SALDO_CLIENTE", saldo_cliente)

    # Mover las columnas de auditoria/trazabilidad del sistema al final.
    # Son metadatos de Microsip que no forman parte del analisis de negocio
    # y su presencia en medio del reporte interrumpe la lectura.
    _COLS_TRAZABILIDAD: list[str] = [
        "USUARIO_CREADOR",
        "FECHA_HORA_CREACION",
        "USUARIO_ULT_MODIF",
        "FECHA_HORA_ULT_MODIF",
        "USUARIO_CANCELACION",
        "FECHA_HORA_CANCELACION",
    ]
    presentes = [c for c in _COLS_TRAZABILIDAD if c in df.columns]
    if presentes:
        cols_resto = [c for c in df.columns if c not in presentes]
        df = df[cols_resto + presentes]

    # ------------------------------------------------------------------
    # Z-score de IMPORTE — solo cargos (lado del debe, TIPO_IMPTE == C)
    # Los abonos (haber) reciben None en ZSCORE e ATIPICO.
    # ------------------------------------------------------------------
    df["ZSCORE_IMPORTE"] = np.nan
    df["ATIPICO_IMPORTE"] = None

    if "IMPORTE" in df.columns and "TIPO_IMPTE" in df.columns:
        mask_ventas = df["TIPO_IMPTE"] == "C"
        importes = df.loc[mask_ventas, "IMPORTE"].dropna()
        if len(importes) >= 3 and importes.std() > 0:
            zscore_vals = np.abs(
                (df.loc[mask_ventas, "IMPORTE"] - importes.mean())
                / importes.std()
            )
            df.loc[mask_ventas, "ZSCORE_IMPORTE"] = zscore_vals.round(4)
            df.loc[mask_ventas, "ATIPICO_IMPORTE"] = (
                zscore_vals >= umbral
            )
            n_atipicos = int((zscore_vals >= umbral).sum())
            logger.info(
                "Z-score IMPORTE: %d cargos analizados, %d atipicos (>=%.1f)",
                int(mask_ventas.sum()), n_atipicos, umbral,
            )

    df = _insertar_columna_despues(
        df, "IMPORTE", "ZSCORE_IMPORTE", df.pop("ZSCORE_IMPORTE"),
    )
    df = _insertar_columna_despues(
        df, "ZSCORE_IMPORTE", "ATIPICO_IMPORTE", df.pop("ATIPICO_IMPORTE"),
    )

    # ------------------------------------------------------------------
    # Z-score de DELTA_RECAUDO — facturas cerradas (saldo == 0)
    # ------------------------------------------------------------------
    df["ZSCORE_DELTA_RECAUDO"] = np.nan
    df["ATIPICO_DELTA_RECAUDO"] = None

    if "DELTA_RECAUDO" in df.columns:
        vals_recaudo = df["DELTA_RECAUDO"].dropna()
        if len(vals_recaudo) >= 3 and vals_recaudo.std() > 0:
            mask_recaudo = df["DELTA_RECAUDO"].notna()
            zscore_recaudo = np.abs(
                (df.loc[mask_recaudo, "DELTA_RECAUDO"] - vals_recaudo.mean())
                / vals_recaudo.std()
            )
            df.loc[mask_recaudo, "ZSCORE_DELTA_RECAUDO"] = (
                zscore_recaudo.round(4)
            )
            df.loc[mask_recaudo, "ATIPICO_DELTA_RECAUDO"] = (
                zscore_recaudo >= umbral
            )
            n_atipicos = int((zscore_recaudo >= umbral).sum())
            logger.info(
                "Z-score DELTA_RECAUDO: %d facturas analizadas, "
                "%d atipicas (>=%.1f)",
                int(mask_recaudo.sum()), n_atipicos, umbral,
            )

    df = _insertar_columna_despues(
        df,
        "DELTA_RECAUDO",
        "ZSCORE_DELTA_RECAUDO",
        df.pop("ZSCORE_DELTA_RECAUDO"),
    )
    df = _insertar_columna_despues(
        df,
        "ZSCORE_DELTA_RECAUDO",
        "ATIPICO_DELTA_RECAUDO",
        df.pop("ATIPICO_DELTA_RECAUDO"),
    )

    # ------------------------------------------------------------------
    # Z-score de DELTA_MORA — facturas abiertas (saldo > 0)
    # ------------------------------------------------------------------
    df["ZSCORE_DELTA_MORA"] = np.nan
    df["ATIPICO_DELTA_MORA"] = None

    if "DELTA_MORA" in df.columns:
        vals_mora = df["DELTA_MORA"].dropna()
        if len(vals_mora) >= 3 and vals_mora.std() > 0:
            mask_mora = df["DELTA_MORA"].notna()
            zscore_mora = np.abs(
                (df.loc[mask_mora, "DELTA_MORA"] - vals_mora.mean())
                / vals_mora.std()
            )
            df.loc[mask_mora, "ZSCORE_DELTA_MORA"] = zscore_mora.round(4)
            df.loc[mask_mora, "ATIPICO_DELTA_MORA"] = (
                zscore_mora >= umbral
            )
            n_atipicos = int((zscore_mora >= umbral).sum())
            logger.info(
                "Z-score DELTA_MORA: %d facturas analizadas, "
                "%d atipicas (>=%.1f)",
                int(mask_mora.sum()), n_atipicos, umbral,
            )

    df = _insertar_columna_despues(
        df,
        "DELTA_MORA",
        "ZSCORE_DELTA_MORA",
        df.pop("ZSCORE_DELTA_MORA"),
    )
    df = _insertar_columna_despues(
        df,
        "ZSCORE_DELTA_MORA",
        "ATIPICO_DELTA_MORA",
        df.pop("ATIPICO_DELTA_MORA"),
    )

    return df


# ======================================================================
# FUNCIONES INTERNAS — PREPARACION
# ======================================================================


def _preparar(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza columnas, tipos numericos y de fecha del DataFrame crudo.

    Operaciones aplicadas:
        - Nombres de columna en MAYUSCULAS sin espacios.
        - Columnas de fecha convertidas a ``datetime64``.
        - Columnas numericas convertidas a ``float64`` con NaN en 0.
        - ``TIPO_IMPTE`` normalizado a mayusculas sin espacios.

    Args:
        df: DataFrame crudo directo del query de Firebird.

    Returns:
        DataFrame normalizado listo para calculos.
    """
    df = df.copy()
    df.columns = pd.Index([c.upper().strip() for c in df.columns])

    for col in ["FECHA_EMISION", "FECHA_VENCIMIENTO"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    for col in ["IMPORTE", "IMPUESTO", "CARGOS", "ABONOS"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    if "TIPO_IMPTE" in df.columns:
        df["TIPO_IMPTE"] = (
            df["TIPO_IMPTE"].astype(str).str.strip().str.upper()
        )

    return df


def _seleccionar_columnas(
    df: pd.DataFrame,
    columnas: list[str],
) -> pd.DataFrame:
    """Selecciona del DataFrame solo las columnas presentes en la lista.

    Args:
        df: DataFrame fuente.
        columnas: Lista ordenada de nombres de columna deseados.

    Returns:
        Copia del DataFrame con las columnas disponibles en el orden dado.
    """
    cols: list[str] = [c for c in columnas if c in df.columns]
    return df[cols].copy()


_CANCELADO_VALUES: list[Any] = ["S", "SI", "s", "si", 1, True, "1"]
"""Valores que Microsip usa para marcar un documento como cancelado."""


def _obtener_por_acreditar(df: pd.DataFrame) -> pd.DataFrame:
    """Extrae movimientos con ``TIPO_IMPTE = 'A'`` (por acreditar).

    Estos son anticipos o pagos que aun no se han aplicado a una
    factura especifica. Se filtran tambien los cancelados.

    Args:
        df: DataFrame ya preparado por ``_preparar()``.

    Returns:
        DataFrame con los registros tipo A, o vacio si no hay ninguno.
    """
    if "TIPO_IMPTE" not in df.columns:
        logger.warning("Columna TIPO_IMPTE no encontrada.")
        return pd.DataFrame()

    df_a = df[df["TIPO_IMPTE"] == "A"].copy()

    if "CANCELADO" in df_a.columns:
        df_a = df_a[~df_a["CANCELADO"].isin(_CANCELADO_VALUES)]

    if df_a.empty:
        logger.info("No se encontraron registros con TIPO_IMPTE = 'A'.")
        return pd.DataFrame()

    total: float = float((df_a["IMPORTE"] + df_a["IMPUESTO"]).sum())
    n_clientes = (
        df_a["NOMBRE_CLIENTE"].nunique()
        if "NOMBRE_CLIENTE" in df_a.columns else 0
    )
    logger.info(
        "Por Acreditar: %d registros, %d clientes, $%.2f total",
        len(df_a), n_clientes, total,
    )
    return df_a


def _filtrar_movimientos(df: pd.DataFrame) -> pd.DataFrame:
    """Excluye documentos cancelados y movimientos tipo ``A``.

    Args:
        df: DataFrame ya preparado por ``_preparar()``.

    Returns:
        DataFrame filtrado con indice reiniciado.
    """
    df_f = df.copy()
    total_antes = len(df_f)

    if "CANCELADO" in df_f.columns:
        df_f = df_f[~df_f["CANCELADO"].isin(_CANCELADO_VALUES)]
        logger.info(
            "Filtro cancelados: %d -> %d", total_antes, len(df_f),
        )

    antes_a = len(df_f)
    if "TIPO_IMPTE" in df_f.columns:
        df_f = df_f[df_f["TIPO_IMPTE"] != "A"]
        logger.info(
            "Filtro TIPO_IMPTE != 'A': %d -> %d", antes_a, len(df_f),
        )

    return df_f.reset_index(drop=True)


# ======================================================================
# FUNCIONES INTERNAS — CALCULOS DE SALDO
# ======================================================================


def _calcular_saldo_factura(df: pd.DataFrame) -> pd.DataFrame:
    """Calcula el saldo pendiente de cada factura (cargo).

    Formula para cada cargo (``TIPO_IMPTE = 'C'``)::

        SALDO_FACTURA = (IMPORTE + IMPUESTO)
                        menos la suma de (IMPORTE + IMPUESTO) de abonos
                        vinculados via DOCTO_CC_ACR_ID.

    Los movimientos tipo ``R`` (abonos) quedan como NULL porque no
    representan una factura con saldo propio.

    Args:
        df: DataFrame filtrado con movimientos activos.

    Returns:
        DataFrame con columna ``SALDO_FACTURA`` agregada.
    """
    df = df.copy()
    df["SALDO_FACTURA"] = np.nan
    df["_MONTO"] = df["IMPORTE"] + df["IMPUESTO"]

    es_cargo: pd.Series = df["TIPO_IMPTE"] == "C"
    es_abono: pd.Series = df["TIPO_IMPTE"] == "R"

    if "DOCTO_CC_ACR_ID" in df.columns and "DOCTO_CC_ID" in df.columns:
        abonos_por_cargo: pd.Series = (
            df.loc[es_abono & df["DOCTO_CC_ACR_ID"].notna()]
            .groupby("DOCTO_CC_ACR_ID")["_MONTO"]
            .sum()
        )
        cargo_ids = df.loc[es_cargo, "DOCTO_CC_ID"]
        df.loc[es_cargo, "SALDO_FACTURA"] = (
            df.loc[es_cargo, "_MONTO"].values
            - cargo_ids.map(abonos_por_cargo).fillna(0).values
        )
        logger.info(
            "Saldo factura: %d cargos, %d abonos vinculados",
            int(es_cargo.sum()), len(abonos_por_cargo),
        )
    else:
        df.loc[es_cargo, "SALDO_FACTURA"] = df.loc[es_cargo, "_MONTO"]
        logger.warning(
            "Sin DOCTO_CC_ACR_ID — saldo = monto total sin restar abonos."
        )

    df["SALDO_FACTURA"] = df["SALDO_FACTURA"].round(2)
    return df.drop(columns=["_MONTO"])


def _calcular_saldo_cliente(df: pd.DataFrame) -> pd.DataFrame:
    """Calcula el saldo acumulado (running balance) por cliente.

    Para cada fila: tipo ``C`` suma ``IMPORTE + IMPUESTO``, tipo ``R``
    resta. El resultado es una suma acumulativa dentro de cada grupo
    de cliente, redondeada a 2 decimales.

    Args:
        df: DataFrame con movimientos activos y saldos calculados.

    Returns:
        DataFrame con columna ``SALDO_CLIENTE`` agregada, ordenado
        por nombre de cliente y documento.
    """
    sort_cols = [
        c for c in
        ["NOMBRE_CLIENTE", "DOCTO_CC_ACR_ID", "DOCTO_CC_ID", "FECHA_EMISION"]
        if c in df.columns
    ]
    df = df.sort_values(
        sort_cols,
        ascending=[True] * len(sort_cols),
        na_position="first",
    ).reset_index(drop=True)

    monto: pd.Series = df["IMPORTE"] + df["IMPUESTO"]
    movimiento: pd.Series = pd.Series(
        np.where(
            df["TIPO_IMPTE"] == "C", monto,
            np.where(df["TIPO_IMPTE"] == "R", -monto, 0),
        ),
        index=df.index,
    )

    df["SALDO_CLIENTE"] = (
        movimiento.groupby(df["NOMBRE_CLIENTE"]).cumsum().round(2)
    )
    logger.info("Saldo acumulado por cliente calculado.")
    return df


# ======================================================================
# FUNCIONES INTERNAS — METRICAS DE CICLO
# ======================================================================


def _calcular_metricas_ciclo(df: pd.DataFrame) -> pd.DataFrame:
    """Calcula las metricas de ciclo de cobranza para cada cargo.

    Agrega cuatro columnas al DataFrame, con valor solo en filas de
    tipo ``C``; las filas de tipo ``R`` quedan como NULL o cadena vacia:

    **DELTA_RECAUDO** — Ciclo cerrado (facturas pagadas, SALDO == 0)::

        DELTA_RECAUDO = Fecha del ultimo abono menos FECHA_VENCIMIENTO

        Negativo = pago anticipado.
        Cero     = pago puntual en el dia del vencimiento.
        Positivo = dias de retraso real del cliente.

    **CATEGORIA_RECAUDO** — Clasificacion del comportamiento de pago::

        DELTA_RECAUDO <  0             -> Pago anticipado
        DELTA_RECAUDO == 0             -> Pago puntual
        1  <= DELTA_RECAUDO <= 15      -> Retraso leve (1-15)
        16 <= DELTA_RECAUDO <= 30      -> Retraso moderado (16-30)
        31 <= DELTA_RECAUDO <= 60      -> Retraso alto (31-60)
        DELTA_RECAUDO >  60            -> Retraso critico (>60)

    **DELTA_MORA** — Analisis de riesgo (facturas abiertas, SALDO > 0)::

        DELTA_MORA = HOY menos FECHA_VENCIMIENTO

        Negativo = factura aun vigente.
        Positivo = dias de mora acumulados.

    **CATEGORIA_MORA** — Clasificacion para accion inmediata::

        DELTA_MORA <  0              -> Por vencer
        1  <= DELTA_MORA <= 30       -> Mora temprana (1-30)
        31 <= DELTA_MORA <= 60       -> Mora media (31-60)
        61 <= DELTA_MORA <= 90       -> Mora alta (61-90)
        DELTA_MORA > 90              -> Mora critica (>90)

    Args:
        df: DataFrame con ``SALDO_FACTURA`` ya calculado.

    Returns:
        DataFrame con las cuatro columnas adicionales.
    """
    df = df.copy()
    hoy = pd.Timestamp.now().normalize()

    es_cargo: pd.Series = df["TIPO_IMPTE"] == "C"
    es_abono: pd.Series = df["TIPO_IMPTE"] == "R"

    df["DELTA_RECAUDO"] = np.nan
    df["CATEGORIA_RECAUDO"] = ""
    df["DELTA_MORA"] = np.nan
    df["CATEGORIA_MORA"] = ""

    # ------------------------------------------------------------------
    # DELTA_RECAUDO y CATEGORIA_RECAUDO — facturas completamente pagadas
    # ------------------------------------------------------------------
    pagadas = es_cargo & (df["SALDO_FACTURA"] == 0)

    if (
        pagadas.any()
        and "DOCTO_CC_ACR_ID" in df.columns
        and "DOCTO_CC_ID" in df.columns
        and "FECHA_EMISION" in df.columns
    ):
        ultimo_abono: pd.Series = (
            df.loc[es_abono & df["DOCTO_CC_ACR_ID"].notna()]
            .groupby("DOCTO_CC_ACR_ID")["FECHA_EMISION"]
            .max()
        )
        fecha_ultimo = df.loc[pagadas, "DOCTO_CC_ID"].map(ultimo_abono)
        recaudo_dias = (
            (fecha_ultimo.values - df.loc[pagadas, "FECHA_VENCIMIENTO"].values)
            / np.timedelta64(1, "D")
        )
        df.loc[pagadas, "DELTA_RECAUDO"] = recaudo_dias

        condiciones_recaudo = [
            recaudo_dias < 0,
            recaudo_dias == 0,
            (recaudo_dias >= 1)  & (recaudo_dias <= 15),
            (recaudo_dias >= 16) & (recaudo_dias <= 30),
            (recaudo_dias >= 31) & (recaudo_dias <= 60),
            recaudo_dias > 60,
        ]
        categorias_recaudo = [
            "Pago anticipado",
            "Pago puntual",
            "Retraso leve (1-15)",
            "Retraso moderado (16-30)",
            "Retraso alto (31-60)",
            "Retraso critico (>60)",
        ]
        df.loc[pagadas, "CATEGORIA_RECAUDO"] = np.select(
            condiciones_recaudo, categorias_recaudo, default="",
        )

        n_anticipados = int((recaudo_dias < 0).sum())
        n_puntuales   = int((recaudo_dias == 0).sum())
        n_retrasados  = int((recaudo_dias > 0).sum())
        logger.info(
            "Ciclo cerrado: %d pagadas — %d anticipadas, "
            "%d puntuales, %d con retraso",
            int(pagadas.sum()),
            n_anticipados, n_puntuales, n_retrasados,
        )

    # ------------------------------------------------------------------
    # DELTA_MORA y CATEGORIA_MORA — facturas con saldo pendiente
    # ------------------------------------------------------------------
    abiertas = es_cargo & (df["SALDO_FACTURA"] > 0)

    if abiertas.any() and "FECHA_VENCIMIENTO" in df.columns:
        mora_dias = (hoy - df.loc[abiertas, "FECHA_VENCIMIENTO"]).dt.days
        df.loc[abiertas, "DELTA_MORA"] = mora_dias

        condiciones_mora = [
            mora_dias <= 0,
            (mora_dias >= 1)  & (mora_dias <= 30),
            (mora_dias >= 31) & (mora_dias <= 60),
            (mora_dias >= 61) & (mora_dias <= 90),
            mora_dias > 90,
        ]
        categorias_mora = [
            "Por vencer",
            "Mora temprana (1-30)",
            "Mora media (31-60)",
            "Mora alta (61-90)",
            "Mora critica (>90)",
        ]
        df.loc[abiertas, "CATEGORIA_MORA"] = np.select(
            condiciones_mora, categorias_mora, default="",
        )

        n_abiertas = int(abiertas.sum())
        n_criticas = int((mora_dias > 90).sum())
        logger.info(
            "Facturas abiertas: %d — %d en mora critica (>90 dias)",
            n_abiertas, n_criticas,
        )

    return df


# ======================================================================
# FUNCIONES INTERNAS — VISTAS DE FACTURAS
# ======================================================================


def agregar_bandas_grupo(df: pd.DataFrame) -> pd.DataFrame:
    """Agrupa cargos con sus abonos y asigna un identificador de banda 0/1.

    Cada grupo (cargo mas sus abonos vinculados) recibe un valor de banda
    que alterna entre 0 y 1 respecto al grupo anterior. Se usa para
    aplicar colores alternados en Excel que faciliten la lectura visual.

    Args:
        df: DataFrame con columnas DOCTO_CC_ID y TIPO_IMPTE.

    Returns:
        DataFrame ordenado con columna ``_BAND_GROUP`` (0 o 1).
    """
    if "DOCTO_CC_ID" not in df.columns or "TIPO_IMPTE" not in df.columns:
        df = df.copy()
        df["_BAND_GROUP"] = 0
        return df

    df = df.copy()

    acr_id = df.get("DOCTO_CC_ACR_ID", df["DOCTO_CC_ID"])
    if isinstance(acr_id, pd.Series) and "DOCTO_CC_ID" in df.columns:
        acr_id = acr_id.fillna(df["DOCTO_CC_ID"])

    df["_GRUPO_CARGO"] = np.where(
        df["TIPO_IMPTE"] == "C",
        df["DOCTO_CC_ID"],
        acr_id,
    )

    sort_cols = [
        c for c in
        ["NOMBRE_CLIENTE", "_GRUPO_CARGO", "TIPO_IMPTE", "FECHA_EMISION"]
        if c in df.columns
    ]
    df = df.sort_values(
        sort_cols,
        ascending=[True] * len(sort_cols),
        na_position="first",
    ).reset_index(drop=True)

    cambio = df["_GRUPO_CARGO"] != df["_GRUPO_CARGO"].shift()
    df["_BAND_GROUP"] = cambio.cumsum() % 2

    return df.drop(columns=["_GRUPO_CARGO"])


def _extraer_facturas_abiertas(df: pd.DataFrame) -> pd.DataFrame:
    """Extrae las facturas con saldo pendiente y sus abonos parciales.

    Incluye:
        - Cargos con ``SALDO_FACTURA > 0`` (facturas sin cobrar del todo).
        - Abonos parciales vinculados via ``DOCTO_CC_ACR_ID`` a esos cargos.

    Cada grupo recibe una banda de color para identificacion visual.
    No incluye ``DELTA_RECAUDO`` ni ``CATEGORIA_RECAUDO`` porque esas
    metricas solo aplican a facturas completamente pagadas.

    Args:
        df: DataFrame completo del reporte con saldos y metricas calculados.

    Returns:
        DataFrame filtrado con ``_BAND_GROUP`` para formato Excel.
    """
    if "SALDO_FACTURA" not in df.columns or "TIPO_IMPTE" not in df.columns:
        logger.warning(
            "Columnas insuficientes para extraer facturas abiertas."
        )
        return pd.DataFrame()

    cargos_abiertos = df[
        (df["TIPO_IMPTE"] == "C") & (df["SALDO_FACTURA"] > 0)
    ]

    if cargos_abiertos.empty:
        logger.info("No hay facturas con saldo pendiente.")
        return pd.DataFrame()

    ids_abiertos: set[Any] = set()
    if "DOCTO_CC_ID" in cargos_abiertos.columns:
        ids_abiertos = set(cargos_abiertos["DOCTO_CC_ID"].dropna())

    abonos_parciales = pd.DataFrame()
    if ids_abiertos and "DOCTO_CC_ACR_ID" in df.columns:
        abonos_parciales = df[
            (df["TIPO_IMPTE"] == "R")
            & (df["DOCTO_CC_ACR_ID"].isin(ids_abiertos))
        ]

    resultado = pd.concat(
        [cargos_abiertos, abonos_parciales], ignore_index=True,
    )
    resultado = agregar_bandas_grupo(resultado)

    logger.info(
        "Facturas abiertas: %d cargos + %d abonos parciales, "
        "saldo total $%.2f",
        len(cargos_abiertos),
        len(abonos_parciales),
        float(cargos_abiertos["SALDO_FACTURA"].sum()),
    )
    return resultado


def _extraer_facturas_cerradas(df: pd.DataFrame) -> pd.DataFrame:
    """Extrae las facturas completamente cobradas y todos sus abonos.

    Incluye:
        - Cargos con ``SALDO_FACTURA == 0`` (facturas 100% cobradas).
        - Todos los abonos vinculados via ``DOCTO_CC_ACR_ID`` a esos cargos.

    Cada grupo recibe una banda de color para identificacion visual.
    Incluye ``DELTA_RECAUDO`` y ``CATEGORIA_RECAUDO``, que son las
    metricas relevantes para facturas ya cobradas.
    No incluye ``DELTA_MORA`` ni ``CATEGORIA_MORA`` porque esas metricas
    solo aplican a facturas con saldo pendiente.

    Args:
        df: DataFrame completo del reporte con saldos y metricas calculados.

    Returns:
        DataFrame filtrado con ``_BAND_GROUP`` para formato Excel.
    """
    if "SALDO_FACTURA" not in df.columns or "TIPO_IMPTE" not in df.columns:
        logger.warning(
            "Columnas insuficientes para extraer facturas cerradas."
        )
        return pd.DataFrame()

    cargos_cerrados = df[
        (df["TIPO_IMPTE"] == "C") & (df["SALDO_FACTURA"] == 0)
    ]

    if cargos_cerrados.empty:
        logger.info("No hay facturas completamente cobradas.")
        return pd.DataFrame()

    ids_cerrados: set[Any] = set()
    if "DOCTO_CC_ID" in cargos_cerrados.columns:
        ids_cerrados = set(cargos_cerrados["DOCTO_CC_ID"].dropna())

    abonos_completos = pd.DataFrame()
    if ids_cerrados and "DOCTO_CC_ACR_ID" in df.columns:
        abonos_completos = df[
            (df["TIPO_IMPTE"] == "R")
            & (df["DOCTO_CC_ACR_ID"].isin(ids_cerrados))
        ]

    resultado = pd.concat(
        [cargos_cerrados, abonos_completos], ignore_index=True,
    )
    resultado = agregar_bandas_grupo(resultado)

    n_anticipados = 0
    n_retrasados = 0
    if "DELTA_RECAUDO" in cargos_cerrados.columns:
        delta = cargos_cerrados["DELTA_RECAUDO"].dropna()
        n_anticipados = int((delta < 0).sum())
        n_retrasados  = int((delta > 0).sum())

    logger.info(
        "Facturas cerradas: %d cargos + %d abonos — "
        "%d anticipados, %d con retraso",
        len(cargos_cerrados),
        len(abonos_completos),
        n_anticipados,
        n_retrasados,
    )
    return resultado