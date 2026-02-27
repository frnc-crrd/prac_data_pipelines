"""Modulo de analisis de cartera de Cuentas por Cobrar (CxC).

Genera ocho reportes a partir de las vistas ya procesadas por
``reporte_cxc.generar_reporte_cxc`` y ``main.preparar_registros_totales``.
Cada reporte usa la fuente mas apropiada segun su proposito:

    ``antiguedad_cartera``
        Distribucion de saldos pendientes por rango de dias de mora.
        Columnas: RANGO_ANTIGUEDAD, NUM_DOCUMENTOS, IMPORTE_TOTAL,
        PCT_DEL_TOTAL.
        Fuente: ``movimientos_abiertos_cxc``, solo cargos (TIPO_IMPTE C).

    ``antiguedad_por_cliente_mxn``
    ``antiguedad_por_cliente_usd``
        Pivot de saldos por cliente y rango. Incluye facturas pagadas,
        vigentes y vencidas por rango, mas totales de cargos, abonos y
        saldo pendiente. Orden: ESTATUS_CLIENTE asc, NOMBRE_CLIENTE asc.
        Fuente: ``movimientos_totales_cxc`` filtrado por moneda.

    ``cartera_vencida_vs_vigente``
        Comparativo Vigente vs Vencido.
        Columnas: ESTATUS_VENCIMIENTO, NUM_DOCUMENTOS, IMPORTE_TOTAL,
        SALDO_PENDIENTE, PCT_DEL_TOTAL.
        Fuente: ``movimientos_abiertos_cxc``, solo cargos.

    ``resumen_por_vendedor``
        Totales por vendedor y moneda. Los sin vendedor aparecen como
        "Sin vendedor asignado". Orden: MONEDA asc, VENDEDOR asc.
        Fuente: ``movimientos_totales_cxc``.

    ``resumen_por_concepto``
        Totales por concepto y moneda. Orden: MONEDA asc, CONCEPTO asc.
        Fuente: ``movimientos_totales_cxc``.

    ``resumen_ajustes``
        Resumen de registros por acreditar (anticipos sin aplicar).
        Orden: MONEDA asc, TIPO_REGISTRO asc.
        Fuente: ``registros_por_acreditar_cxc``.

    ``resumen_cancelados``
        Resumen de documentos cancelados.
        Orden: MONEDA asc, TIPO_REGISTRO asc.
        Fuente: ``registros_cancelados_cxc``.
"""

from __future__ import annotations

import logging
from typing import Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

_SIN_VENDEDOR: str = "SIN VENDEDOR ASIGNADO"
_SIN_CONCEPTO: str = "Sin concepto asignado"


class Analytics:
    """Motor de analisis de cartera CxC.

    Args:
        rangos_antiguedad: Lista de tuplas (min_dias, max_dias | None, etiqueta).
            Define los rangos de mora para los pivots de antiguedad.
            El rango "Por vencer" (dias <= 0) se agrega automaticamente.
    """

    def __init__(
        self,
        rangos_antiguedad: list[tuple[int, Optional[int], str]],
    ) -> None:
        self.rangos_antiguedad = rangos_antiguedad

    # ==================================================================
    # METODO PRINCIPAL
    # ==================================================================

    def run_analytics(
        self,
        vistas: dict[str, pd.DataFrame],
    ) -> dict[str, pd.DataFrame]:
        """Ejecuta todos los analisis y devuelve un dict de DataFrames.

        Args:
            vistas: Diccionario con las vistas procesadas del pipeline.
                Claves esperadas:
                    ``"movimientos_abiertos_cxc"``    Facturas con saldo pendiente.
                    ``"movimientos_totales_cxc"``     Todos los movimientos activos.
                    ``"registros_por_acreditar_cxc"`` Anticipos sin aplicar.
                    ``"registros_cancelados_cxc"``    Documentos cancelados.

        Returns:
            Diccionario con ocho DataFrames de analisis.
        """
        df_abiertos   = self._preparar(vistas.get("movimientos_abiertos_cxc",    pd.DataFrame()))
        df_totales    = self._preparar(vistas.get("movimientos_totales_cxc",     pd.DataFrame()))
        df_ajustes    = self._preparar(vistas.get("registros_por_acreditar_cxc", pd.DataFrame()))
        df_cancelados = self._preparar(vistas.get("registros_cancelados_cxc",    pd.DataFrame()))

        resultados: dict[str, pd.DataFrame] = {
            "antiguedad_cartera":         self._antiguedad_cartera(df_abiertos),
            "antiguedad_por_cliente_mxn": self._antiguedad_por_cliente(df_totales, "MXN"),
            "antiguedad_por_cliente_usd": self._antiguedad_por_cliente(df_totales, "USD"),
            "cartera_vencida_vs_vigente": self._cartera_vencida_vs_vigente(df_abiertos),
            "resumen_por_vendedor":       self._resumen_por_vendedor(df_totales),
            "resumen_por_concepto":       self._resumen_por_concepto(df_totales),
            "resumen_ajustes":            self._resumen_general(df_ajustes, "AJUSTE"),
            "resumen_cancelados":         self._resumen_general(df_cancelados, "CANCELADO"),
        }

        for nombre, df in resultados.items():
            logger.info("Analisis '%s': %d filas.", nombre, len(df))

        return resultados

    # ==================================================================
    # PREPARACION
    # ==================================================================

    def _preparar(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normaliza columnas, fechas, tipos numericos e indice.

        Elimina _BAND_GROUP si existe para evitar interferencias en
        agrupaciones. Reinicia el indice para garantizar conteos correctos
        cuando el DataFrame proviene de filtros o concatenaciones previas.

        Args:
            df: DataFrame de cualquier vista del pipeline.

        Returns:
            Copia normalizada con indice contiguo, o vacio si la entrada
            es vacia.
        """
        if df.empty:
            return df.copy()

        df = df.copy()
        df.columns = pd.Index([c.upper().strip() for c in df.columns])

        # Eliminar columna de bandas: no es un dato de negocio y puede
        # interferir en agrupaciones si el indice no es unico.
        if "_BAND_GROUP" in df.columns:
            df = df.drop(columns=["_BAND_GROUP"])

        # Reiniciar indice: los DataFrames filtrados pueden tener indices
        # no-contiguos que causan conteos incorrectos en operaciones
        # vectorizadas con np.where o pd.Series.values.
        df = df.reset_index(drop=True)

        for col in ["FECHA_EMISION", "FECHA_VENCIMIENTO"]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")

        for col in ["IMPORTE", "IMPUESTO", "CARGOS", "ABONOS",
                    "SALDO_FACTURA", "DELTA_MORA", "DELTA_RECAUDO"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        if "TIPO_IMPTE" in df.columns:
            df["TIPO_IMPTE"] = df["TIPO_IMPTE"].astype(str).str.strip().str.upper()
        if "MONEDA" in df.columns:
            df["MONEDA"] = df["MONEDA"].astype(str).str.strip().str.upper()

        return df

    def _monto(self, df: pd.DataFrame) -> pd.Series:
        """Calcula IMPORTE + IMPUESTO como serie numerica.

        Args:
            df: DataFrame con columnas IMPORTE e IMPUESTO.

        Returns:
            Serie con el monto total por fila.
        """
        imp = df["IMPORTE"]  if "IMPORTE"  in df.columns else pd.Series(0.0, index=df.index)
        tax = df["IMPUESTO"] if "IMPUESTO" in df.columns else pd.Series(0.0, index=df.index)
        return imp + tax

    # ==================================================================
    # HELPERS DE RANGOS
    # ==================================================================

    def _col_pivot_rango(self, min_d: int, max_d: Optional[int]) -> str:
        """Genera el nombre de columna pivot para un rango de mora.

        Args:
            min_d: Dias minimos del rango (inclusive).
            max_d: Dias maximos del rango (inclusive), o None si es abierto.

        Returns:
            Nombre de columna con formato ``FACTURAS_VENCIDAS (X-Y)``
            o ``FACTURAS_VENCIDAS (+X)`` para rangos abiertos.
        """
        if max_d is None:
            return f"FACTURAS_VENCIDAS (+{min_d - 1})"
        return f"FACTURAS_VENCIDAS ({min_d}-{max_d})"

    def _bucket_mora(self, df: pd.DataFrame) -> pd.Series:
        """Asigna cada cargo abierto a un rango de antiguedad segun DELTA_MORA.

        Usa las etiquetas originales de ``self.rangos_antiguedad`` (p.ej.
        "0-30 dias") como valores internos para la clasificacion. La
        conversion a nombres de columna pivot se realiza en
        ``_antiguedad_por_cliente``.

        Args:
            df: DataFrame de cargos con columna DELTA_MORA.

        Returns:
            Serie de strings con la etiqueta del rango para cada fila.
        """
        if "DELTA_MORA" not in df.columns:
            return pd.Series("Sin datos", index=df.index)

        mora = df["DELTA_MORA"]
        condiciones = [mora <= 0]
        etiquetas   = ["Por vencer"]

        for min_d, max_d, label in self.rangos_antiguedad:
            condiciones.append(
                mora > min_d - 1 if max_d is None
                else (mora >= min_d) & (mora <= max_d)
            )
            etiquetas.append(label)

        return pd.Series(
            np.select(condiciones, etiquetas, default="Sin clasificar"),
            index=df.index,
        )

    # ==================================================================
    # ANTIGUEDAD DE CARTERA
    # ==================================================================

    def _antiguedad_cartera(self, df_abiertos: pd.DataFrame) -> pd.DataFrame:
        """Distribucion de saldos pendientes por rango de dias de mora.

        Trabaja solo sobre cargos (TIPO_IMPTE == 'C') de facturas abiertas.
        Usa SALDO_FACTURA como importe base para reflejar lo realmente
        pendiente de cobro (no el importe original de la factura).

        Columnas de salida:
            RANGO_ANTIGUEDAD, NUM_DOCUMENTOS, IMPORTE_TOTAL, PCT_DEL_TOTAL.

        Args:
            df_abiertos: Vista ``movimientos_abiertos_cxc`` preparada.

        Returns:
            DataFrame ordenado por rango de mayor a menor mora.
        """
        if df_abiertos.empty:
            return pd.DataFrame()

        cargos = (
            df_abiertos[df_abiertos["TIPO_IMPTE"] == "C"].copy()
            if "TIPO_IMPTE" in df_abiertos.columns
            else df_abiertos.copy()
        )
        if cargos.empty:
            return pd.DataFrame()

        saldo = (
            cargos["SALDO_FACTURA"]
            if "SALDO_FACTURA" in cargos.columns
            else self._monto(cargos)
        )
        cargos["_RANGO"]   = self._bucket_mora(cargos)
        cargos["_SALDO"]   = saldo
        cargos["_MONEDA"]  = (
            cargos["MONEDA"] if "MONEDA" in cargos.columns
            else "Sin moneda"
        )

        orden_rangos = ["Por vencer"] + [r[2] for r in self.rangos_antiguedad]

        agrupado = (
            cargos.groupby(["_MONEDA", "_RANGO"])["_SALDO"]
            .agg(NUM_DOCUMENTOS="count", IMPORTE_TOTAL="sum")
            .reset_index()
            .rename(columns={"_MONEDA": "MONEDA", "_RANGO": "RANGO_ANTIGUEDAD"})
        )

        # PCT_DEL_TOTAL se calcula dentro de cada moneda con transform para
        # que sume 100% por grupo sin alterar el indice ni las columnas.
        total_por_moneda = agrupado.groupby("MONEDA")["IMPORTE_TOTAL"].transform("sum")
        agrupado["PCT_DEL_TOTAL"] = (
            (agrupado["IMPORTE_TOTAL"] / total_por_moneda * 100).round(2)
        ).where(total_por_moneda > 0, 0.0)
        agrupado["IMPORTE_TOTAL"] = agrupado["IMPORTE_TOTAL"].round(2)

        agrupado["RANGO_ANTIGUEDAD"] = pd.Categorical(
            agrupado["RANGO_ANTIGUEDAD"], categories=orden_rangos, ordered=True,
        )
        agrupado = (
            agrupado
            .sort_values(["MONEDA", "RANGO_ANTIGUEDAD"])
            .reset_index(drop=True)
        )
        agrupado["RANGO_ANTIGUEDAD"] = agrupado["RANGO_ANTIGUEDAD"].astype(str)

        # Renombrar etiquetas para coherencia visual con las columnas
        # pivot de antiguedad_por_cliente.
        def _label_a_rango(label: str) -> str:
            if label == "Por vencer":
                return "FACTURAS_VIGENTES"
            # Buscar el rango correspondiente por etiqueta
            for min_d, max_d, lbl in self.rangos_antiguedad:
                if lbl == label:
                    return self._col_pivot_rango(min_d, max_d)
            return label

        agrupado["RANGO_ANTIGUEDAD"] = agrupado["RANGO_ANTIGUEDAD"].map(_label_a_rango)

        return agrupado[["MONEDA", "RANGO_ANTIGUEDAD", "NUM_DOCUMENTOS",
                          "IMPORTE_TOTAL", "PCT_DEL_TOTAL"]]

    # ==================================================================
    # ANTIGUEDAD POR CLIENTE
    # ==================================================================

    def _antiguedad_por_cliente(
        self,
        df_totales: pd.DataFrame,
        moneda: str,
    ) -> pd.DataFrame:
        """Pivot de saldos por cliente, rango de antiguedad y moneda.

        Columnas de salida (en orden):
            NOMBRE_CLIENTE, ESTATUS_CLIENTE, NUM_DOCUMENTOS,
            FACTURAS_PAGADAS, FACTURAS_VIGENTES,
            FACTURAS_VENCIDAS (0-30), ..., FACTURAS_VENCIDAS (+120),
            TOTAL_CARGOS, TOTAL_ABONOS, SALDO_PENDIENTE.

        ``FACTURAS_PAGADAS`` — monto total (IMPORTE+IMPUESTO) de cargos
        completamente cobrados (SALDO_FACTURA == 0).

        ``FACTURAS_VIGENTES`` — saldo pendiente de cargos abiertos cuyo
        vencimiento aun no ha llegado (DELTA_MORA <= 0).

        ``FACTURAS_VENCIDAS (X-Y)`` — saldo pendiente de cargos abiertos
        en ese rango de dias de mora.

        Orden de filas: ESTATUS_CLIENTE asc, NOMBRE_CLIENTE asc.

        Args:
            df_totales: Vista ``movimientos_totales_cxc`` preparada.
            moneda: Codigo de moneda a filtrar ("MXN" o "USD").

        Returns:
            DataFrame pivot, o vacio si no hay datos para esa moneda.
        """
        if df_totales.empty:
            return pd.DataFrame()

        df = df_totales.copy()
        if "MONEDA" in df.columns:
            df = df[df["MONEDA"] == moneda].reset_index(drop=True)

        if df.empty:
            logger.info("Sin datos para moneda %s en antiguedad_por_cliente.", moneda)
            return pd.DataFrame()

        tiene_tipo = "TIPO_IMPTE" in df.columns
        es_cargo   = df["TIPO_IMPTE"] == "C" if tiene_tipo else pd.Series(True,  index=df.index)
        es_abono   = df["TIPO_IMPTE"] == "R" if tiene_tipo else pd.Series(False, index=df.index)

        col_cliente = "NOMBRE_CLIENTE"  if "NOMBRE_CLIENTE"  in df.columns else None
        col_estatus = "ESTATUS_CLIENTE" if "ESTATUS_CLIENTE" in df.columns else None

        if col_cliente is None:
            logger.warning("Sin columna NOMBRE_CLIENTE para antiguedad_por_cliente.")
            return pd.DataFrame()

        monto = self._monto(df)

        df_cargos = df[es_cargo].copy()
        df_abonos = df[es_abono].copy()

        # -- Conteos y totales globales por cliente --------------------
        num_docs = (
            df_cargos.groupby(col_cliente).size().rename("NUM_DOCUMENTOS")
        )
        total_cargos = (
            monto[es_cargo]
            .groupby(df.loc[es_cargo, col_cliente].values)
            .sum()
            .rename("TOTAL_CARGOS")
        )
        total_abonos = (
            monto[es_abono]
            .groupby(df.loc[es_abono, col_cliente].values)
            .sum()
            .rename("TOTAL_ABONOS")
        )

        # -- Facturas pagadas (saldo == 0) ----------------------------
        saldo_col = "SALDO_FACTURA" if "SALDO_FACTURA" in df_cargos.columns else None
        pagadas_monto: dict[str, float] = {}
        pivot_rows:   dict[str, dict[str, float]] = {}

        if saldo_col is not None:
            pagadas = df_cargos[df_cargos[saldo_col] == 0].copy()
            if not pagadas.empty:
                monto_pagadas = self._monto(pagadas)
                for cliente, grupo_idx in pagadas.groupby(col_cliente).groups.items():
                    pagadas_monto[cliente] = round(
                        float(monto_pagadas.loc[grupo_idx].sum()), 2
                    )

            # -- Pivot de saldos abiertos por rango -------------------
            abiertos = df_cargos[df_cargos[saldo_col] > 0].copy()
            if not abiertos.empty:
                abiertos["_RANGO"] = self._bucket_mora(abiertos)
                abiertos["_SALDO"] = abiertos[saldo_col]

                for cliente, grupo in abiertos.groupby(col_cliente):
                    pivot_rows[cliente] = {}
                    # Vigentes (por vencer)
                    pivot_rows[cliente]["FACTURAS_VIGENTES"] = round(
                        float(grupo.loc[grupo["_RANGO"] == "Por vencer", "_SALDO"].sum()), 2,
                    )
                    # Vencidas por rango
                    for min_d, max_d, label in self.rangos_antiguedad:
                        col_dest = self._col_pivot_rango(min_d, max_d)
                        pivot_rows[cliente][col_dest] = round(
                            float(grupo.loc[grupo["_RANGO"] == label, "_SALDO"].sum()), 2,
                        )

        # -- Estatus del cliente --------------------------------------
        estatus_map: dict[str, str] = {}
        if col_estatus:
            estatus_map = (
                df_cargos.dropna(subset=[col_cliente, col_estatus])
                .groupby(col_cliente)[col_estatus]
                .first()
                .to_dict()
            )

        # -- Columnas pivot en orden ----------------------------------
        cols_pivot_vencidas = [
            self._col_pivot_rango(min_d, max_d)
            for min_d, max_d, _ in self.rangos_antiguedad
        ]
        cols_pivot = ["FACTURAS_VIGENTES"] + cols_pivot_vencidas

        # -- Ensamblar resultado --------------------------------------
        todos_clientes = sorted(
            set(list(num_docs.index))
            | set(list(total_cargos.index))
            | set(list(total_abonos.index))
        )

        filas: list[dict] = []
        for cliente in todos_clientes:
            tc = round(float(total_cargos.get(cliente, 0)), 2)
            ta = round(float(total_abonos.get(cliente, 0)), 2)
            fila: dict = {
                "NOMBRE_CLIENTE":  cliente,
                "ESTATUS_CLIENTE": estatus_map.get(cliente, ""),
                "NUM_DOCUMENTOS":  int(num_docs.get(cliente, 0)),
                "FACTURAS_PAGADAS": pagadas_monto.get(cliente, 0.0),
            }
            for col in cols_pivot:
                fila[col] = pivot_rows.get(cliente, {}).get(col, 0.0)
            fila["TOTAL_CARGOS"]    = tc
            fila["TOTAL_ABONOS"]    = ta
            fila["SALDO_PENDIENTE"] = round(tc - ta, 2)
            filas.append(fila)

        resultado = pd.DataFrame(filas)
        sort_cols = [c for c in ["ESTATUS_CLIENTE", "NOMBRE_CLIENTE"] if c in resultado.columns]
        if sort_cols:
            resultado = resultado.sort_values(sort_cols).reset_index(drop=True)

        # Agregar MONEDA como primera columna para identificacion rapida
        # al tener MXN y USD en pestanas separadas.
        resultado.insert(0, "MONEDA", moneda)

        return resultado

    # ==================================================================
    # CARTERA VENCIDA VS VIGENTE
    # ==================================================================

    def _cartera_vencida_vs_vigente(self, df_abiertos: pd.DataFrame) -> pd.DataFrame:
        """Comparativo de cargos vencidos vs vigentes en la cartera abierta.

        Vigentes: DELTA_MORA <= 0 (dentro del plazo pactado).
        Vencidos: DELTA_MORA >  0 (plazo de pago superado).

        Solo trabaja sobre cargos con saldo pendiente porque representan
        el riesgo real de no cobro.

        Columnas de salida:
            ESTATUS_VENCIMIENTO, NUM_DOCUMENTOS, IMPORTE_TOTAL,
            SALDO_PENDIENTE, PCT_DEL_TOTAL.

        Args:
            df_abiertos: Vista ``movimientos_abiertos_cxc`` preparada.

        Returns:
            DataFrame con dos filas: Vigente y Vencido.
        """
        if df_abiertos.empty:
            return pd.DataFrame()

        cargos = (
            df_abiertos[df_abiertos["TIPO_IMPTE"] == "C"].copy()
            if "TIPO_IMPTE" in df_abiertos.columns
            else df_abiertos.copy()
        )
        if cargos.empty or "DELTA_MORA" not in cargos.columns:
            return pd.DataFrame()

        if "MONEDA" not in cargos.columns:
            cargos["MONEDA"] = "Sin moneda"

        saldo = (
            cargos["SALDO_FACTURA"]
            if "SALDO_FACTURA" in cargos.columns
            else self._monto(cargos)
        )
        cargos["_ESTATUS"] = np.where(cargos["DELTA_MORA"] <= 0, "FACTURAS_VIGENTES", "FACTURAS_VENCIDAS")
        cargos["_SALDO"]   = saldo
        cargos["_MONTO"]   = self._monto(cargos)

        agrupado = (
            cargos.groupby(["MONEDA", "_ESTATUS"])
            .agg(
                NUM_DOCUMENTOS=("_SALDO", "count"),
                IMPORTE_TOTAL=("_MONTO",  "sum"),
                SALDO_PENDIENTE=("_SALDO", "sum"),
            )
            .reset_index()
            .rename(columns={"_ESTATUS": "ESTATUS_VENCIMIENTO"})
        )

        # PCT_DEL_TOTAL calculado dentro de cada moneda.
        total_por_moneda = agrupado.groupby("MONEDA")["SALDO_PENDIENTE"].transform("sum")
        agrupado["PCT_DEL_TOTAL"] = (
            (agrupado["SALDO_PENDIENTE"] / total_por_moneda * 100).round(2)
        ).where(total_por_moneda > 0, 0.0)

        for col in ["IMPORTE_TOTAL", "SALDO_PENDIENTE"]:
            agrupado[col] = agrupado[col].round(2)

        orden_estatus = ["FACTURAS_VIGENTES", "FACTURAS_VENCIDAS"]
        agrupado["ESTATUS_VENCIMIENTO"] = pd.Categorical(
            agrupado["ESTATUS_VENCIMIENTO"], categories=orden_estatus, ordered=True,
        )
        resultado = (
            agrupado
            .sort_values(["MONEDA", "ESTATUS_VENCIMIENTO"])
            .reset_index(drop=True)
        )
        resultado["ESTATUS_VENCIMIENTO"] = resultado["ESTATUS_VENCIMIENTO"].astype(str)
        return resultado[["MONEDA", "ESTATUS_VENCIMIENTO", "NUM_DOCUMENTOS",
                           "IMPORTE_TOTAL", "SALDO_PENDIENTE", "PCT_DEL_TOTAL"]]

    # ==================================================================
    # RESUMEN POR VENDEDOR
    # ==================================================================

    def _resumen_por_vendedor(self, df_totales: pd.DataFrame) -> pd.DataFrame:
        """Totales de cargos, abonos y saldo por vendedor y moneda.

        Los registros sin vendedor asignado se agrupan como
        "Sin vendedor asignado". Orden: MONEDA asc, VENDEDOR asc.

        Columnas de salida:
            VENDEDOR, MONEDA, NUM_CARGOS, NUM_ABONOS,
            TOTAL_CARGOS, TOTAL_ABONOS, SALDO_PENDIENTE.

        Args:
            df_totales: Vista ``movimientos_totales_cxc`` preparada.

        Returns:
            DataFrame ordenado por MONEDA asc, VENDEDOR asc.
        """
        if df_totales.empty or "TIPO_IMPTE" not in df_totales.columns:
            return pd.DataFrame()

        df = df_totales.copy()
        if "VENDEDOR" in df.columns:
            df["VENDEDOR"] = (
                df["VENDEDOR"].fillna(_SIN_VENDEDOR).replace("", _SIN_VENDEDOR)
            )
        else:
            df["VENDEDOR"] = _SIN_VENDEDOR

        if "MONEDA" not in df.columns:
            df["MONEDA"] = "Sin moneda"

        monto      = self._monto(df)
        es_cargo   = df["TIPO_IMPTE"] == "C"
        es_abono   = df["TIPO_IMPTE"] == "R"
        group_keys = ["VENDEDOR", "MONEDA"]

        cargos_agg = (
            df[es_cargo]
            .assign(_MONTO=monto[es_cargo])
            .groupby(group_keys)
            .agg(NUM_CARGOS=("_MONTO", "count"), TOTAL_CARGOS=("_MONTO", "sum"))
        )
        abonos_agg = (
            df[es_abono]
            .assign(_MONTO=monto[es_abono])
            .groupby(group_keys)
            .agg(NUM_ABONOS=("_MONTO", "count"), TOTAL_ABONOS=("_MONTO", "sum"))
        )

        resultado = (
            cargos_agg.join(abonos_agg, how="outer").fillna(0).reset_index()
        )
        resultado["SALDO_PENDIENTE"] = (
            resultado["TOTAL_CARGOS"] - resultado["TOTAL_ABONOS"]
        ).round(2)
        for col in ["TOTAL_CARGOS", "TOTAL_ABONOS"]:
            resultado[col] = resultado[col].round(2)
        for col in ["NUM_CARGOS", "NUM_ABONOS"]:
            resultado[col] = resultado[col].astype(int)

        return resultado.sort_values(
            ["MONEDA", "VENDEDOR"]
        ).reset_index(drop=True)[["MONEDA", "VENDEDOR", "NUM_CARGOS",
                                   "NUM_ABONOS", "TOTAL_CARGOS",
                                   "TOTAL_ABONOS", "SALDO_PENDIENTE"]]

    # ==================================================================
    # RESUMEN POR CONCEPTO
    # ==================================================================

    def _resumen_por_concepto(self, df_totales: pd.DataFrame) -> pd.DataFrame:
        """Totales por tipo de concepto sobre movimientos activos.

        ``movimientos_totales_cxc`` ya excluye ajustes y cancelados,
        por lo que este reporte refleja solo la actividad regular.
        Orden: MONEDA asc, CONCEPTO asc.

        Columnas de salida:
            CONCEPTO, MONEDA, NUM_CARGOS, NUM_ABONOS,
            TOTAL_CARGOS, TOTAL_ABONOS, SALDO_PENDIENTE.

        Args:
            df_totales: Vista ``movimientos_totales_cxc`` preparada.

        Returns:
            DataFrame ordenado por MONEDA asc, CONCEPTO asc.
        """
        if df_totales.empty or "TIPO_IMPTE" not in df_totales.columns:
            return pd.DataFrame()

        df = df_totales.copy()
        if "CONCEPTO" in df.columns:
            df["CONCEPTO"] = (
                df["CONCEPTO"].fillna(_SIN_CONCEPTO).replace("", _SIN_CONCEPTO)
            )
        else:
            df["CONCEPTO"] = _SIN_CONCEPTO

        if "MONEDA" not in df.columns:
            df["MONEDA"] = "Sin moneda"

        monto      = self._monto(df)
        es_cargo   = df["TIPO_IMPTE"] == "C"
        es_abono   = df["TIPO_IMPTE"] == "R"
        group_keys = ["CONCEPTO", "MONEDA"]

        cargos_agg = (
            df[es_cargo]
            .assign(_MONTO=monto[es_cargo])
            .groupby(group_keys)
            .agg(NUM_CARGOS=("_MONTO", "count"), TOTAL_CARGOS=("_MONTO", "sum"))
        )
        abonos_agg = (
            df[es_abono]
            .assign(_MONTO=monto[es_abono])
            .groupby(group_keys)
            .agg(NUM_ABONOS=("_MONTO", "count"), TOTAL_ABONOS=("_MONTO", "sum"))
        )

        resultado = (
            cargos_agg.join(abonos_agg, how="outer").fillna(0).reset_index()
        )
        resultado["SALDO_PENDIENTE"] = (
            resultado["TOTAL_CARGOS"] - resultado["TOTAL_ABONOS"]
        ).round(2)
        for col in ["TOTAL_CARGOS", "TOTAL_ABONOS"]:
            resultado[col] = resultado[col].round(2)
        for col in ["NUM_CARGOS", "NUM_ABONOS"]:
            resultado[col] = resultado[col].astype(int)

        return resultado.sort_values(
            ["MONEDA", "CONCEPTO"]
        ).reset_index(drop=True)[["MONEDA", "CONCEPTO", "NUM_CARGOS",
                                   "NUM_ABONOS", "TOTAL_CARGOS", "TOTAL_ABONOS"]]

    # ==================================================================
    # RESUMEN GENERAL (AJUSTES Y CANCELADOS)
    # ==================================================================

    def _resumen_general(
        self,
        df: pd.DataFrame,
        tipo_registro: str,
    ) -> pd.DataFrame:
        """Resumen agregado para registros especiales (ajustes o cancelados).

        Produce una tabla por CONCEPTO y MONEDA con conteos y montos.
        Orden: MONEDA asc, TIPO_REGISTRO asc.

        Columnas de salida:
            TIPO_REGISTRO, CONCEPTO, MONEDA,
            NUM_REGISTROS, IMPORTE_TOTAL, IMPUESTO_TOTAL, MONTO_TOTAL.

        Args:
            df: Vista de registros especiales ya preparada.
            tipo_registro: Etiqueta descriptiva ("AJUSTE" o "CANCELADO").

        Returns:
            DataFrame ordenado por MONEDA asc, TIPO_REGISTRO asc,
            o vacio si no hay datos.
        """
        if df.empty:
            return pd.DataFrame()

        df = df.copy()
        if "CONCEPTO" in df.columns:
            df["CONCEPTO"] = (
                df["CONCEPTO"].fillna(_SIN_CONCEPTO).replace("", _SIN_CONCEPTO)
            )
        else:
            df["CONCEPTO"] = _SIN_CONCEPTO

        if "MONEDA" not in df.columns:
            df["MONEDA"] = "Sin moneda"

        imp = pd.to_numeric(df.get("IMPORTE",  pd.Series(0, index=df.index)), errors="coerce").fillna(0)
        tax = pd.to_numeric(df.get("IMPUESTO", pd.Series(0, index=df.index)), errors="coerce").fillna(0)
        df["_IMP"]   = imp
        df["_TAX"]   = tax
        df["_MONTO"] = imp + tax

        agrupado = (
            df.groupby(["CONCEPTO", "MONEDA"])
            .agg(
                NUM_REGISTROS=("_MONTO",  "count"),
                IMPORTE_TOTAL=("_IMP",    "sum"),
                IMPUESTO_TOTAL=("_TAX",   "sum"),
                MONTO_TOTAL=("_MONTO",   "sum"),
            )
            .reset_index()
        )
        for col in ["IMPORTE_TOTAL", "IMPUESTO_TOTAL", "MONTO_TOTAL"]:
            agrupado[col] = agrupado[col].round(2)

        agrupado.insert(0, "TIPO_REGISTRO", tipo_registro)

        return agrupado.sort_values(
            ["MONEDA", "TIPO_REGISTRO"]
        ).reset_index(drop=True)[["MONEDA", "TIPO_REGISTRO", "CONCEPTO",
                                   "NUM_REGISTROS", "IMPORTE_TOTAL",
                                   "IMPUESTO_TOTAL", "MONTO_TOTAL"]]