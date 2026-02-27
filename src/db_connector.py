"""Conector a base de datos Firebird de Microsip."""

from __future__ import annotations

import logging
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Generator

import pandas as pd

logger = logging.getLogger(__name__)


class FirebirdConnector:
    def __init__(self, config: dict[str, str | int]) -> None:
        self.config = config
        self._driver: str = self._detect_driver()

    @staticmethod
    def _detect_driver() -> str:
        try:
            import firebird.driver  # noqa: F401
            return "firebird-driver"
        except ImportError:
            pass
        try:
            import fdb  # noqa: F401
            return "fdb"
        except ImportError:
            pass
        raise ImportError(
            "No se encontro driver de Firebird. Instala uno:\n"
            "  pip install firebird-driver   (Firebird 3+/4+)\n"
            "  pip install fdb               (Firebird 2.5)\n"
        )

    @contextmanager
    def connect(self) -> Generator[Any, None, None]:
        conn: Any = None
        try:
            if self._driver == "firebird-driver":
                from firebird.driver import connect as fb_connect
                host     = self.config["host"]
                port     = self.config.get("port", 3050)
                database = self.config["database"]
                # firebird-driver requires a DSN string: "host/port:database"
                dsn = f"{host}/{port}:{database}"
                conn = fb_connect(
                    dsn,
                    user=self.config["user"],
                    password=self.config["password"],
                    charset=self.config.get("charset", "WIN1252"),
                )
            else:
                import fdb
                conn = fdb.connect(
                    host=self.config["host"],
                    port=self.config.get("port", 3050),
                    database=self.config["database"],
                    user=self.config["user"],
                    password=self.config["password"],
                    charset=self.config.get("charset", "WIN1252"),
                )
            logger.info("Conexion a Firebird establecida: %s", self.config["database"])
            yield conn
        except Exception as e:
            logger.error("Error de conexion a Firebird: %s", e)
            raise
        finally:
            if conn is not None:
                conn.close()
                logger.info("Conexion a Firebird cerrada.")

    def execute_query(self, sql: str) -> pd.DataFrame:
        with self.connect() as conn:
            logger.info("Ejecutando query (%d caracteres)...", len(sql))
            cursor = conn.cursor()
            cursor.execute(sql)
            cols = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            cursor.close()
            df = pd.DataFrame(rows, columns=cols)
            logger.info("Query ejecutado — %d filas x %d columnas", *df.shape)
        return df

    def execute_sql_file(self, sql_path: str | Path) -> pd.DataFrame:
        sql_path = Path(sql_path)
        if not sql_path.exists():
            raise FileNotFoundError(f"Archivo SQL no encontrado: {sql_path}")
        sql = sql_path.read_text(encoding="utf-8")
        logger.info("Archivo SQL cargado: %s", sql_path.name)
        return self.execute_query(sql)

    def test_connection(self) -> bool:
        try:
            with self.connect() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1 FROM RDB$DATABASE")
                cursor.fetchone()
                cursor.close()
            logger.info("Prueba de conexion exitosa.")
            return True
        except Exception as e:
            logger.error("Prueba de conexion fallida: %s", e)
            return False