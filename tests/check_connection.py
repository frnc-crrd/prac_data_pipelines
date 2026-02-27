"""Diagnóstico rápido de conexión a Firebird.

Ejecuta antes de correr el pipeline para verificar que todo está
configurado correctamente.  No requiere datos reales ni módulos src/.

Uso:
    python tests/check_connection.py
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def _ok(msg: str)   -> None: print(f"  ✅  {msg}")
def _fail(msg: str) -> None: print(f"  ❌  {msg}")
def _warn(msg: str) -> None: print(f"  ⚠️   {msg}")
def _info(msg: str) -> None: print(f"  ℹ️   {msg}")


def main() -> int:
    print("\n" + "═" * 55)
    print("  Diagnóstico de Conexión — Pipeline CxC Microsip")
    print("═" * 55)

    errores = 0

    # ── 1. Settings ────────────────────────────────────────────────────
    print("\n[1] Verificando configuración (settings.py)...")
    try:
        from config.settings import FIREBIRD_CONFIG, SQL_FILE, OUTPUT_DIR
        _ok(f"host:     {FIREBIRD_CONFIG.get('host', '?')}")
        _ok(f"port:     {FIREBIRD_CONFIG.get('port', 3050)}")
        _ok(f"database: {FIREBIRD_CONFIG.get('database', '?')}")
        _ok(f"user:     {FIREBIRD_CONFIG.get('user', '?')}")
        _ok(f"charset:  {FIREBIRD_CONFIG.get('charset', 'WIN1252')}")
    except Exception as e:
        _fail(f"Error importando settings: {e}")
        errores += 1
        return errores

    # ── 2. Archivo de base de datos ────────────────────────────────────
    print("\n[2] Verificando archivo .fdb en disco...")
    db_path = Path(str(FIREBIRD_CONFIG.get("database", "")))
    if db_path.exists():
        tamaño_mb = db_path.stat().st_size / (1024 * 1024)
        _ok(f"Archivo encontrado: {db_path.name} ({tamaño_mb:.1f} MB)")
    else:
        _fail(f"Archivo no encontrado: {db_path}")
        _info("Verifica la ruta en config/settings.py → FIREBIRD_CONFIG['database']")
        errores += 1

    # ── 3. Archivo SQL ─────────────────────────────────────────────────
    print("\n[3] Verificando archivo SQL maestro...")
    if SQL_FILE.exists():
        tamaño = SQL_FILE.stat().st_size
        _ok(f"SQL encontrado: {SQL_FILE.name} ({tamaño} bytes)")
        if tamaño < 50:
            _warn("El archivo SQL parece estar vacío o incompleto")
    else:
        _fail(f"SQL no encontrado: {SQL_FILE}")
        _info("Crea el archivo sql/maestro_cxc.sql con el query maestro de CxC")
        errores += 1

    # ── 4. Driver de Firebird ──────────────────────────────────────────
    print("\n[4] Verificando driver de Firebird...")
    driver_encontrado = False
    try:
        import fdb
        _ok(f"fdb instalado (Firebird 2.5) — versión: {fdb.__version__}")
        driver_encontrado = True
    except ImportError:
        _warn("fdb no instalado")

    if not driver_encontrado:
        try:
            import firebird.driver
            _ok("firebird-driver instalado (Firebird 3+/4+)")
            driver_encontrado = True
        except ImportError:
            _warn("firebird-driver no instalado")

    if not driver_encontrado:
        _fail("No se encontró ningún driver de Firebird")
        _info("Para Microsip (Firebird 2.5): pip install fdb")
        _info("Para Firebird 3+/4+:          pip install firebird-driver")
        errores += 1

    # ── 5. Dependencias Python ─────────────────────────────────────────
    print("\n[5] Verificando dependencias Python...")
    deps = {
        "pandas":   "pandas",
        "numpy":    "numpy",
        "openpyxl": "openpyxl",
        "streamlit": "streamlit",
        "plotly":   "plotly",
    }
    for nombre, modulo in deps.items():
        try:
            mod = __import__(modulo)
            version = getattr(mod, "__version__", "?")
            _ok(f"{nombre} — versión {version}")
        except ImportError:
            _warn(f"{nombre} no instalado — pip install {nombre}")

    # ── 6. Prueba de conexión real ─────────────────────────────────────
    if driver_encontrado and db_path.exists():
        print("\n[6] Probando conexión real a Firebird...")
        try:
            from src.db_connector import FirebirdConnector
            connector = FirebirdConnector(FIREBIRD_CONFIG)
            ok = connector.test_connection()
            if ok:
                _ok("Conexión exitosa 🎉")
            else:
                _fail("Conexión fallida — revisa credenciales y que el servidor esté activo")
                errores += 1
        except Exception as e:
            _fail(f"Error de conexión: {e}")
            _info("Verifica que el servicio de Firebird esté corriendo")
            _info("En Windows: services.msc → Firebird Server")
            errores += 1
    else:
        print("\n[6] Saltando prueba de conexión (driver o .fdb no disponible)")

    # ── Resumen ────────────────────────────────────────────────────────
    print("\n" + "─" * 55)
    if errores == 0:
        print("  ✅ Todo listo — puedes ejecutar: python main.py")
    else:
        print(f"  ❌ {errores} problema(s) encontrado(s) — revisa los puntos marcados arriba")
    print("─" * 55 + "\n")

    return errores


if __name__ == "__main__":
    sys.exit(main())