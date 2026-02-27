Qué hace cada script


config/settings.py

Es el "panel de control" del proyecto. Aquí defines una sola vez la ruta a tu base de datos Firebird, las credenciales, los rangos de antigüedad de cartera (0-30, 31-60, etc.), los umbrales para detectar anomalías y qué formatos de salida quieres (Excel, CSV). El resto de los scripts simplemente importan estas constantes, así que si algo cambia (nueva ruta de base de datos, nuevo umbral) solo tocas este archivo.


src/db_connector.py

Es el puente con Firebird. Se encarga de detectar automáticamente si tienes instalado fdb (para Firebird 2.5, que es lo que usa Microsip) o firebird-driver (para versiones más nuevas), abrir la conexión, ejecutar el SQL y devolverte un DataFrame de pandas listo para trabajar. También tiene un método test_connection() para verificar que puedes conectarte antes de correr todo el pipeline.


sql/maestro_cxc.sql

No es Python pero es la pieza más importante del sistema: es la consulta SQL que extrae todos los movimientos de CxC desde Firebird. Todo el pipeline depende de lo que este query devuelva. Si el query está bien construido (con DOCTO_CC_ACR_ID para vincular abonos a cargos, LIMITE_CREDITO, CANCELADO, etc.), todos los cálculos posteriores funcionan correctamente.


src/reporte_cxc.py

Es el corazón operativo. Toma el DataFrame crudo y produce tres salidas: el reporte principal (todos los movimientos activos con saldo por factura calculado, saldo acumulado por cliente y métricas de ciclo como días de mora y categoría), las facturas vivas (solo las facturas con saldo pendiente junto con sus abonos parciales, agrupadas visualmente) y los movimientos "por acreditar" (anticipos sin aplicar). También calcula DELTA_RECAUDO para facturas pagadas, que te dice si el cliente pagó antes o después del vencimiento.


src/analytics.py

Genera los reportes de análisis de cartera: antigüedad global (cuánto dinero está en cada bucket de tiempo), antigüedad por cliente en formato pivote, cartera vencida vs vigente con porcentajes, y resúmenes agregados por cliente, vendedor y concepto. Todo esto alimenta las pestañas analíticas del Excel.


src/auditor.py

Aplica reglas de negocio para encontrar problemas en los datos: duplicados por cliente+folio+concepto, importes estadísticamente atípicos usando Z-score, documentos sin cliente o vendedor asignado, documentos cancelados, y facturas vencidas más de 90 días. Devuelve un resumen consolidado y un DataFrame por cada tipo de hallazgo para que puedas revisar caso por caso.


src/kpis.py

Calcula los cinco indicadores estratégicos: DSO (cuántos días tarda la empresa en cobrar en promedio), CEI (qué porcentaje de lo cobrable se recuperó en el período), índice de morosidad (qué porcentaje de la cartera está vencida), análisis Pareto/ABC (qué clientes concentran el 80% del saldo), y utilización del límite de crédito por cliente con niveles de alerta.


main.py

Es el director de orquesta. Ejecuta los 6 pasos en orden (extracción → reporte → auditoría → análisis → KPIs → exportación), junta todos los DataFrames en un solo diccionario y los exporta a un Excel con múltiples pestañas formateadas. Desde la línea de comandos puedes saltarte pasos con flags como --skip-audit o solo probar la conexión con --test-connection.