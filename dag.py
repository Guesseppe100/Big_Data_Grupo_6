from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.email import EmailOperator
import os

BASE_PATH = "/opt/airflow"
DB_PATH = os.path.join(BASE_PATH, "dw_v3.duckdb")
STAGING_DIR = os.path.join(BASE_PATH, "staging")

default_args = {
    "owner": "data_engineer",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


@dag(
    dag_id="elt_duckdb_pipeline_V7",
    default_args=default_args,
    schedule="*/5 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["duckdb", "elt", "incremental"],
)
def elt_pipeline():
    @task()
    def preparar_control_y_staging():
        import duckdb
        import logging

        conn = duckdb.connect(DB_PATH)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS processed_files (
                file_name VARCHAR,
                file_path VARCHAR,
                file_size_bytes BIGINT,
                file_modified_at TIMESTAMP,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        conn.execute("DROP TABLE IF EXISTS staging_raw")

        logging.info("Tabla processed_files verificada")
        logging.info("Tabla staging_raw reiniciada para la ejecución actual")

        conn.close()

    @task()
    def detectar_archivos_nuevos():
        import duckdb
        import glob
        import os
        import logging
        from datetime import datetime

        conn = duckdb.connect(DB_PATH)
        csv_files = sorted(glob.glob(os.path.join(STAGING_DIR, "*.csv")))

        if not csv_files:
            logging.info("No se encontraron archivos CSV en staging")
            conn.close()
            return {
                "files_found": 0,
                "files_to_process": 0,
                "processed_files_info": [],
                "processed_file_names": [],
            }

        files_to_process = []

        for file_path in csv_files:
            stat = os.stat(file_path)
            file_name = os.path.basename(file_path)
            file_size_bytes = stat.st_size
            file_modified_at = datetime.fromtimestamp(stat.st_mtime)

            exists = conn.execute(
                """
                SELECT 1
                FROM processed_files
                WHERE file_name = ?
                  AND file_size_bytes = ?
                  AND file_modified_at = ?
                LIMIT 1
                """,
                [file_name, file_size_bytes, file_modified_at],
            ).fetchone()

            if exists is None:
                files_to_process.append(
                    {
                        "file_name": file_name,
                        "file_path": file_path,
                        "file_size_bytes": file_size_bytes,
                        "file_modified_at": file_modified_at,
                    }
                )

        conn.close()

        logging.info(f"Archivos encontrados: {len(csv_files)}")
        logging.info(f"Archivos nuevos/modificados: {len(files_to_process)}")
        logging.info("Archivos a procesar: %s", [f["file_name"] for f in files_to_process])

        return {
            "files_found": len(csv_files),
            "files_to_process": len(files_to_process),
            "processed_files_info": files_to_process,
            "processed_file_names": [f["file_name"] for f in files_to_process],
        }

    @task.short_circuit()
    def hay_archivos_nuevos(detection_info: dict):
        return detection_info.get("files_to_process", 0) > 0

    @task()
    def cargar_staging_incremental(detection_info: dict):
        import duckdb
        import logging

        conn = duckdb.connect(DB_PATH)
        files_to_process = detection_info.get("processed_files_info", [])

        if not files_to_process:
            conn.execute("""
                CREATE TABLE staging_raw AS
                SELECT * FROM (SELECT 1 AS dummy) WHERE 1=0
            """)
            conn.close()
            return {
                "files_processed": [],
                "records_loaded": 0,
            }

        select_statements = []

        for file_info in files_to_process:
            safe_path = file_info["file_path"].replace("'", "''")
            safe_name = file_info["file_name"].replace("'", "''")

            select_statements.append(
                f"""
                SELECT
                    *,
                    '{safe_name}' AS source_file
                FROM read_csv_auto('{safe_path}')
                """
            )

        union_sql = "\nUNION ALL\n".join(select_statements)
        conn.execute(f"CREATE TABLE staging_raw AS {union_sql}")

        rows_loaded = conn.execute("SELECT COUNT(*) FROM staging_raw").fetchone()[0]

        for file_info in files_to_process:
            conn.execute(
                """
                INSERT INTO processed_files (
                    file_name,
                    file_path,
                    file_size_bytes,
                    file_modified_at,
                    processed_at
                )
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                [
                    file_info["file_name"],
                    file_info["file_path"],
                    file_info["file_size_bytes"],
                    file_info["file_modified_at"],
                ],
            )

        logging.info("Archivos procesados: %s", [f["file_name"] for f in files_to_process])
        logging.info("Registros cargados en staging_raw: %s", rows_loaded)

        conn.close()

        return {
            "files_processed": [f["file_name"] for f in files_to_process],
            "records_loaded": rows_loaded,
        }

    @task()
    def transformar_datos(staging_info: dict):
        import duckdb
        import logging

        conn = duckdb.connect(DB_PATH)
        total_staging = conn.execute("SELECT COUNT(*) FROM staging_raw").fetchone()[0]

        conn.execute("""
            CREATE TABLE IF NOT EXISTS fact_finanzas_elt (
                id BIGINT,
                salario DOUBLE,
                gastos DOUBLE,
                fecha DATE,
                correo_hash VARCHAR,
                utilidad DOUBLE,
                source_file VARCHAR,
                fecha_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                fecha_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        conn.execute("DROP TABLE IF EXISTS tmp_fact_finanzas_elt")

        conn.execute("""
            CREATE TEMP TABLE tmp_fact_finanzas_elt AS
            WITH base AS (
                SELECT
                    id,
                    salario,
                    COALESCE(gastos, 0) AS gastos,
                    CASE
                        WHEN regexp_matches(CAST(fecha AS VARCHAR), '^[0-9]{4}-[0-9]{2}-[0-9]{2}$')
                            THEN CAST(CAST(fecha AS VARCHAR) AS DATE)
                        WHEN regexp_matches(CAST(fecha AS VARCHAR), '^[0-9]{2}/[0-9]{2}/[0-9]{4}$')
                            THEN CAST(STRPTIME(CAST(fecha AS VARCHAR), '%d/%m/%Y') AS DATE)
                        WHEN regexp_matches(CAST(fecha AS VARCHAR), '^[0-9]{2}-[0-9]{2}-[0-9]{4}$')
                            THEN CAST(STRPTIME(CAST(fecha AS VARCHAR), '%m-%d-%Y') AS DATE)
                        ELSE NULL
                    END AS fecha,
                    CASE
                        WHEN correo IS NOT NULL THEN sha256(CAST(correo AS VARCHAR))
                        ELSE NULL
                    END AS correo_hash,
                    source_file,
                    CURRENT_TIMESTAMP AS fecha_proceso
                FROM staging_raw
            ),
            filtrado AS (
                SELECT
                    id,
                    salario,
                    gastos,
                    fecha,
                    correo_hash,
                    salario - gastos AS utilidad,
                    source_file,
                    fecha_proceso AS fecha_carga,
                    fecha_proceso AS fecha_actualizacion
                FROM base
                WHERE
                    id IS NOT NULL
                    AND fecha IS NOT NULL
                    AND salario > 0
                    AND gastos >= 0
            ),
            deduplicado AS (
                SELECT *
                FROM (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (
                            PARTITION BY id, fecha
                            ORDER BY fecha_actualizacion DESC, source_file DESC
                        ) AS rn
                    FROM filtrado
                )
                WHERE rn = 1
            )
            SELECT
                id,
                salario,
                gastos,
                fecha,
                correo_hash,
                utilidad,
                source_file,
                fecha_carga,
                fecha_actualizacion
            FROM deduplicado
        """)

        valid_rows = conn.execute("SELECT COUNT(*) FROM tmp_fact_finanzas_elt").fetchone()[0]
        filtered_rows = total_staging - valid_rows

        inserted_rows = conn.execute("""
            SELECT COUNT(*)
            FROM tmp_fact_finanzas_elt s
            LEFT JOIN fact_finanzas_elt t
              ON t.id = s.id
             AND t.fecha = s.fecha
            WHERE t.id IS NULL
        """).fetchone()[0]

        updated_rows = conn.execute("""
            SELECT COUNT(*)
            FROM tmp_fact_finanzas_elt s
            JOIN fact_finanzas_elt t
              ON t.id = s.id
             AND t.fecha = s.fecha
            WHERE
                COALESCE(t.salario, -1) <> COALESCE(s.salario, -1)
                OR COALESCE(t.gastos, -1) <> COALESCE(s.gastos, -1)
                OR COALESCE(t.correo_hash, '') <> COALESCE(s.correo_hash, '')
                OR COALESCE(t.utilidad, -1) <> COALESCE(s.utilidad, -1)
                OR COALESCE(t.source_file, '') <> COALESCE(s.source_file, '')
        """).fetchone()[0]

        conn.execute("""
            UPDATE fact_finanzas_elt AS target
            SET
                salario = source.salario,
                gastos = source.gastos,
                correo_hash = source.correo_hash,
                utilidad = source.utilidad,
                source_file = source.source_file,
                fecha_actualizacion = CURRENT_TIMESTAMP
            FROM tmp_fact_finanzas_elt AS source
            WHERE target.id = source.id
              AND target.fecha = source.fecha
              AND (
                    COALESCE(target.salario, -1) <> COALESCE(source.salario, -1)
                    OR COALESCE(target.gastos, -1) <> COALESCE(source.gastos, -1)
                    OR COALESCE(target.correo_hash, '') <> COALESCE(source.correo_hash, '')
                    OR COALESCE(target.utilidad, -1) <> COALESCE(source.utilidad, -1)
                    OR COALESCE(target.source_file, '') <> COALESCE(source.source_file, '')
              )
        """)

        conn.execute("""
            INSERT INTO fact_finanzas_elt (
                id, salario, gastos, fecha, correo_hash, utilidad,
                source_file, fecha_carga, fecha_actualizacion
            )
            SELECT
                s.id, s.salario, s.gastos, s.fecha, s.correo_hash, s.utilidad,
                s.source_file, s.fecha_carga, s.fecha_actualizacion
            FROM tmp_fact_finanzas_elt s
            LEFT JOIN fact_finanzas_elt t
              ON t.id = s.id
             AND t.fecha = s.fecha
            WHERE t.id IS NULL
        """)

        total_final = conn.execute("SELECT COUNT(*) FROM fact_finanzas_elt").fetchone()[0]
        conn.close()

        logging.info(f"Registros leídos en staging: {total_staging}")
        logging.info(f"Registros válidos procesados: {valid_rows}")
        logging.info(f"Registros filtrados: {filtered_rows}")
        logging.info(f"Registros insertados: {inserted_rows}")
        logging.info(f"Registros actualizados: {updated_rows}")
        logging.info(f"Total histórico en fact_finanzas_elt: {total_final}")

        return {
            "files_processed": staging_info.get("files_processed", []),
            "records_processed": valid_rows,
            "records_filtered": filtered_rows,
            "records_inserted": inserted_rows,
            "records_updated": updated_rows,
            "historical_total": total_final,
        }

    @task()
    def resumen_salida(resultado_transformacion: dict):
        import logging

        archivos_procesados = resultado_transformacion.get("files_processed", [])
        registros_procesados = resultado_transformacion.get("records_processed", 0)
        registros_filtrados = resultado_transformacion.get("records_filtered", 0)

        logging.info("========== SALIDA DEL DAG ==========")
        logging.info(f"Archivos procesados  : {archivos_procesados}")
        logging.info(f"Registros procesados : {registros_procesados}")
        logging.info(f"Registros filtrados  : {registros_filtrados}")
        logging.info("====================================")

        return {
            "archivos_procesados": archivos_procesados,
            "registros_procesados": registros_procesados,
            "registros_filtrados": registros_filtrados,
        }

    @task()
    def enviar_reporte_email(resumen: dict):
        from airflow.utils.email import send_email

        archivos = resumen.get("archivos_procesados", [])
        registros_procesados = resumen.get("registros_procesados", 0)
        registros_filtrados = resumen.get("registros_filtrados", 0)

        archivos_html = "".join(f"<li>{a}</li>" for a in archivos) if archivos else "<li>No files processed</li>"

        html_content = f"""
        <h3>ELT DuckDB pipeline finished successfully</h3>
        <p>The pipeline completed successfully.</p>

        <h4>Summary</h4>
        <ul>
            <li><strong>Processed records:</strong> {registros_procesados}</li>
            <li><strong>Filtered records:</strong> {registros_filtrados}</li>
        </ul>

        <h4>Processed files</h4>
        <ul>
            {archivos_html}
        </ul>
        """

        send_email(
            to="sergio85sanchez1@gmail.com",
            subject="ELT DuckDB pipeline finished successfully",
            html_content=html_content,
            conn_id="smtp_default",
        )

    preparar = preparar_control_y_staging()
    detection_info = detectar_archivos_nuevos()
    gate = hay_archivos_nuevos(detection_info)
    staging_info = cargar_staging_incremental(detection_info)
    resultado_transformacion = transformar_datos(staging_info)
    salida = resumen_salida(resultado_transformacion)
    email_ok = enviar_reporte_email(salida)

    preparar >> detection_info >> gate >> staging_info >> resultado_transformacion >> salida >> email_ok


elt_duckdb_pipeline_V7 = elt_pipeline()