from __future__ import annotations
 
import logging
import sys
import os
from pathlib import Path
 
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
 
 
# ============================================================
# Project paths
# ============================================================
 
# PROJECT_ROOT = Path(__file__).resolve().parents[2]
PROJECT_ROOT = "/mnt/c/GraddedAssignment"
 
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
 
 
from de.de2 import (
    detect_files,
    validate_schema,
    validate_minimum_quality,
    route_file,
    already_processed,
    write_audit_record,
)
 
 
logger = logging.getLogger(__name__)
 
# LANDING_DIR = PROJECT_ROOT / "data" / "landing"
# RAW_DIR = PROJECT_ROOT / "data" / "raw" / "csv"
# REJECTED_DIR = PROJECT_ROOT / "data" / "rejected"
# AUDIT_LOG = PROJECT_ROOT / "logs" / "de2_ingestion.csv"
 
LANDING_DIR= os.path.join(PROJECT_ROOT, "dataset","landingde")
RAW_DIR= os.path.join(PROJECT_ROOT,"dataset","raw")
REJECTED_DIR= os.path.join(PROJECT_ROOT,"dataset","rejectedde")
AUDIT_LOG= os.path.join(PROJECT_ROOT, "logs", "de2_ingestion.csv")
 
 
# ============================================================
# Airflow task 1 — Detect
# ============================================================
 
def detect_task(**context):
 
    files = detect_files(
        LANDING_DIR
    )
 
    filenames = [
        str(path)
        for path in files
    ]
 
    logger.info(
        "DE2 detected %d file(s).",
        len(filenames),
    )
 
    # XCom
    context["ti"].xcom_push(
        key="detected_files",
        value=filenames,
    )
 
 
# ============================================================
# Airflow task 2 — Validate
# ============================================================
 
def validate_task(**context):
 
    files = context["ti"].xcom_pull(
        task_ids="detect_files",
        key="detected_files",
    )
 
    validation_results = []
 
    for file_string in files or []:
 
        file_path = Path(file_string)
 
        # ----------------------------------------------------
        # Duplicate check
        # ----------------------------------------------------
 
        if already_processed(
            file_path.name,
            RAW_DIR,
            REJECTED_DIR,
        ):
 
            validation_results.append(
                {
                    "file": str(file_path),
                    "valid": False,
                    "status": "SKIPPED",
                    "row_count": 0,
                    "reason": (
                        "ALREADY_PROCESSED: file already exists "
                        "in raw or rejected zone"
                    ),
                }
            )
 
            continue
 
        # ----------------------------------------------------
        # Schema validation
        # ----------------------------------------------------
 
        schema_valid, schema_reason, row_count = (
            validate_schema(file_path)
        )
 
        if not schema_valid:
 
            validation_results.append(
                {
                    "file": str(file_path),
                    "valid": False,
                    "status": "REJECTED",
                    "row_count": row_count,
                    "reason": schema_reason,
                }
            )
 
            continue
 
        # ----------------------------------------------------
        # Minimum quality validation
        # ----------------------------------------------------
 
        quality_valid, quality_reason, row_count = (
            validate_minimum_quality(file_path)
        )
 
        if not quality_valid:
 
            validation_results.append(
                {
                    "file": str(file_path),
                    "valid": False,
                    "status": "REJECTED",
                    "row_count": row_count,
                    "reason": quality_reason,
                }
            )
 
            continue
 
        # ----------------------------------------------------
        # Valid
        # ----------------------------------------------------
 
        validation_results.append(
            {
                "file": str(file_path),
                "valid": True,
                "status": "VALID",
                "row_count": row_count,
                "reason": (
                    "SCHEMA_AND_QUALITY_VALIDATION_PASSED"
                ),
            }
        )
 
    context["ti"].xcom_push(
        key="validation_results",
        value=validation_results,
    )
 
    logger.info(
        "Validation completed for %d file(s).",
        len(validation_results),
    )
 
 
# ============================================================
# Airflow task 3 — Route
# ============================================================
 
def route_task(**context):
 
    validation_results = context["ti"].xcom_pull(
        task_ids="validate_files",
        key="validation_results",
    )
 
    routing_results = []
 
    for result in validation_results or []:
 
        file_path = Path(result["file"])
 
        # ----------------------------------------------------
        # Already processed
        # ----------------------------------------------------
 
        if result["status"] == "SKIPPED":
 
            write_audit_record(
                AUDIT_LOG,
                file_path.name,
                "SKIPPED",
                result["row_count"],
                result["reason"],
            )
 
            routing_results.append(
                {
                    "filename": file_path.name,
                    "status": "SKIPPED",
                }
            )
 
            continue
 
        # ----------------------------------------------------
        # Route valid / invalid file
        # ----------------------------------------------------
 
        status = route_file(
            file_path=file_path,
            is_valid=result["valid"],
            reason=result["reason"],
            row_count=result["row_count"],
            raw_dir=RAW_DIR,
            rejected_dir=REJECTED_DIR,
            audit_log=AUDIT_LOG,
        )
 
        routing_results.append(
            {
                "filename": file_path.name,
                "status": status,
            }
        )
 
    context["ti"].xcom_push(
        key="routing_results",
        value=routing_results,
    )
 
    logger.info(
        "Routing completed."
    )
 
 
# ============================================================
# Airflow task 4 — Log
# ============================================================
 
def log_task(**context):
 
    routing_results = context["ti"].xcom_pull(
        task_ids="route_files",
        key="routing_results",
    )
 
    logger.info("=" * 60)
    logger.info("DE2 INGESTION SUMMARY")
    logger.info("=" * 60)
 
    for result in routing_results or []:
 
        logger.info(
            "filename=%s | status=%s",
            result["filename"],
            result["status"],
        )
 
    logger.info(
        "Audit log: %s",
        AUDIT_LOG,
    )
 
 
# ============================================================
# DAG
# ============================================================
 
with DAG(
    dag_id="de2_landing_to_raw",
    description=(
        "DE2 landing-to-raw telecom ingestion flow"
    ),
    start_date=datetime(2026, 9, 1),
    schedule=None,
    catchup=False,
    tags=[
        "network-project",
        "DE2",
        "ingestion",
    ],
) as dag:
 
    detect_files_task = PythonOperator(
        task_id="detect_files",
        python_callable=detect_task,
    )
 
    validate_files_task = PythonOperator(
        task_id="validate_files",
        python_callable=validate_task,
    )
 
    route_files_task = PythonOperator(
        task_id="route_files",
        python_callable=route_task,
    )
 
    log_results_task = PythonOperator(
        task_id="log_results",
        python_callable=log_task,
    )
 
    (
        detect_files_task
        >> validate_files_task
        >> route_files_task
        >> log_results_task
    )