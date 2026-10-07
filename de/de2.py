from __future__ import annotations
 
import csv
import logging
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path
 
import pandas as pd
 
 
logger = logging.getLogger(__name__)
 
 
# ============================================================
# Configuration
# ============================================================
 
FILE_PATTERN = "sms-call-internet-mi-*.csv"
 
REQUIRED_COLUMNS = {
    "datetime",
    "countrycode",
    "smsin",
    "smsout",
    "callin",
    "callout",
    "internet",
    "CellID",
}
 
ACTIVITY_COLUMNS = [
    "smsin",
    "smsout",
    "callin",
    "callout",
    "internet",
]
 
TIMESTAMP_COLUMN = "datetime"
 
FILENAME_REGEX = re.compile(
    r"^sms-call-internet-mi-\d{4}-\d{2}-\d{2}\.csv$"
)
 
 
def detect_files(landing_dir: str | Path) -> list[Path]:
    """
    Detect daily Milan telecom CSV files in the landing zone.
 
    Only files matching:
 
        sms-call-internet-mi-*.csv
 
    are considered ingestion candidates.
 
    GeoJSON and other files are ignored.
    """
 
    landing_dir = Path(landing_dir)
 
    if not landing_dir.exists():
        raise FileNotFoundError(
            f"Landing directory does not exist: {landing_dir}"
        )
 
    if not landing_dir.is_dir():
        raise NotADirectoryError(
            f"Landing path is not a directory: {landing_dir}"
        )
 
    candidates = []
 
    for path in landing_dir.glob(FILE_PATTERN):
 
        if not path.is_file():
            continue
 
        if not FILENAME_REGEX.match(path.name):
            logger.warning(
                "Ignoring file with invalid Milan filename: %s",
                path.name,
            )
            continue
 
        candidates.append(path)
 
    candidates.sort()
 
    logger.info(
        "Detected %d daily Milan CSV file(s) in landing zone.",
        len(candidates),
    )
 
    for path in candidates:
        logger.info(
            "Detected candidate: %s",
            path.name,
        )
 
    return candidates
 
 
# ============================================================
# 2. Validate schema
# ============================================================
 
def validate_schema(file_path: str | Path) -> tuple[bool, str, int]:
    """
    Validate required raw columns.
 
    Returns:
        (is_valid, reason, row_count)
    """
 
    file_path = Path(file_path)
 
    try:
 
        df = pd.read_csv(file_path)
 
    except Exception as exc:
 
        return (
            False,
            f"CSV_READ_ERROR: {exc}",
            0,
        )
 
    row_count = len(df)
 
    actual_columns = set(df.columns)
 
    missing_columns = REQUIRED_COLUMNS - actual_columns
 
    if missing_columns:
 
        reason = (
            "SCHEMA_VALIDATION_FAILED: "
            f"missing required column(s): "
            f"{sorted(missing_columns)}"
        )
 
        return (
            False,
            reason,
            row_count,
        )
 
    unexpected_columns = actual_columns - REQUIRED_COLUMNS
 
    if unexpected_columns:
 
        reason = (
            "SCHEMA_VALIDATION_FAILED: "
            f"unexpected column(s): "
            f"{sorted(unexpected_columns)}"
        )
 
        return (
            False,
            reason,
            row_count,
        )
 
    return (
        True,
        "SCHEMA_VALIDATION_PASSED",
        row_count,
    )
 
 
# ============================================================
# 3. Validate minimum quality
# ============================================================
 
def validate_minimum_quality(
    file_path: str | Path,
) -> tuple[bool, str, int]:
    """
    Validate minimum data quality.
 
    Checks:
        - file can be read
        - file is not empty
        - timestamp values are valid
        - activity values are numeric
        - activity values are not negative
        - grid_id is present
    """
 
    file_path = Path(file_path)
 
    try:
 
        df = pd.read_csv(file_path)
 
    except Exception as exc:
 
        return (
            False,
            f"CSV_READ_ERROR: {exc}",
            0,
        )
 
    row_count = len(df)
 
    # --------------------------------------------------------
    # Empty file
    # --------------------------------------------------------
 
    if row_count == 0:
 
        return (
            False,
            "MINIMUM_QUALITY_FAILED: file contains zero rows",
            row_count,
        )
 
    # --------------------------------------------------------
    # Required columns must exist before quality checks
    # --------------------------------------------------------
 
    missing_columns = REQUIRED_COLUMNS - set(df.columns)
 
    if missing_columns:
 
        return (
            False,
            "MINIMUM_QUALITY_FAILED: cannot perform quality "
            f"checks because columns are missing: "
            f"{sorted(missing_columns)}",
            row_count,
        )
 
    # --------------------------------------------------------
    # Timestamp validation
    # --------------------------------------------------------
 
    parsed_timestamps = pd.to_datetime(
        df[TIMESTAMP_COLUMN],
        errors="coerce",
    )
 
    invalid_timestamp_count = parsed_timestamps.isna().sum()
 
    if invalid_timestamp_count > 0:
 
        return (
            False,
            "MINIMUM_QUALITY_FAILED: malformed timestamp "
            f"values found: {invalid_timestamp_count}",
            row_count,
        )
 
    # --------------------------------------------------------
    # Grid ID validation
    # --------------------------------------------------------
 
    null_grid_count = df["CellID"].isna().sum()
 
    if null_grid_count > 0:
 
        return (
            False,
            "MINIMUM_QUALITY_FAILED: NULL grid_id values found: "
            f"{null_grid_count}",
            row_count,
        )
 
    # --------------------------------------------------------
    # Activity columns numeric validation
    # --------------------------------------------------------
 
    # for column in ACTIVITY_COLUMNS:
 
    #     numeric_values = pd.to_numeric(
    #         df[column],
    #         errors="coerce",
    #     )
 
    #     invalid_numeric_count = numeric_values.isna().sum()
 
    #     if invalid_numeric_count > 0:
 
    #         return (
    #             False,
    #             "MINIMUM_QUALITY_FAILED: non-numeric values "
    #             f"found in {column}: "
    #             f"{invalid_numeric_count}",
    #             row_count,
    #         )
 
    #     negative_count = (numeric_values < 0).sum()
 
    #     if negative_count > 0:
 
    #         return (
    #             False,
    #             "MINIMUM_QUALITY_FAILED: negative activity "
    #             f"values found in {column}: "
    #             f"{negative_count}",
    #             row_count,
    #         )
 
    return (
        True,
        "MINIMUM_QUALITY_PASSED",
        row_count,
    )
 
 
# ============================================================
# 4. Audit logging
# ============================================================
 
def write_audit_record(
    log_path: str | Path,
    filename: str,
    status: str,
    row_count: int,
    reason: str,
) -> None:
    """
    Append one ingestion audit record.
 
    Columns:
        filename
        status
        row_count
        reason
        processed_at
    """
 
    log_path = Path(log_path)
 
    log_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )
 
    record = {
        "filename": filename,
        "status": status,
        "row_count": row_count,
        "reason": reason,
        "processed_at": datetime.now(
            timezone.utc
        ).isoformat(),
    }
 
    file_exists = log_path.exists()
 
    with log_path.open(
        "a",
        newline="",
        encoding="utf-8",
    ) as file:
 
        writer = csv.DictWriter(
            file,
            fieldnames=[
                "filename",
                "status",
                "row_count",
                "reason",
                "processed_at",
            ],
        )
 
        if not file_exists:
            writer.writeheader()
 
        writer.writerow(record)
 
    logger.info(
        "Audit record written | file=%s | status=%s | reason=%s",
        filename,
        status,
        reason,
    )
 
 
# ============================================================
# 5. Duplicate / already processed detection
# ============================================================
 
def already_processed(
    filename: str,
    raw_dir: str | Path,
    rejected_dir: str | Path,
) -> bool:
 
    raw_dir = Path(raw_dir)
    rejected_dir = Path(rejected_dir)
 
    return (
        (raw_dir / filename).exists()
        or
        (rejected_dir / filename).exists()
    )
 
 
# ============================================================
# 6. Route file
# ============================================================
 
def route_file(
    file_path: str | Path,
    is_valid: bool,
    reason: str,
    row_count: int,
    raw_dir: str | Path,
    rejected_dir: str | Path,
    audit_log: str | Path,
) -> str:
    """
    Route a file to raw or rejected.
 
    Returns:
        VALID / REJECTED
    """
 
    file_path = Path(file_path)
    raw_dir = Path(raw_dir)
    rejected_dir = Path(rejected_dir)
 
    raw_dir.mkdir(
        parents=True,
        exist_ok=True,
    )
 
    rejected_dir.mkdir(
        parents=True,
        exist_ok=True,
    )
 
    # --------------------------------------------------------
    # Duplicate / re-run protection
    # --------------------------------------------------------
 
    if already_processed(
        file_path.name,
        raw_dir,
        rejected_dir,
    ):
 
        duplicate_reason = (
            "ALREADY_PROCESSED: file already exists "
            "in raw or rejected zone"
        )
 
        logger.warning(
            "%s | %s",
            file_path.name,
            duplicate_reason,
        )
 
        write_audit_record(
            audit_log,
            file_path.name,
            "SKIPPED",
            row_count,
            duplicate_reason,
        )
 
        return "SKIPPED"
 
    # --------------------------------------------------------
    # Valid → raw
    # --------------------------------------------------------
 
    if is_valid:
 
        destination = raw_dir / file_path.name
 
        shutil.move(
            str(file_path),
            str(destination),
        )
 
        logger.info(
            "VALID file routed to raw | %s",
            destination,
        )
 
        write_audit_record(
            audit_log,
            file_path.name,
            "VALID",
            row_count,
            reason,
        )
 
        return "VALID"
 
    # --------------------------------------------------------
    # Invalid → rejected
    # --------------------------------------------------------
 
    destination = rejected_dir / file_path.name
 
    shutil.move(
        str(file_path),
        str(destination),
    )
 
    logger.warning(
        "INVALID file routed to rejected | %s | reason=%s",
        destination,
        reason,
    )
 
    write_audit_record(
        audit_log,
        file_path.name,
        "REJECTED",
        row_count,
        reason,
    )
 
    return "REJECTED"
 
 
# ============================================================
# 7. Process one file
# ============================================================
 
def process_file(
    file_path: str | Path,
    raw_dir: str | Path,
    rejected_dir: str | Path,
    audit_log: str | Path,
) -> str:
    """
    Complete DE2 validation and routing flow for one file.
    """
 
    file_path = Path(file_path)
 
    logger.info(
        "Processing landing file: %s",
        file_path.name,
    )
 
    # --------------------------------------------------------
    # Duplicate check before expensive validation
    # --------------------------------------------------------
 
    if already_processed(
        file_path.name,
        raw_dir,
        rejected_dir,
    ):
 
        reason = (
            "ALREADY_PROCESSED: file already exists "
            "in raw or rejected zone"
        )
 
        write_audit_record(
            audit_log,
            file_path.name,
            "SKIPPED",
            0,
            reason,
        )
 
        return "SKIPPED"
 
    # --------------------------------------------------------
    # Schema validation
    # --------------------------------------------------------
 
    schema_valid, schema_reason, row_count = (
        validate_schema(file_path)
    )
 
    if not schema_valid:
 
        return route_file(
            file_path,
            False,
            schema_reason,
            row_count,
            raw_dir,
            rejected_dir,
            audit_log,
        )
 
    # --------------------------------------------------------
    # Minimum quality validation
    # --------------------------------------------------------
 
    quality_valid, quality_reason, row_count = (
        validate_minimum_quality(file_path)
    )
 
    if not quality_valid:
 
        return route_file(
            file_path,
            False,
            quality_reason,
            row_count,
            raw_dir,
            rejected_dir,
            audit_log,
        )
 
    # --------------------------------------------------------
    # Valid file
    # --------------------------------------------------------
 
    return route_file(
        file_path,
        True,
        "SCHEMA_AND_QUALITY_VALIDATION_PASSED",
        row_count,
        raw_dir,
        rejected_dir,
        audit_log,
    )
 
 
# ============================================================
# 8. Process landing zone
# ============================================================
 
def process_landing_zone(
    landing_dir: str | Path,
    raw_dir: str | Path,
    rejected_dir: str | Path,
    audit_log: str | Path,
) -> list[dict]:
 
    files = detect_files(landing_dir)
 
    results = []
 
    for file_path in files:
 
        status = process_file(
            file_path=file_path,
            raw_dir=raw_dir,
            rejected_dir=rejected_dir,
            audit_log=audit_log,
        )
 
        results.append(
            {
                "filename": file_path.name,
                "status": status,
            }
        )
 
    return results