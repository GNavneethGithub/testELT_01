from framework.utils.custom_logger import CustomLogger, trace
from framework.orchestration.get_count import get_count
from framework.src_scripts.get_source_count import get_source_count
from framework.trg_scripts.get_target_count import get_target_count
from framework.drive_tbl_scripts.sf_drive_tbl_scripts import update_record_details_in_drive_table

@trace(logger_attr_name="logger")
def audit_counts(logger: CustomLogger, config: dict, record: dict):
    """
    Runs both source and target count functions (even if one fails),
    collects errors, updates the drive table, and returns a standardized result.

    Return shape:
    {
        "source_count": int,
        "target_count": int,
        "audit_result": "MATCH"|"MISMATCH"|"ERROR",
        "continue_dag_run": bool,
        "error": None | { "error01": "...", "error02": "..." }
    }
    """
    logger.info("[RUNNING] Starting audit process...", tag="AUDIT")

    errors = {}
    err_idx = 0

    # --- Run source count (in try/except to be safe) ---
    try:
        src_res = get_count(logger, config, record, get_source_count)
    except Exception as e:
        src_res = {"count": 0, "continue_dag_run": False, "error": f"get_count raised: {e}"}

    if not src_res.get("continue_dag_run", False):
        err_idx += 1
        errors[f"error{err_idx:02d}"] = f"Source count error: {src_res.get('error')}"

    # --- Run target count (always run) ---
    try:
        trg_res = get_count(logger, config, record, get_target_count)
    except Exception as e:
        trg_res = {"count": 0, "continue_dag_run": False, "error": f"get_count raised: {e}"}

    if not trg_res.get("continue_dag_run", False):
        err_idx += 1
        errors[f"error{err_idx:02d}"] = f"Target count error: {trg_res.get('error')}"

    source_count = src_res.get("count", 0)
    target_count = trg_res.get("count", 0)

    # --- Determine audit result ---
    if errors:
        audit_result = "ERROR"
        continue_dag = False
        record["audit_status"] = "ERROR"
    else:
        if source_count == target_count:
            audit_result = "MATCH"
            continue_dag = True
            record["audit_status"] = "SUCCESS"
        else:
            audit_result = "MISMATCH"
            continue_dag = False
            record["audit_status"] = "FAILURE"
            err_idx += 1
            errors[f"error{err_idx:02d}"] = (
                f"Counts do not match for record {record.get('id')}. "
                f"Source: {source_count}, Target: {target_count}"
            )

    # --- Persist status and finish ---
    update_record_details_in_drive_table(logger, config, record)
    logger.info("[COMPLETED] Audit process finished.", tag="AUDIT")

    return {
        "source_count": source_count,
        "target_count": target_count,
        "audit_result": audit_result,
        "continue_dag_run": continue_dag,
        "error": None if not errors else errors
    }



