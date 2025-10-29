# framework/orchestration/generic_transfer_orchestrator.py

import datetime
import pytz
from typing import Callable, Dict, Any
from framework.utils.helpers import (
    get_timezone,
    format_duration,
    update_record_values,
    send_mail_alert
)
from framework.drive_tbl_scripts.sf_drive_tbl_scripts import (
    update_record_details_in_drive_table,
    rest_record_details_in_drive_table
)
from framework.utils.custom_logger import CustomLogger


def generic_transfer_orchestrator(
    logger: CustomLogger,
    config: Dict[str, Any],
    record: Dict[str, Any],
    transfer_func: Callable[[CustomLogger, Dict[str, Any], Dict[str, Any]], None],
    cleanup_func: Callable[[CustomLogger, Dict[str, Any], Dict[str, Any]], None],
    process_type: str = "src_to_stg"
) -> Dict[str, Any]:
    """
    Generic orchestrator for data transfer jobs.
    Handles transfer, cleanup, retries, state tracking, and error logging.

    Args:
        logger: Custom logger for structured logging.
        config: Configuration dictionary.
        record: Record dictionary for this job.
        transfer_func: Function performing the main transfer logic.
        cleanup_func: Function performing cleanup logic.
        process_type: Optional label (e.g., 'src_to_stg', 'stg_to_prod').

    Returns:
        dict: {
            "continue_dag_run": bool,
            "error": dict or None
        }
    """
    TAG = f"GENERIC_ORCHESTRATOR_{process_type.upper()}"
    process_failed = False
    error_dict = {}
    error_count = 0

    tz = get_timezone(logger, config)
    start_time = datetime.datetime.now(tz)

    try:
        logger.info(f"[STARTED] {process_type} process started.", tag=TAG)
        details = {
            f"{process_type}_status": "RUNNING",
            f"{process_type}_started_at": start_time.isoformat(),
            f"{process_type}_ended_at": None,
            f"{process_type}_duration_str": None,
            f"{process_type}_error": None
        }
        record = update_record_values(record, details)
        update_record_details_in_drive_table(logger, config, record)

        # --- Run transfer function ---
        try:
            transfer_func(logger, config, record)
        except Exception as e:
            process_failed = True
            error_count += 1
            error_dict[f"error{error_count:02d}"] = f"Transfer Error: {str(e)}"
            logger.error(f"Transfer function failed: {str(e)}", tag=TAG)

    except Exception as e:
        process_failed = True
        error_count += 1
        error_dict[f"error{error_count:02d}"] = f"Critical Error: {str(e)}"
        logger.critical(f"[CRITICAL] Non-recoverable error: {str(e)}", tag=TAG)

    finally:
        # --- Run cleanup if transfer failed ---
        if process_failed:
            try:
                logger.info(f"[CLEANUP] Starting cleanup step...", tag=TAG)
                cleanup_func(logger, config, record)
                logger.info(f"[CLEANUP COMPLETED]", tag=TAG)
            except Exception as clean_e:
                error_count += 1
                error_dict[f"error{error_count:02d}"] = f"Cleanup Error: {str(clean_e)}"
                logger.error(f"[CLEANUP FAILED]: {str(clean_e)}", tag=TAG)

        # --- Finalize record status ---
        end_time = datetime.datetime.now(tz)
        duration_sec = (end_time - start_time).total_seconds()

        if not process_failed:
            logger.info(f"[COMPLETED] {process_type} process successful.", tag=TAG)
            details = {
                f"{process_type}_status": "COMPLETED",
                f"{process_type}_ended_at": end_time.isoformat(),
                f"{process_type}_duration_str": format_duration(duration_sec)
            }
            record = update_record_values(record, details)
            update_record_details_in_drive_table(logger, config, record)
        else:
            logger.error(f"[FAILED] {process_type} process failed: {error_dict}", tag=TAG)
            details = {
                f"{process_type}_status": "FAILED",
                f"{process_type}_ended_at": end_time.isoformat(),
                f"{process_type}_duration_str": format_duration(duration_sec),
                f"{process_type}_error": error_dict
            }
            record = update_record_values(record, details)
            rest_record_details_in_drive_table(logger, config, record)

            # Send alert
            try:
                send_mail_alert(logger, config, record, error_dict)
            except Exception as alert_e:
                logger.error(f"Alert sending failed: {str(alert_e)}", tag=TAG)

        # --- Return standardized result ---
        return {
            "continue_dag_run": not process_failed,
            "error": None if not process_failed else error_dict
        }



