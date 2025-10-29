"""
Source-to-Stage Orchestrator
-----------------------------
Defines the transfer and cleanup functions for Source → Stage process
and runs them in parallel using the generic parallel orchestrator.
"""

from framework.utils.custom_logger import CustomLogger
from framework.orchestration.run_generic_parallel_jobs import run_generic_parallel_jobs


# --- Step 1: Define your pipeline functions ---

def src_to_stg_transfer(logger, config, record):
    """
    Example transfer step from source system to staging area.
    Replace this with your actual logic.
    """
    logger.info(f"[SRC→STG] Transferring record {record['id']} from {record.get('source', 'unknown')}...")
    # Simulate success/failure
    if "fail" in record.get("id", "").lower():
        raise Exception("Simulated transfer failure")
    logger.info(f"[SRC→STG] Transfer completed for {record['id']}")


def src_to_stg_cleanup(logger, config, record):
    """
    Example cleanup step for failed Source→Stage transfers.
    """
    logger.info(f"[SRC→STG] Cleaning up after failure for record {record['id']}...")
    # Add cleanup logic here (e.g., remove partial files, rollback DB, etc.)
    logger.info(f"[SRC→STG] Cleanup complete for {record['id']}")


# --- Step 2: Define a DAG-friendly callable ---

def run_src_to_stg_parallel(config: dict, records: list):
    """
    Airflow-compatible callable that runs Source→Stage transfers in parallel.
    """
    return run_generic_parallel_jobs(
        config=config,
        records=records,
        transfer_func_path="framework.orchestration.src_to_stg_orchestrator.src_to_stg_transfer",
        cleanup_func_path="framework.orchestration.src_to_stg_orchestrator.src_to_stg_cleanup",
        process_type="src_to_stg",
    )


