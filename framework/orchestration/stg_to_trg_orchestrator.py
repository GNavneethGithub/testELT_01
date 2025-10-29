"""
Stage-to-Target Orchestrator
-----------------------------
Defines the transfer and cleanup functions for Stage → Target process
and runs them in parallel using the generic parallel orchestrator.
"""

from framework.utils.custom_logger import CustomLogger
from framework.orchestration.run_generic_parallel_jobs import run_generic_parallel_jobs


# --- Step 1: Define your pipeline functions ---

def stg_to_trg_transfer(logger, config, record):
    """
    Example transfer step from staging area to target.
    """
    logger.info(f"[STG→TRG] Transferring record {record['id']} to target...")
    if "fail" in record.get("id", "").lower():
        raise Exception("Simulated transfer failure")
    logger.info(f"[STG→TRG] Transfer completed for {record['id']}")


def stg_to_trg_cleanup(logger, config, record):
    """
    Example cleanup step for failed Stage→Target transfers.
    """
    logger.info(f"[STG→TRG] Cleaning up after failure for record {record['id']}...")
    # Add cleanup logic here
    logger.info(f"[STG→TRG] Cleanup complete for {record['id']}")


# --- Step 2: Define a DAG-friendly callable ---

def run_stg_to_trg_parallel(config: dict, records: list):
    """
    Airflow-compatible callable that runs Stage→Target transfers in parallel.
    """
    return run_generic_parallel_jobs(
        config=config,
        records=records,
        transfer_func_path="framework.orchestration.stg_to_trg_orchestrator.stg_to_trg_transfer",
        cleanup_func_path="framework.orchestration.stg_to_trg_orchestrator.stg_to_trg_cleanup",
        process_type="stg_to_trg",
    )


