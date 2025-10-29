import subprocess
import json
import os
import shutil
import uuid
from typing import List, Dict, Any

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
WORKER_SCRIPT_PATH = os.path.join(SCRIPT_DIR, "generic_worker.py")
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, '..', '..'))
BASE_JOB_DIR = os.path.join(PROJECT_ROOT, "parallel_jobs_tmp")

def setup_job_directory(job_dir_path: str, records: List[Dict[str, Any]]):
    os.makedirs(job_dir_path)
    job_specs = []
    for i, record in enumerate(records):
        job_id = f"job_{i+1:03d}"
        job_path = os.path.join(job_dir_path, job_id)
        os.makedirs(job_path)
        job_spec = {
            "id": job_id,
            "record_id": record.get("id", "unknown"),
            "result_path": os.path.join(job_path, "result.json"),
            "stdout_log_path": os.path.join(job_path, "stdout.log"),
            "stderr_log_path": os.path.join(job_path, "stderr.log"),
        }
        job_specs.append(job_spec)
    return job_specs

def run_jobs_in_parallel(job_specs, main_config, all_records, transfer_func_path, cleanup_func_path, process_type):
    print(f"--- Launching {len(job_specs)} parallel jobs ---")
    for i, job in enumerate(job_specs):
        command = ["python", WORKER_SCRIPT_PATH, job["result_path"]]

        data_to_pass = {
            "config": main_config,
            "record": all_records[i],
            "transfer_func": transfer_func_path,
            "cleanup_func": cleanup_func_path,
            "process_type": process_type
        }

        stdin_data = json.dumps(data_to_pass)
        stdout_file = open(job["stdout_log_path"], 'w')
        stderr_file = open(job["stderr_log_path"], 'w')

        process = subprocess.Popen(
            command,
            stdin=subprocess.PIPE,
            stdout=stdout_file,
            stderr=stderr_file,
            text=True,
            cwd=PROJECT_ROOT
        )

        process.stdin.write(stdin_data)
        process.stdin.close()
        job["process"] = process

        print(f"Launched {job['id']} (Record: {job['record_id']}, PID: {process.pid})")

    print("--- Waiting for all jobs to finish... ---")
    for job in job_specs:
        job["process"].wait()
    print("--- All jobs completed ---")

def summarize_results(job_specs):
    success, fail = 0, 0
    for job in job_specs:
        try:
            with open(job["result_path"], 'r') as f:
                result = json.load(f)
            if result.get("continue_dag_run"):
                print(f"{job['id']} SUCCESS ({job['record_id']})")
                success += 1
            else:
                print(f"{job['id']} FAILED ({job['record_id']})")
                fail += 1
        except Exception:
            fail += 1
            print(f"{job['id']} CRASHED ({job['record_id']})")
    print(f"Total Success: {success}, Total Failed: {fail}")
    return {"continue_dag_run": success > 0}

if __name__ == "__main__":
    UNIQUE_RUN_ID = str(uuid.uuid4())
    JOB_DIR_PATH = os.path.join(BASE_JOB_DIR, UNIQUE_RUN_ID)

    try:
        main_config = {"env": "prod"}
        records = [
            {"id": "record_001"},
            {"id": "record_002"},
            {"id": "record_003"},
        ]

        job_specs = setup_job_directory(JOB_DIR_PATH, records)
        run_jobs_in_parallel(
            job_specs,
            main_config,
            records,
            transfer_func_path="framework.demo_funcs.demo_transfer",
            cleanup_func_path="framework.demo_funcs.demo_cleanup",
            process_type="src_to_stg"
        )
        summarize_results(job_specs)
    finally:
        shutil.rmtree(JOB_DIR_PATH, ignore_errors=True)



def run_generic_parallel_jobs(
    config: Dict[str, Any],
    records: List[Dict[str, Any]],
    transfer_func_path: str,
    cleanup_func_path: str,
    process_type: str = "src_to_stg",
) -> Dict[str, Any]:
    """
    DAG-friendly entrypoint. Runs all jobs in parallel using subprocess workers.
    Automatically cleans up after completion.
    """
    UNIQUE_RUN_ID = str(uuid.uuid4())
    JOB_DIR_PATH = os.path.join(BASE_JOB_DIR, UNIQUE_RUN_ID)
    os.makedirs(JOB_DIR_PATH, exist_ok=True)

    try:
        job_specs = setup_job_directory(JOB_DIR_PATH, records)
        run_jobs_in_parallel(
            job_specs,
            config,
            records,
            transfer_func_path,
            cleanup_func_path,
            process_type,
        )
        result = summarize_results(job_specs)
        return result  # <-- Returned to Airflow (pushed to XCom)
    finally:
        shutil.rmtree(JOB_DIR_PATH, ignore_errors=True)
        print(f"Cleaned up temporary directory: {JOB_DIR_PATH}")