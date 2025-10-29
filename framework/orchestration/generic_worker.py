import json
import sys
import os
import importlib
from typing import Any, Dict
from framework.utils.custom_logger import CustomLogger
from framework.orchestration.generic_transfer_orchestrator import generic_transfer_orchestrator

def write_json_file(file_path: str, data: Dict[str, Any]):
    with open(file_path, 'w') as f:
        json.dump(data, f, indent=4)

def dynamic_import(func_path: str):
    """
    Dynamically import a function by its full path.
    Example: "my_module.my_submodule.my_function"
    """
    module_path, func_name = func_path.rsplit(".", 1)
    module = importlib.import_module(module_path)
    return getattr(module, func_name)

def main_worker(result_path: str):
    record_id = "unknown"
    try:
        stdin_data = sys.stdin.read()
        data = json.loads(stdin_data)

        config = data["config"]
        record = data["record"]
        record_id = record.get("id", "unknown")

        transfer_func_path = data["transfer_func"]
        cleanup_func_path = data["cleanup_func"]
        process_type = data.get("process_type", "src_to_stg")

        transfer_func = dynamic_import(transfer_func_path)
        cleanup_func = dynamic_import(cleanup_func_path)

        logger = CustomLogger(f"worker_{record_id}")
        logger.info(f"--- Worker Started for record: {record_id} ---")

        result = generic_transfer_orchestrator(
            logger=logger,
            config=config,
            record=record,
            transfer_func=transfer_func,
            cleanup_func=cleanup_func,
            process_type=process_type
        )

        write_json_file(result_path, result)
        logger.info(f"--- Worker Finished for record: {record_id} ---")

        sys.exit(0)

    except Exception as e:
        print(f"CRITICAL WORKER FAILURE (Record: {record_id}): {e}")
        result = {
            "continue_dag_run": False,
            "error": {"error01": f"Critical Worker Failure: {str(e)}"}
        }
        try:
            write_json_file(result_path, result)
        except:
            pass
        sys.exit(1)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Usage: python generic_worker.py <result_path>")
        sys.exit(1)
    main_worker(result_path=sys.argv[1])



