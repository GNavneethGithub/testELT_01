import sys
import os

# --- Manually set your path ---
# For LOCAL testing
project_root = r'D:\all_python_projects\project_01'
# For PRODUCTION (uncomment this on the server)
# project_root = '/opt/airflow/my_project'
# --------------------------------

if project_root not in sys.path:
    sys.path.append(project_root)

from framework.orchestration.cleaning_orchestrator import cleaning_orchestrator
from framework.stg_scripts.stg_cleaning import stage_cleaning
from framework.trg_scripts.trg_cleaning import target_cleaning
from framework.utils.custom_logger import CustomLogger

if __name__ == "__main__":
    logger = CustomLogger("TestCleaning")
    config = {}
    record = {
        "id": "test_record_456",
    }

    cleaning_orchestrator(logger, config, record, stage_cleaning, target_cleaning)
