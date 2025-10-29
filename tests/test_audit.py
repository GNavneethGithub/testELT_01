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

from framework.audit.audit import audit_counts
from framework.utils.custom_logger import CustomLogger

if __name__ == "__main__":
    logger = CustomLogger("TestAudit")
    config = {}
    record = {
        "id": "test_record_123",
        "source_table": "source.table",
        "target_table": "target.table"
    }

    audit_counts(logger, config, record)
