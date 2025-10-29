import sys
import os

# --- Manually set your path ---
# For LOCAL testing
project_root = r'D:\all_python_projects\project_01' 
# For PRODUCTION (uncomment this on the server)
# project_root = '/opt/airflow/my_project'
# --------------------------------

print(f"--- Attempting to add project root: {project_root} ---")

# --- Your code, with print statements added ---
if project_root not in sys.path:
    print("Path was NOT found in sys.path. Appending it now...")
    sys.path.append(project_root)
else:
    print("Path was ALREADY in sys.path. No action needed.")

# # --- Optional: Print the full sys.path to confirm ---
# print("\n--- Full sys.path content ---")
# # This loop prints each path on a new line, which is easier to read
# for i, path in enumerate(sys.path):
#     print(f"{i}: {path}")




from utils.custom_logger import CustomLogger, trace


# Example nested functions
@trace()
def func04(logger: CustomLogger):
    logger.debug("Starting calculation in func04", tag="[func04_computation]", details={"x1": 42})
    try:
        result = 10 / 0
    except ZeroDivisionError as e:
        logger.error("Division by zero occurred", tag="[func04_error]", details={"error": str(e)})
    return "done"


@trace()
def func03(logger: CustomLogger):
    logger.info("Entering func03", tag="[func03_checks_probability]", details={"probability": 0.8})
    func04(logger)
    logger.info("Exiting func03", tag="[func03_summary]", details={"status": "ok"})


@trace()
def func02(logger: CustomLogger):
    func03(logger)


@trace()
def func01(logger: CustomLogger):
    func02(logger)


if __name__ == "__main__":
    logger = CustomLogger("MyApp")
    func01(logger)


