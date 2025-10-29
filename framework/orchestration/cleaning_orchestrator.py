from framework.utils.custom_logger import CustomLogger, trace

@trace(logger_attr_name="logger")
def cleaning_orchestrator(logger: CustomLogger, config: dict, record: dict, *cleaning_funcs):
    """
    Orchestrates the execution of multiple cleaning functions.
    """
    logger.info("[RUNNING] Starting cleaning process...", tag="CLEANING_ORCHESTRATOR")

    errors = []

    for func in cleaning_funcs:
        try:
            func(logger, config, record)
        except Exception as e:
            msg = f"Cleaning function {func.__name__} failed: {e}"
            errors.append(msg)
            logger.error(f"[FAILURE] {msg}", tag="CLEANING_ORCHESTRATOR")

    logger.info("[COMPLETED] Cleaning process finished.", tag="CLEANING_ORCHESTRATOR")

    return {
        "continue_dag_run": len(errors) == 0,
        "error": None if not errors else "; ".join(errors)
    }



