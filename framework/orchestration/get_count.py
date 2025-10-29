from framework.utils.custom_logger import CustomLogger, trace

@trace(logger_attr_name="logger")
def get_count(logger: CustomLogger, config: dict, record: dict, count_function: callable) -> dict:
    """
    Executes the provided count function, handles exceptions, and returns a standardized result.

    Returns:
        {
            "count": int,
            "continue_dag_run": True|False,
            "error": None|str
        }
    """
    try:
        count = count_function(logger, config, record)
        return {"count": count, "continue_dag_run": True, "error": None}
    except Exception as e:
        msg = f"Count function {getattr(count_function, '__name__', str(count_function))} failed: {e}"
        logger.error(msg, tag="GET_COUNT")
        return {"count": 0, "continue_dag_run": False, "error": msg}
