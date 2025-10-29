from framework.utils.custom_logger import CustomLogger, trace

@trace(logger_attr_name="logger")
def stage_cleaning(logger: CustomLogger, config: dict, record: dict):
    """
    (Placeholder) The cleanup logic for the STAGE layer.
    """
    logger.info("[RUNNING] Cleaning up corrupt STAGE data...", tag="STAGE_CLEANUP")
    
    # Simulate a cleanup failure
    if record.get("id") == "fail-both-456":
        raise ConnectionError("Simulated cleanup error: Cannot connect to archive")

    logger.info("[COMPLETED] STAGE cleanup finished.", tag="STAGE_CLEANUP")
    pass