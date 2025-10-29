from framework.utils.custom_logger import CustomLogger, trace

@trace(logger_attr_name="logger")
def get_source_count(logger: CustomLogger, config: dict, record: dict) -> int:
    """
    (Placeholder) Gets the count of records from the source table.
    """
    table_name = record.get("source_table", "UNKNOWN_SOURCE")
    logger.info(f"[RUNNING] Getting source count for table: {table_name}", tag="GET_SOURCE_COUNT")
    
    # Simulate getting the count
    count = 100
    
    logger.info(f"[COMPLETED] Source count for table {table_name} is {count}", tag="GET_SOURCE_COUNT")
    return count
