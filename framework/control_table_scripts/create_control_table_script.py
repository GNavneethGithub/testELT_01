# framework/control_table_scripts/create_control_table_script.py

import snowflake.connector
from typing import Dict, Any, Tuple
from framework.utils.custom_logger import CustomLogger, trace
from framework.utils.snowflake_connector import get_snowflake_connection

def _execute_create_table(
    create_sql: str,
    fully_qualified_name: str,
    db_name: str,
    schema_name: str,
    config: Dict[str, Any], 
    logger: CustomLogger,
    log_tag: str
) -> Dict[str, Any]:
    """
    Internal helper function to execute a CREATE TABLE IF NOT EXISTS statement.
    Handles connection, context setting (DB, Schema), execution, and logging.
    """
    
    return_val = {
        "continue_dag_run": False,
        "error": None
    }
    
    conn = None
    query_id = None

    try:
        conn_result = get_snowflake_connection(config, logger)
        conn = conn_result.get('conn')
        if conn_result.get('error'):
            logger.error(
                f"Failed to connect to Snowflake. Cannot create {fully_qualified_name}.",
                tag=log_tag,
                details=conn_result['error']
            )
            return_val["error"] = conn_result['error']
            return return_val

        with conn.cursor() as cursor:
            use_db_sql = f"USE DATABASE {db_name}"
            logger.info(f"Executing: {use_db_sql}", tag=log_tag)
            cursor.execute(use_db_sql)
            query_id = cursor.sfqid
            logger.info(f"USE DATABASE successful.", tag=log_tag, details={"query_id": query_id})
            
            use_schema_sql = f"USE SCHEMA {schema_name}"
            logger.info(f"Executing: {use_schema_sql}", tag=log_tag)
            cursor.execute(use_schema_sql)
            query_id = cursor.sfqid
            logger.info(f"USE SCHEMA successful.", tag=log_tag, details={"query_id": query_id})

            # Print the DDL as requested
            print(create_sql)

            logger.info(f"Executing CREATE TABLE IF NOT EXISTS for {fully_qualified_name}...", tag=log_tag)
            cursor.execute(create_sql)
            query_id = cursor.sfqid
        
        logger.info(
            f"Successfully created or verified '{fully_qualified_name}'.",
            tag=log_tag,
            details={"query_id": query_id}
        )
        
        return_val["continue_dag_run"] = True
        return return_val
        
    except snowflake.connector.Error as e:
        error_details = {
            "message": e.msg,
            "errno": e.errno,
            "sqlstate": e.sqlstate,
            "sfqid": e.sfqid
        }
        logger.error(
            f"A Snowflake error occurred for {fully_qualified_name}.",
            tag=log_tag,
            details=error_details
        )
        return_val["error"] = error_details
        return return_val
        
    except Exception as e:
        error_details = {
            "message": f"A non-Snowflake error occurred: {e}",
            "error_type": type(e).__name__
        }
        logger.error(
            error_details["message"],
            tag=log_tag,
            details={"error_string": str(e)}
        )
        return_val["error"] = error_details
        return return_val
        
    finally:
        if conn:
            conn.close()
            logger.info("Snowflake connection closed.", tag=log_tag)

@trace(logger_attr_name="logger")
def create_control_table(config: Dict[str, Any], logger: CustomLogger) -> Dict[str, Any]:
    """
    Creates the 'pipeline_config_tbl' in Snowflake if it doesn't already exist.
    """
    log_tag = "create_control_table"
    
    try:
        db_name = config.get("pipeline_config_db")
        schema_name = config.get("pipeline_config_sch")
        table_name = config.get("pipeline_config_tbl")

        if not all([db_name, schema_name, table_name]):
            err_msg = (
                "Missing one or more config keys: 'pipeline_config_db', "
                "'pipeline_config_sch', or 'pipeline_config_tbl'."
            )
            logger.error(err_msg, tag=log_tag)
            return {"continue_dag_run": False, "error": {"message": err_msg, "source": log_tag}}

        fully_qualified_name = f"{db_name}.{schema_name}.{table_name}"
        logger.info(f"Attempting to create control table '{fully_qualified_name}'...", tag=log_tag)

        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {fully_qualified_name} (
            PIPELINE_ID VARCHAR NOT NULL PRIMARY KEY,
            PIPELINE_NAME VARCHAR NOT NULL,
            PIPELINE_CURRENT_STATUS VARCHAR,
            PIPELINE_VALID_FROM TIMESTAMP_TZ,
            PIPELINE_VALID_TILL TIMESTAMP_TZ,
            PIPELINE_OWNER ARRAY,
            PIPELINE_OBJECTS VARIANT,
            PIPELINE_PRIORITY FLOAT,
            RECORD_FIRST_INSERTED TIMESTAMP_TZ,
            RECORD_LAST_UPDATED TIMESTAMP_TZ
        )
        """
        
        return _execute_create_table(
            create_sql, fully_qualified_name, db_name, schema_name, config, logger, log_tag
        )

    except Exception as e:
        # Catch-all for unexpected errors (e.g., config key access)
        err_msg = f"Unexpected error in create_control_table setup: {e}"
        logger.error(err_msg, tag=log_tag, details={"error_type": type(e).__name__})
        return {"continue_dag_run": False, "error": {"message": err_msg, "source": log_tag}}


@trace(logger_attr_name="logger")
def create_tracking_table(config: Dict[str, Any], logger: CustomLogger) -> Dict[str, Any]:
    """
    Creates the 'pipeline_tracking_tbl' in Snowflake if it doesn't already exist.
    """
    log_tag = "create_tracking_table"
    
    try:
        db_name = config.get("pipeline_config_db")
        schema_name = config.get("pipeline_config_sch")
        table_name = config.get("pipeline_tracking_tbl")

        if not all([db_name, schema_name, table_name]):
            err_msg = (
                "Missing one or more config keys: 'pipeline_config_db', "
                "'pipeline_config_sch', or 'pipeline_tracking_tbl'."
            )
            logger.error(err_msg, tag=log_tag)
            return {"continue_dag_run": False, "error": {"message": err_msg, "source": log_tag}}

        fully_qualified_name = f"{db_name}.{schema_name}.{table_name}"
        logger.info(f"Attempting to create tracking table '{fully_qualified_name}'...", tag=log_tag)

        create_sql = f"""
                        CREATE TABLE IF NOT EXISTS {fully_qualified_name} (
                            TRACKING_ID VARCHAR PRIMARY KEY,
                            REQUEST_ID VARCHAR NOT NULL,
                            PIPELINE_ID VARCHAR NOT NULL,
                            PIPELINE_CURRENT_STATUS VARCHAR,
                            PIPELINE_START_TIMESTAMP TIMESTAMP_TZ,
                            PIPELINE_END_TIMESTAMP TIMESTAMP_TZ,
                            WINDOW_START_TIME TIMESTAMP_TZ,
                            WINDOW_END_TIME TIMESTAMP_TZ,
                            WINDOW_INTERVAL_DURATION VARCHAR,
                            TARGET_DAY DATE,
                            PHASE_DETAILS VARIANT,
                            AUDIT_DETAILS VARIANT,
                            RECORD_FIRST_INSERTED TIMESTAMP_TZ,
                            RECORD_LAST_UPDATED TIMESTAMP_TZ
                        )
        """
        
        return _execute_create_table(
            create_sql, fully_qualified_name, db_name, schema_name, config, logger, log_tag
        )

    except Exception as e:
        
        err_msg = f"Unexpected error in create_tracking_table setup: {e}"
        logger.error(err_msg, tag=log_tag, details={"error_type": type(e).__name__})
        return {"continue_dag_run": False, "error": {"message": err_msg, "source": log_tag}}


@trace(logger_attr_name="logger")
def create_backfill_table(config: Dict[str, Any], logger: CustomLogger) -> Dict[str, Any]:
    """
    Creates the 'pipeline_backfill_tbl' in Snowflake if it doesn't already exist.
    """
    log_tag = "create_backfill_table"
    
    try:
        db_name = config.get("pipeline_config_db")
        schema_name = config.get("pipeline_config_sch")
        table_name = config.get("pipeline_backfill_tbl")

        if not all([db_name, schema_name, table_name]):
            err_msg = (
                "Missing one or more config keys: 'pipeline_config_db', "
                "'pipeline_config_sch', or 'pipeline_backfill_tbl'."
            )
            logger.error(err_msg, tag=log_tag)
            return {"continue_dag_run": False, "error": {"message": err_msg, "source": log_tag}}

        fully_qualified_name = f"{db_name}.{schema_name}.{table_name}"
        logger.info(f"Attempting to create backfill table '{fully_qualified_name}'...", tag=log_tag)

        create_sql = f"""
                        CREATE TABLE IF NOT EXISTS {fully_qualified_name} (
                            REQUEST_ID VARCHAR PRIMARY KEY,
                            PIPELINE_ID VARCHAR NOT NULL,
                            REQUESTED_BY VARCHAR,                            
                            REQUEST_STATUS VARCHAR,
                            REQUEST_WINDOW_START_TIME TIMESTAMP_TZ,
                            REQUEST_WINDOW_END_TIME TIMESTAMP_TZ,
                            REQUEST_WINDOW_INTERVAL_DURATION VARCHAR,
                            DATA_LOAD_TYPE VARCHAR,
                            RECORD_FIRST_INSERTED TIMESTAMP_TZ,
                            RECORD_LAST_UPDATED TIMESTAMP_TZ,
                            REQUEST_ACCEPTED_TIMESTAMP TIMESTAMP_TZ,
                            REQUEST_COMPLETED_TIMESTAMP TIMESTAMP_TZ,
                            REQUEST_COMPLETION_DURATION VARCHAR,
                            REQUEST_COMPLETION_ALERT_SENT BOOLEAN,
                            STOP_BACK_FILL BOOLEAN DEFAULT FALSE,
                            TOTAL_RECORDS_TRANSFERRED INT
                        )
        """
        
        return _execute_create_table(
            create_sql, fully_qualified_name, db_name, schema_name, config, logger, log_tag
        )

    except Exception as e:
        # Catch-all for unexpected errors
        err_msg = f"Unexpected error in create_backfill_table setup: {e}"
        logger.error(err_msg, tag=log_tag, details={"error_type": type(e).__name__})
        return {"continue_dag_run": False, "error": {"message": err_msg, "source": log_tag}}
















