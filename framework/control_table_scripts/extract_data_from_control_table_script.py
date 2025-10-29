
# framework/control_table_scripts/extract_data_from_control_table_script.py
import json
import snowflake.connector
from snowflake.connector.cursor import DictCursor
from typing import Dict, Any, List
from framework.utils.custom_logger import CustomLogger, trace
from framework.utils.snowflake_connector import get_snowflake_connection

@trace(logger_attr_name="logger")
def get_pending_tracking_records(
    config: Dict[str, Any], 
    logger: CustomLogger
) -> Dict[str, Any]:
    """
    Collects N records from the tracking table for a specific PIPELINE_ID
    where the PIPELINE_CURRENT_STATUS is 'PENDING'.

    Parameters:
        config (dict): The configuration dictionary. Expected to contain:
                       - Snowflake connection params
                       - 'pipeline_tracking_db' (str)
                       - 'pipeline_tracking_sch' (str)
                       - 'pipeline_tracking_tbl' (str)
                       - 'PIPELINE_ID' (str): The specific pipeline ID to query for.
                       - 'record_limit' (int): The (N) max number of records to return.
        logger (CustomLogger): An instance of the custom logger.

    Returns:
        dict: A dictionary containing:
              - 'continue_dag_run' (bool): True on success, False on failure.
              - 'error' (dict | None): Error details on failure, else None.
              - 'data' (List[Dict[str, Any]] | None): A list of dictionaries (rows) on success.
    """
    log_tag = "get_pending_tracking_records"
    
    return_val = {
        "continue_dag_run": False,
        "error": None,
        "data": None
    }
    
    conn = None
    query_id = None
    
    try:
        db_name = config.get("pipeline_tracking_db")
        schema_name = config.get("pipeline_tracking_sch")
        table_name = config.get("pipeline_tracking_tbl")
        PIPELINE_ID = config.get("PIPELINE_ID")
        record_limit = config.get("record_limit")

        if not all([db_name, schema_name, table_name, PIPELINE_ID, record_limit is not None]):
            err_msg = (
                "Missing one or more config keys: 'pipeline_tracking_db', "
                "'pipeline_tracking_sch', 'pipeline_tracking_tbl', 'PIPELINE_ID', or 'record_limit'."
            )
            logger.error(err_msg, tag=log_tag)
            return_val["error"] = {"message": err_msg, "source": log_tag}
            return return_val

        fully_qualified_name = f"{db_name}.{schema_name}.{table_name}"
        logger.info(f"Querying {fully_qualified_name} for pending records...", tag=log_tag)

        conn_result = get_snowflake_connection(config, logger)
        conn = conn_result.get('conn')
        if conn_result.get('error'):
            logger.error(
                f"Failed to connect to Snowflake. Cannot query {fully_qualified_name}.",
                tag=log_tag,
                details=conn_result['error']
            )
            return_val["error"] = conn_result['error']
            return return_val

        # Use DictCursor to get results as dictionaries
        with conn.cursor(DictCursor) as cursor:
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
            
            # Use named parameters (%(key)s) in the SQL
            select_sql = f"""
            SELECT * FROM {fully_qualified_name}
            WHERE PIPELINE_ID = %(pid)s
            AND PIPELINE_CURRENT_STATUS = %(status)s
            LIMIT %(limit)s
            """
            parms = {
                "pid": PIPELINE_ID,
                "status": "PENDING",
                "limit": record_limit
            }

            # printing the SQL script here 
            print_select_sql = select_sql.replace("%(pid)s", f"'{parms['pid']}'")\
                                        .replace("%(status)s", f"'{parms['status']}'")\
                                        .replace("%(limit)s", str(parms['limit'])) 
            print(print_select_sql)

            
            logger.info(
                f"Executing SELECT query on {fully_qualified_name}...", 
                tag=log_tag,
                details=parms
            )
            
            cursor.execute(select_sql, parms)
            query_id = cursor.sfqid
            
            results = cursor.fetchall()
        
        logger.info(
            f"Successfully fetched {len(results)} records.",
            tag=log_tag,
            details={"query_id": query_id}
        )
        
        return_val["continue_dag_run"] = True
        return_val["data"] = results
        return return_val
        
    except snowflake.connector.Error as e:
        error_details = {
            "message": e.msg,
            "errno": e.errno,
            "sqlstate": e.sqlstate,
            "sfqid": e.sfqid
        }
        logger.error(
            "A Snowflake error occurred while querying.",
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
def get_completed_records_by_target_day(
    config: Dict[str, Any], 
    logger: CustomLogger
) -> Dict[str, Any]:
    """
    Collects a summary for a given TARGET_DAY where the 
    PIPELINE_CURRENT_STATUS is 'COMPLETED'.
    
    It returns the COUNT(*) and the MAX(WINDOW_END_TIME) for that set of records.

    Parameters:
        config (dict): The configuration dictionary. Expected to contain:
                       - Snowflake connection params
                       - 'pipeline_tracking_db' (str)
                       - 'pipeline_tracking_sch' (str)
                       - 'pipeline_tracking_tbl' (str)
                       - 'TARGET_DAY' (str or date): The specific day to query for.
        logger (CustomLogger): An instance of the custom logger.

    Returns:
        dict: A dictionary containing:
              - 'continue_dag_run' (bool): True on success, False on failure.
              - 'error' (dict | None): Error details on failure, else None.
              - 'data' (List[Dict[str, Any]] | None): A list containing a single
                dictionary with 'COMPLETED_RECORD_COUNT' and 'MAX_WINDOW_END_TIME'.
              - 'max_window_end_time' (datetime | None): The max window end time.
    """
    log_tag = "get_completed_records_by_target_day"
    
    return_val = {
        "continue_dag_run": False,
        "error": None,
        "data": None,
        "max_window_end_time": None
    }
    
    conn = None
    query_id = None
    
    try:
        db_name = config.get("pipeline_tracking_db")
        schema_name = config.get("pipeline_tracking_sch")
        table_name = config.get("pipeline_tracking_tbl")
        target_day = config.get("TARGET_DAY")

        if not all([db_name, schema_name, table_name, target_day]):
            err_msg = (
                "Missing one or more config keys: 'pipeline_tracking_db', "
                "'pipeline_tracking_sch', 'pipeline_tracking_tbl', or 'TARGET_DAY'."
            )
            logger.error(err_msg, tag=log_tag)
            return_val["error"] = {"message": err_msg, "source": log_tag}
            return return_val

        fully_qualified_name = f"{db_name}.{schema_name}.{table_name}"
        logger.info(f"Querying {fully_qualified_name} for COMPLETED summary...", tag=log_tag)

        conn_result = get_snowflake_connection(config, logger)
        conn = conn_result.get('conn')
        if conn_result.get('error'):
            logger.error(
                f"Failed to connect to Snowflake. Cannot query {fully_qualified_name}.",
                tag=log_tag,
                details=conn_result['error']
            )
            return_val["error"] = conn_result['error']
            return return_val

        with conn.cursor(DictCursor) as cursor:
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
            
            # Using the new aliases
            select_sql = f"""
            SELECT
                COUNT(*) as COMPLETED_RECORD_COUNT,
                MAX(WINDOW_END_TIME) as MAX_WINDOW_END_TIME
            FROM {fully_qualified_name}
            WHERE PIPELINE_CURRENT_STATUS = %(status)s
            AND TARGET_DAY = %(tday)s
            """
            
            parms = {
                "status": "COMPLETED",
                "tday": target_day
            }

            print_select_sql = select_sql.replace("%(status)s", f"'{parms['status']}'")\
                                         .replace("%(tday)s", f"'{parms['tday']}'")
            print(print_select_sql)
            
            logger.info(
                f"Executing summary query on {fully_qualified_name}...", 
                tag=log_tag,
                details=parms
            )
            
            cursor.execute(select_sql, parms)
            query_id = cursor.sfqid
            
            results = cursor.fetchall()
            
        max_time_found = None
        record_count = 0

        if results and results[0]:
            # Use new alias to fetch count
            record_count = results[0].get('COMPLETED_RECORD_COUNT', 0)
            # Use new alias to fetch max time
            max_time_found = results[0].get('MAX_WINDOW_END_TIME')
        
        logger.info(
            f"Successfully fetched summary. Found {record_count} completed records.",
            tag=log_tag,
            details={
                "query_id": query_id,
                "max_window_end_time_found": max_time_found,
                "completed_record_count": record_count
            }
        )
        
        return_val["continue_dag_run"] = True
        return_val["data"] = results
        return_val["max_window_end_time"] = max_time_found
        return return_val
        
    except snowflake.connector.Error as e:
        error_details = {
            "message": e.msg,
            "errno": e.errno,
            "sqlstate": e.sqlstate,
            "sfqid": e.sfqid
        }
        logger.error(
            "A Snowflake error occurred while querying.",
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
def get_phase_names_by_tracking_id(
    config: Dict[str, Any], 
    logger: CustomLogger
) -> Dict[str, Any]:
    """
    Collects the PHASE_DETAILS column for a specific TRACKING_ID,
    parses it in Python, and returns the names of phases.

    Parameters:
        config (dict): The configuration dictionary. Expected to contain:
                       - Snowflake connection params
                       - 'pipeline_tracking_db' (str)
                       - 'pipeline_tracking_sch' (str)
                       - 'pipeline_tracking_tbl' (str)
                       - 'TRACKING_ID' (str): The specific tracking ID to query for.
        logger (CustomLogger): An instance of the custom logger.

    Returns:
        dict: A dictionary containing:
              - 'continue_dag_run' (bool): True on success, False on failure.
              - 'error' (dict | None): Error details on failure, else None.
              - 'data' (List[Dict[str, Any]] | None): A list containing a single
                dictionary with 'COMPLETED_PHASES', 'RUNNING_PHASES', 
                and 'PENDING_PHASES' arrays.
    """
    log_tag = "get_phase_names_by_tracking_id"
    
    return_val = {
        "continue_dag_run": False,
        "error": None,
        "data": None
    }
    
    conn = None
    query_id = None
    
    try:
        db_name = config.get("pipeline_tracking_db")
        schema_name = config.get("pipeline_tracking_sch")
        table_name = config.get("pipeline_tracking_tbl")
        tracking_id = config.get("TRACKING_ID")

        if not all([db_name, schema_name, table_name, tracking_id]):
            err_msg = (
                "Missing one or more config keys: 'pipeline_tracking_db', "
                "'pipeline_tracking_sch', 'pipeline_tracking_tbl', or 'TRACKING_ID'."
            )
            logger.error(err_msg, tag=log_tag)
            return_val["error"] = {"message": err_msg, "source": log_tag}
            return return_val

        fully_qualified_name = f"{db_name}.{schema_name}.{table_name}"
        logger.info(f"Querying {fully_qualified_name} for phase details...", tag=log_tag)

        conn_result = get_snowflake_connection(config, logger)
        conn = conn_result.get('conn')
        if conn_result.get('error'):
            logger.error(
                f"Failed to connect to Snowflake. Cannot query {fully_qualified_name}.",
                tag=log_tag,
                details=conn_result['error']
            )
            return_val["error"] = conn_result['error']
            return return_val

        with conn.cursor(DictCursor) as cursor:
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
            
            # Fetch the raw PHASE_DETAILS column
            select_sql = f"""
            SELECT
                PHASE_DETAILS
            FROM {fully_qualified_name}
            WHERE TRACKING_ID = %(tid)s
            """
            
            parms = {
                "tid": tracking_id
            }

            print_select_sql = select_sql.replace("%(tid)s", f"'{parms['tid']}'")
            print(print_select_sql)
            
            logger.info(
                f"Executing phase details query on {fully_qualified_name}...", 
                tag=log_tag,
                details=parms
            )
            
            cursor.execute(select_sql, parms)
            query_id = cursor.sfqid
            
            results = cursor.fetchall()
            
        # --- Python JSON Parsing Logic ---
        completed_phases = []
        running_phases = []
        pending_phases = []
        
        if results and results[0]:
            phase_details_str = results[0].get('PHASE_DETAILS')
            if phase_details_str:
                logger.info("Parsing PHASE_DETAILS JSON in Python.", tag=log_tag)
                # Parse the JSON string
                phase_data = json.loads(phase_details_str)
                
                # Use list comprehensions and .get() for safety
                completed_phases = [
                    p.get('name') for p in phase_data.get('completed', []) if p.get('name')
                ]
                running_phases = [
                    p.get('name') for p in phase_data.get('running', []) if p.get('name')
                ]
                pending_phases = [
                    p.get('name') for p in phase_data.get('pending', []) if p.get('name')
                ]
            else:
                logger.warning("PHASE_DETAILS column was empty or NULL.", tag=log_tag)
        else:
             logger.warning(f"No record found for TRACKING_ID {tracking_id}.", tag=log_tag)
        
        phase_name_data = {
            'COMPLETED_PHASES': completed_phases,
            'RUNNING_PHASES': running_phases,
            'PENDING_PHASES': pending_phases
        }

        logger.info(
            f"Successfully parsed phase names for TRACKING_ID {tracking_id}.",
            tag=log_tag,
            details={
                "query_id": query_id,
                "completed": completed_phases,
                "running": running_phases,
                "pending": pending_phases
            }
        )
        
        return_val["continue_dag_run"] = True
        # Return the data in the same list[dict] format
        return_val["data"] = [phase_name_data]
        return return_val
        
    except json.JSONDecodeError as e:
        error_details = {
            "message": f"Failed to parse PHASE_DETAILS JSON: {e}",
            "error_type": type(e).__name__
        }
        logger.error(error_details["message"], tag=log_tag)
        return_val["error"] = error_details
        return return_val

    except snowflake.connector.Error as e:
        error_details = {
            "message": e.msg,
            "errno": e.errno,
            "sqlstate": e.sqlstate,
            "sfqid": e.sfqid
        }
        logger.error(
            "A Snowflake error occurred while querying.",
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











