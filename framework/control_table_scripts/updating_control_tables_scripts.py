# framework/control_table_scripts/updating_control_tables_scripts.py

import json
import snowflake.connector
from snowflake.connector.cursor import DictCursor
from typing import Dict, Any, List
from framework.utils.custom_logger import CustomLogger, trace
from framework.utils.snowflake_connector import get_snowflake_connection

@trace(logger_attr_name="logger")
def update_single_phase(
    config: Dict[str, Any], 
    logger: CustomLogger, 
    tracking_id: str, 
    phase_status: str,
    phase_object: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Updates a single phase's status within the PHASE_DETAILS column for a
    specific TRACKING_ID.

    It places the phase_object into the list specified by phase_status and
    ensures it is removed from the other status lists (e.g., 'running', 'pending').
    """
    log_tag = "update_single_phase"
    
    return_val = {
        "continue_dag_run": False,
        "error": None
    }
    
    conn = None
    query_id = None
    valid_statuses = ['completed', 'running', 'pending']
    
    try:
        # --- 1. Input Validation ---
        db_name = config.get("pipeline_tracking_db")
        schema_name = config.get("pipeline_tracking_sch")
        table_name = config.get("pipeline_tracking_tbl")
        phase_name = phase_object.get("name")

        if not all([db_name, schema_name, table_name, tracking_id, phase_name]):
            err_msg = (
                "Missing one or more required keys/inputs: 'pipeline_tracking_db', "
                "'pipeline_tracking_sch', 'pipeline_tracking_tbl', 'tracking_id', "
                "or the 'name' key in the phase_object."
            )
            logger.error(err_msg, tag=log_tag)
            return_val["error"] = {"message": err_msg, "source": log_tag}
            return return_val
        
        if phase_status not in valid_statuses:
            err_msg = f"Invalid 'phase_status': {phase_status}. Must be one of {valid_statuses}."
            logger.error(err_msg, tag=log_tag)
            return_val["error"] = {"message": err_msg, "source": log_tag}
            return return_val

        fully_qualified_name = f"{db_name}.{schema_name}.{table_name}"
        logger.info(f"Begin phase update for {fully_qualified_name}...", tag=log_tag)

        # --- 2. Get Connection and Start Transaction ---
        conn_result = get_snowflake_connection(config, logger)
        conn = conn_result.get('conn')
        if conn_result.get('error'):
            logger.error(f"Failed to connect to Snowflake.", tag=log_tag, details=conn_result['error'])
            return_val["error"] = conn_result['error']
            return return_val
        
        conn.autocommit(False)
        logger.info("Transaction started. Autocommit OFF.", tag=log_tag)

        with conn.cursor(DictCursor) as cursor:
            cursor.execute(f"USE DATABASE {db_name}")
            cursor.execute(f"USE SCHEMA {schema_name}")
            
            # --- 3. Read Current Data (No Lock) ---
            select_sql = f"""
            SELECT PHASE_DETAILS FROM {fully_qualified_name}
            WHERE TRACKING_ID = %(tid)s
            """
            parms = {"tid": tracking_id}
            
            logger.info(f"Fetching row for TRACKING_ID {tracking_id}", tag=log_tag)
            cursor.execute(select_sql, parms)
            results = cursor.fetchall()

            if not results:
                raise Exception(f"No record found with TRACKING_ID {tracking_id}")

            current_phase_str = results[0].get('PHASE_DETAILS')
            current_data = json.loads(current_phase_str) if current_phase_str else {}
            
            # --- 4. Python: Modify Data ---
            final_data = {}
            for status in valid_statuses:
                current_list = current_data.get(status, [])
                cleaned_list = [p for p in current_list if p.get('name') != phase_name]
                
                if status == phase_status:
                    final_data[status] = cleaned_list + [phase_object]
                else:
                    final_data[status] = cleaned_list
            
            final_data_str = json.dumps(final_data)
            logger.info(f"Moving phase '{phase_name}' to '{phase_status}'.", tag=log_tag)

            # --- 5. Write Updated Data ---
            update_sql = f"""
            UPDATE {fully_qualified_name}
            SET PHASE_DETAILS = PARSE_JSON(%(details)s)
            WHERE TRACKING_ID = %(tid)s
            """
            update_parms = {"details": final_data_str, "tid": tracking_id}

            print_sql = update_sql.replace("%(tid)s", f"'{tracking_id}'")\
                                  .replace("%(details)s", "PARSE_JSON('...see logs for details...')")
            print(print_sql)

            logger.info(f"Executing UPDATE on {fully_qualified_name}...", tag=log_tag)
            cursor.execute(update_sql, update_parms)
            query_id = cursor.sfqid
            
        # --- 6. Commit Transaction ---
        logger.info("Committing transaction...", tag=log_tag, details={"query_id": query_id})
        conn.commit()
        
        logger.info(f"Successfully updated phase details for {tracking_id}.", tag=log_tag)
        return_val["continue_dag_run"] = True
        return return_val
        
    except json.JSONDecodeError as e:
        if conn: conn.rollback()
        error_details = {"message": f"Failed to parse PHASE_DETAILS JSON: {e}", "error_type": type(e).__name__}
        logger.error(error_details["message"], tag=log_tag)
        return_val["error"] = error_details
        return return_val
        
    except snowflake.connector.Error as e:
        if conn: conn.rollback()
        error_details = {"message": e.msg, "errno": e.errno, "sqlstate": e.sqlstate, "sfqid": e.sfqid}
        logger.error("A Snowflake error occurred. Transaction rolled back.", tag=log_tag, details=error_details)
        return_val["error"] = error_details
        return return_val
        
    except Exception as e:
        if conn: conn.rollback()
        error_details = {"message": f"A non-Snowflake error occurred: {e}", "error_type": type(e).__name__}
        logger.error("An unexpected error occurred. Transaction rolled back.", tag=log_tag, details=error_details)
        return_val["error"] = error_details
        return return_val
        
    finally:
        if conn:
            conn.autocommit(True)
            conn.close()
            logger.info("Snowflake connection closed. Autocommit reset to ON.", tag=log_tag)


@trace(logger_attr_name="logger")
def reset_tracking_columns_to_null(
    config: Dict[str, Any], 
    logger: CustomLogger, 
    tracking_id: str
) -> Dict[str, Any]:
    """
    Sets specific tracking columns to NULL for a given TRACKING_ID.

    The columns reset are:
    - PIPELINE_CURRENT_STATUS
    - PIPELINE_START_TIMESTAMP
    - PIPELINE_END_TIMESTAMP
    - PHASE_DETAILS
    - AUDIT_DETAILS

    Parameters:
        config (dict): The configuration dictionary.
        logger (CustomLogger): An instance of the custom logger.
        tracking_id (str): The specific TRACKING_ID to update.

    Returns:
        dict: A dictionary containing:
              - 'continue_dag_run' (bool): True on success, False on failure.
              - 'error' (dict | None): Error details on failure, else None.
    """
    log_tag = "reset_tracking_columns_to_null"
    
    return_val = {
        "continue_dag_run": False,
        "error": None
    }
    
    conn = None
    query_id = None
    
    try:
        # --- 1. Input Validation ---
        db_name = config.get("pipeline_tracking_db")
        schema_name = config.get("pipeline_tracking_sch")
        table_name = config.get("pipeline_tracking_tbl")

        if not all([db_name, schema_name, table_name, tracking_id]):
            err_msg = (
                "Missing one or more required keys/inputs: 'pipeline_tracking_db', "
                "'pipeline_tracking_sch', 'pipeline_tracking_tbl', or 'tracking_id'."
            )
            logger.error(err_msg, tag=log_tag)
            return_val["error"] = {"message": err_msg, "source": log_tag}
            return return_val

        fully_qualified_name = f"{db_name}.{schema_name}.{table_name}"
        logger.info(f"Resetting columns for {tracking_id} in {fully_qualified_name}...", tag=log_tag)

        # --- 2. Get Connection ---
        conn_result = get_snowflake_connection(config, logger)
        conn = conn_result.get('conn')
        if conn_result.get('error'):
            logger.error(f"Failed to connect to Snowflake.", tag=log_tag, details=conn_result['error'])
            return_val["error"] = conn_result['error']
            return return_val
        
        with conn.cursor(DictCursor) as cursor:
            cursor.execute(f"USE DATABASE {db_name}")
            cursor.execute(f"USE SCHEMA {schema_name}")
            
            # --- 3. Define SQL ---
            update_sql = f"""
            UPDATE {fully_qualified_name}
            SET
                PIPELINE_CURRENT_STATUS = NULL,
                PIPELINE_START_TIMESTAMP = NULL,
                PIPELINE_END_TIMESTAMP = NULL,
                PHASE_DETAILS = NULL,
                AUDIT_DETAILS = NULL
            WHERE TRACKING_ID = %(tid)s
            """
            
            parms = {"tid": tracking_id}

            # --- 4. Print SQL ---
            print_sql = update_sql.replace("%(tid)s", f"'{tracking_id}'")
            print(print_sql)

            # --- 5. Execute ---
            logger.info(f"Executing UPDATE to reset columns for {tracking_id}...", tag=log_tag)
            cursor.execute(update_sql, parms)
            query_id = cursor.sfqid
            rows_updated = cursor.rowcount
            
            if rows_updated == 0:
                logger.warning(
                    f"No row found with TRACKING_ID {tracking_id}. No update performed.",
                    tag=log_tag,
                    details={"query_id": query_id}
                )
            else:
                logger.info(
                    f"Successfully reset columns for {rows_updated} row(s).",
                    tag=log_tag,
                    details={"query_id": query_id}
                )

        # --- 6. Success ---
        return_val["continue_dag_run"] = True
        return return_val
        
    except snowflake.connector.Error as e:
        error_details = {"message": e.msg, "errno": e.errno, "sqlstate": e.sqlstate, "sfqid": e.sfqid}
        logger.error("A Snowflake error occurred.", tag=log_tag, details=error_details)
        return_val["error"] = error_details
        return return_val
        
    except Exception as e:
        error_details = {"message": f"A non-Snowflake error occurred: {e}", "error_type": type(e).__name__}
        logger.error("An unexpected error occurred.", tag=log_tag, details=error_details)
        return_val["error"] = error_details
        return return_val
        
    finally:
        if conn:
            conn.close()
            logger.info("Snowflake connection closed.", tag=log_tag)


@trace(logger_attr_name="logger")
def update_audit_details(
    config: Dict[str, Any], 
    logger: CustomLogger, 
    tracking_id: str, 
    audit_object: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Appends a new audit object to the AUDIT_DETAILS list for a specific
    TRACKING_ID.

    It performs a "read-modify-write" in a transaction to safely
    append to the audit trail.

    Parameters:
        config (dict): The configuration dictionary.
        logger (CustomLogger): An instance of the custom logger.
        tracking_id (str): The specific TRACKING_ID to update.
        audit_object (dict): The new audit dictionary to append to the list.

    Returns:
        dict: A dictionary containing:
              - 'continue_dag_run' (bool): True on success, False on failure.
              - 'error' (dict | None): Error details on failure, else None.
    """
    log_tag = "update_audit_details"
    
    return_val = {
        "continue_dag_run": False,
        "error": None
    }
    
    conn = None
    query_id = None
    
    try:
        # --- 1. Input Validation ---
        db_name = config.get("pipeline_tracking_db")
        schema_name = config.get("pipeline_tracking_sch")
        table_name = config.get("pipeline_tracking_tbl")

        if not all([db_name, schema_name, table_name, tracking_id, audit_object]):
            err_msg = (
                "Missing one or more required keys/inputs: 'pipeline_tracking_db', "
                "'pipeline_tracking_sch', 'pipeline_tracking_tbl', 'tracking_id', or 'audit_object'."
            )
            logger.error(err_msg, tag=log_tag)
            return_val["error"] = {"message": err_msg, "source": log_tag}
            return return_val

        fully_qualified_name = f"{db_name}.{schema_name}.{table_name}"
        logger.info(f"Begin audit update for {fully_qualified_name}...", tag=log_tag)

        # --- 2. Get Connection and Start Transaction ---
        conn_result = get_snowflake_connection(config, logger)
        conn = conn_result.get('conn')
        if conn_result.get('error'):
            logger.error(f"Failed to connect to Snowflake.", tag=log_tag, details=conn_result['error'])
            return_val["error"] = conn_result['error']
            return return_val
        
        conn.autocommit(False)
        logger.info("Transaction started. Autocommit OFF.", tag=log_tag)

        with conn.cursor(DictCursor) as cursor:
            cursor.execute(f"USE DATABASE {db_name}")
            cursor.execute(f"USE SCHEMA {schema_name}")
            
            # --- 3. Read Current Data ---
            select_sql = f"""
            SELECT AUDIT_DETAILS FROM {fully_qualified_name}
            WHERE TRACKING_ID = %(tid)s
            """
            parms = {"tid": tracking_id}
            
            logger.info(f"Fetching row for TRACKING_ID {tracking_id}", tag=log_tag)
            cursor.execute(select_sql, parms)
            results = cursor.fetchall()

            if not results:
                raise Exception(f"No record found with TRACKING_ID {tracking_id}")

            current_audit_str = results[0].get('AUDIT_DETAILS')
            
            # Assume AUDIT_DETAILS is a list, or start a new one
            current_data = json.loads(current_audit_str) if current_audit_str else []
            
            if not isinstance(current_data, list):
                logger.error(
                    "Existing AUDIT_DETAILS is not a list. Cannot append.",
                    tag=log_tag,
                    details={"type_found": str(type(current_data))}
                )
                raise Exception("AUDIT_DETAILS data corruption: expected a list.")

            # --- 4. Python: Modify Data (Append) ---
            current_data.append(audit_object)
            final_data_str = json.dumps(current_data)
            logger.info("Appending new object to AUDIT_DETAILS.", tag=log_tag)

            # --- 5. Write Updated Data ---
            update_sql = f"""
            UPDATE {fully_qualified_name}
            SET AUDIT_DETAILS = PARSE_JSON(%(details)s)
            WHERE TRACKING_ID = %(tid)s
            """
            update_parms = {"details": final_data_str, "tid": tracking_id}

            print_sql = update_sql.replace("%(tid)s", f"'{tracking_id}'")\
                                  .replace("%(details)s", "PARSE_JSON('...see logs for details...')")
            print(print_sql)

            logger.info(f"Executing UPDATE on {fully_qualified_name}...", tag=log_tag)
            cursor.execute(update_sql, update_parms)
            query_id = cursor.sfqid
            
        # --- 6. Commit Transaction ---
        logger.info("Committing transaction...", tag=log_tag, details={"query_id": query_id})
        conn.commit()
        
        logger.info(f"Successfully updated audit details for {tracking_id}.", tag=log_tag)
        return_val["continue_dag_run"] = True
        return return_val
        
    except json.JSONDecodeError as e:
        if conn: conn.rollback()
        error_details = {"message": f"Failed to parse AUDIT_DETAILS JSON: {e}", "error_type": type(e).__name__}
        logger.error(error_details["message"], tag=log_tag)
        return_val["error"] = error_details
        return return_val
        
    except snowflake.connector.Error as e:
        if conn: conn.rollback()
        error_details = {"message": e.msg, "errno": e.errno, "sqlstate": e.sqlstate, "sfqid": e.sfqid}
        logger.error("A Snowflake error occurred. Transaction rolled back.", tag=log_tag, details=error_details)
        return_val["error"] = error_details
        return return_val
        
    except Exception as e:
        if conn: conn.rollback()
        error_details = {"message": f"A non-Snowflake error occurred: {e}", "error_type": type(e).__name__}
        logger.error("An unexpected error occurred. Transaction rolled back.", tag=log_tag, details=error_details)
        return_val["error"] = error_details
        return return_val
        
    finally:
        if conn:
            conn.autocommit(True)
            conn.close()
            logger.info("Snowflake connection closed. Autocommit reset to ON.", tag=log_tag)


@trace(logger_attr_name="logger")
def get_backfill_records_by_status(
    config: Dict[str, Any], 
    logger: CustomLogger, 
    request_status: str
) -> Dict[str, Any]:
    """
    Collects all records from the backfill table for a specific PIPELINE_ID
    and a given REQUEST_STATUS.

    Parameters:
        config (dict): The configuration dictionary. Expected to contain:
                       - Snowflake connection params
                       - 'pipeline_back_fill_request_db' (str)
                       - 'pipeline_back_fill_request_sch' (str)
                       - 'pipeline_backfill_tbl' (str)
                       - 'PIPELINE_ID' (str): The specific pipeline ID to query for.
        logger (CustomLogger): An instance of the custom logger.
        request_status (str): The status to filter by (e.g., 'PENDING', 'RUNNING').

    Returns:
        dict: A dictionary containing:
              - 'continue_dag_run' (bool): True on success, False on failure.
              - 'error' (dict | None): Error details on failure, else None.
              - 'data' (List[Dict[str, Any]] | None): A list of dictionaries (rows) on success.
    """
    log_tag = "get_backfill_records_by_status"
    
    return_val = {
        "continue_dag_run": False,
        "error": None,
        "data": None
    }
    
    conn = None
    query_id = None
    
    try:
        db_name = config.get("pipeline_back_fill_request_db")
        schema_name = config.get("pipeline_back_fill_request_sch")
        table_name = config.get("pipeline_backfill_tbl")
        pipeline_id = config.get("PIPELINE_ID")

        if not all([db_name, schema_name, table_name, pipeline_id, request_status]):
            err_msg = (
                "Missing one or more config keys: 'pipeline_back_fill_request_db', "
                "'pipeline_back_fill_request_sch', 'pipeline_backfill_tbl', 'PIPELINE_ID', or 'request_status' is empty."
            )
            logger.error(err_msg, tag=log_tag)
            return_val["error"] = {"message": err_msg, "source": log_tag}
            return return_val

        fully_qualified_name = f"{db_name}.{schema_name}.{table_name}"
        logger.info(f"Querying {fully_qualified_name} for '{request_status}' records...", tag=log_tag)

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
            
            select_sql = f"""
            SELECT * FROM {fully_qualified_name}
            WHERE PIPELINE_ID = %(pid)s
            AND REQUEST_STATUS = %(status)s
            """
            
            parms = {
                "pid": pipeline_id,
                "status": request_status.upper() # Standardizing to upper case
            }

            print_select_sql = select_sql.replace("%(pid)s", f"'{parms['pid']}'")\
                                         .replace("%(status)s", f"'{parms['status']}'")
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
def update_backfill_record(
    config: Dict[str, Any], 
    logger: CustomLogger, 
    back_fill_details: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Updates a record in the backfill table based on REQUEST_ID and PIPELINE_ID.

    It dynamically builds the SET clause, only updating columns that
    are provided in the 'back_fill_details' dictionary.

    Parameters:
        config (dict): The configuration dictionary.
        logger (CustomLogger): An instance of the custom logger.
        back_fill_details (dict): A dictionary containing 'REQUEST_ID', 
                                  'PIPELINE_ID', and any columns to update.

    Returns:
        dict: A dictionary containing:
              - 'continue_dag_run' (bool): True on success, False on failure.
              - 'error' (dict | None): Error details on failure, else None.
    """
    log_tag = "update_backfill_record"
    
    return_val = {
        "continue_dag_run": False,
        "error": None
    }
    
    conn = None
    query_id = None
    
    # List of columns that are allowed to be updated
    UPDATABLE_COLUMNS = [
        "REQUEST_STATUS",
        "RECORD_LAST_UPDATED",
        "REQUEST_ACCEPTED_TIMESTAMP",
        "REQUEST_COMPLETED_TIMESTAMP",
        "REQUEST_COMPLETION_DURATION",
        "REQUEST_COMPLETION_ALERT_SENT",
        "TOTAL_RECORDS_TRANSFERRED"
    ]
    
    try:
        # --- 1. Input Validation ---
        db_name = config.get("pipeline_tracking_db")
        schema_name = config.get("pipeline_tracking_sch")
        table_name = config.get("pipeline_backfill_tbl")
        
        # Get WHERE clause keys from the details dict
        request_id = back_fill_details.get("REQUEST_ID")
        pipeline_id = back_fill_details.get("PIPELINE_ID")

        if not all([db_name, schema_name, table_name, request_id, pipeline_id]):
            err_msg = (
                "Missing one or more required keys/inputs: 'pipeline_tracking_db', "
                "'pipeline_tracking_sch', 'pipeline_backfill_tbl', "
                "or 'REQUEST_ID'/'PIPELINE_ID' in back_fill_details."
            )
            logger.error(err_msg, tag=log_tag)
            return_val["error"] = {"message": err_msg, "source": log_tag}
            return return_val

        fully_qualified_name = f"{db_name}.{schema_name}.{table_name}"

        # --- 2. Dynamically build SET clause ---
        set_clauses = []
        print_set_clauses = []
        parms = {
            "where_req_id": request_id,
            "where_pipe_id": pipeline_id
        }

        for col in UPDATABLE_COLUMNS:
            if col in back_fill_details:
                val = back_fill_details[col]
                param_name = f"set_{col.lower()}"  # e.g., "set_request_status"
                
                # Add to query parameter list
                set_clauses.append(f"{col} = %({param_name})s")
                parms[param_name] = val
                
                # Add to print string list
                print_val = f"'{val}'" if isinstance(val, str) else str(val)
                print_set_clauses.append(f"{col} = {print_val}")
        
        if not set_clauses:
            logger.warning(
                "No updatable columns found in 'back_fill_details'. No update will be performed.",
                tag=log_tag,
                details={"request_id": request_id}
            )
            return_val["continue_dag_run"] = True
            return return_val

        # --- 3. Get Connection ---
        conn_result = get_snowflake_connection(config, logger)
        conn = conn_result.get('conn')
        if conn_result.get('error'):
            logger.error(f"Failed to connect to Snowflake.", tag=log_tag, details=conn_result['error'])
            return_val["error"] = conn_result['error']
            return return_val
        
        with conn.cursor(DictCursor) as cursor:
            cursor.execute(f"USE DATABASE {db_name}")
            cursor.execute(f"USE SCHEMA {schema_name}")
            
            # --- 4. Finalize and Print SQL ---
            set_sql_str = ", ".join(set_clauses)
            update_sql = f"""
            UPDATE {fully_qualified_name}
            SET {set_sql_str}
            WHERE REQUEST_ID = %(where_req_id)s AND PIPELINE_ID = %(where_pipe_id)s
            """

            print_set_sql_str = ", ".join(print_set_clauses)
            print_sql = f"""
            UPDATE {fully_qualified_name}
            SET {print_set_sql_str}
            WHERE REQUEST_ID = '{request_id}' AND PIPELINE_ID = '{pipeline_id}'
            """
            print(print_sql)

            # --- 5. Execute ---
            logger.info(f"Executing dynamic UPDATE for {request_id}...", tag=log_tag)
            cursor.execute(update_sql, parms)
            query_id = cursor.sfqid
            rows_updated = cursor.rowcount
            
            if rows_updated == 0:
                logger.warning(
                    f"No row found with REQUEST_ID {request_id}. No update performed.",
                    tag=log_tag,
                    details={"query_id": query_id}
                )
            else:
                logger.info(
                    f"Successfully updated {rows_updated} row(s).",
                    tag=log_tag,
                    details={"query_id": query_id}
                )

        # --- 6. Success ---
        return_val["continue_dag_run"] = True
        return return_val
        
    except snowflake.connector.Error as e:
        error_details = {"message": e.msg, "errno": e.errno, "sqlstate": e.sqlstate, "sfqid": e.sfqid}
        logger.error("A Snowflake error occurred.", tag=log_tag, details=error_details)
        return_val["error"] = error_details
        return return_val
        
    except Exception as e:
        error_details = {"message": f"A non-Snowflake error occurred: {e}", "error_type": type(e).__name__}
        logger.error("An unexpected error occurred.", tag=log_tag, details=error_details)
        return_val["error"] = error_details
        return return_val
        
    finally:
        if conn:
            conn.close()
            logger.info("Snowflake connection closed.", tag=log_tag)



















