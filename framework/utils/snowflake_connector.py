import snowflake.connector
from typing import Dict, Any, Optional
from framework.utils.custom_logger import CustomLogger  

def get_snowflake_connection(
    sf_con_parms: Dict[str, Any], 
    logger: CustomLogger
) -> Dict[str, Any]:
    """
    Parameters:
        sf_con_parms (dict): A dictionary *required* to have:
                             'account', 'username', 'password'.
        logger (CustomLogger): An instance of your custom logger.
    Returns:
        dict: A dictionary with two keys:
              - 'conn': The connection object on success, or None on failure.
              - 'error': None on success, or a dictionary of error details on failure.
    """
    
    log_tag = "snowflake_connect"

    # Log the attempt
    log_details = {
        "account": sf_con_parms.get("account"),
        "user": sf_con_parms.get("username")
    }
    logger.info("Attempting Snowflake connection...", tag=log_tag, details=log_details)

    try:
        conn = snowflake.connector.connect(
            user=sf_con_parms.get("username"),
            password=sf_con_parms.get("password"),
            account=sf_con_parms.get("account")
        )
        
        # --- SUCCESS ---
        logger.info("Snowflake connection successful.", tag=log_tag)
        return {
            'conn': conn,
            'error': None
        }
        
    except snowflake.connector.Error as e:
        # --- FAILED ---
        error_details = {
            "message": e.msg,
            "errno": e.errno,
            "sqlstate": e.sqlstate,
            "sfqid": e.sfqid,
            "raw_msg": e.raw_msg
        }
        
        # Log the detailed error
        logger.error(
            "Snowflake connection failed.",
            tag=log_tag,
            details=error_details
        )
        
        # Return the error dictionary
        return {
            'conn': None,
            'error': error_details
        }


