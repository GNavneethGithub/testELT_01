import datetime
import pytz
from typing import Any, Dict, Optional

# We need to import the logger for the functions that use it
from framework.utils.custom_logger import CustomLogger, trace

# --- Timezone Helper ---

def get_timezone(logger: CustomLogger, config: dict) -> pytz.BaseTzInfo:
    """
    Safely gets the timezone object from the config.
    Defaults to UTC if the key is missing or the timezone is invalid.
    """
    TAG = "GET_TIMEZONE"
    try:
        tz_str = config["timezone"]
        tz = pytz.timezone(tz_str)
        return tz
    except KeyError:
        logger.warning(
            "[WARNING] 'timezone' key not found in config. Defaulting to UTC.", 
            tag=TAG
        )
        return pytz.utc
    except pytz.exceptions.UnknownTimeZoneError:
        logger.warning(
            f"[WARNING] Unknown timezone '{config.get('timezone')}' in config. Defaulting to UTC.", 
            tag=TAG
        )
        return pytz.utc

# --- Duration Helper ---

def format_duration(duration_sec: Optional[float]) -> str:
    """
    Converts a duration in seconds (float) to a human-readable string 
    like '1d2h30m40s33ms'. Omits zero-value components.
    """
    if duration_sec is None:
        return ""
    if duration_sec == 0:
        return "0s"

    DAY_IN_SECONDS = 86400
    HOUR_IN_SECONDS = 3600
    MINUTE_IN_SECONDS = 60
    parts = []
    
    seconds_int = int(duration_sec)
    milliseconds = int(round((duration_sec - seconds_int) * 1000))
    
    days = seconds_int // DAY_IN_SECONDS
    remaining_sec = seconds_int % DAY_IN_SECONDS
    
    hours = remaining_sec // HOUR_IN_SECONDS
    remaining_sec = remaining_sec % HOUR_IN_SECONDS
    
    minutes = remaining_sec // MINUTE_IN_SECONDS
    seconds = remaining_sec % MINUTE_IN_SECONDS
    
    if days > 0:
        parts.append(f"{days}d")
    if hours > 0:
        parts.append(f"{hours}h")
    if minutes > 0:
        parts.append(f"{minutes}m")
    if seconds > 0:
        parts.append(f"{seconds}s")
    if milliseconds > 0:
        parts.append(f"{milliseconds}ms")
    
    return "".join(parts) if parts else "0s"

# --- State Manager Helper ---

def update_record_values(record: dict, details: dict) -> Dict[str, Any]:
    """
    Merges the key-value pairs from the 'details' dictionary into
    the 'record' dictionary (adds new keys or overwrites existing ones).
    """
    record.update(details)
    return record

# --- Notification Helper ---

@trace(logger_attr_name="logger")
def send_mail_alert(logger: CustomLogger, config: dict, record: dict, error_dict: dict):
    """
    (Placeholder) Sends an email alert with the provided error details.
    """
    logger.info(f"[RUNNING] Sending immediate alert for errors: {error_dict}", tag="ALERT")
    # Real email logic would go here
    logger.info("[COMPLETED] Alert sent.", tag="ALERT")
    pass