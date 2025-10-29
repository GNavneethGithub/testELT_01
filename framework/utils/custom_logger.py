# project_01/utils/custom_logger.py

import logging
import datetime
import pytz
from typing import Any, Dict, Optional, List


class CustomLogger:
    def __init__(self, name: str,  level=logging.DEBUG, correlation_id: Optional[str] = None):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter('%(message)s'))
        self.logger.addHandler(handler)

        self.call_stack: List[str] = []
        self.correlation_id = correlation_id or self._generate_correlation_id()

    # ------------------------------------------------------------
    def _generate_correlation_id(self):
        return datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S%f")

    def _get_timezones(self):
        now_utc = datetime.datetime.now(pytz.utc)
        now_pst = now_utc.astimezone(pytz.timezone("America/Los_Angeles"))
        now_ist = now_utc.astimezone(pytz.timezone("Asia/Kolkata"))
        return now_utc, now_pst, now_ist

    def _get_function_path(self):
        return " <-- ".join(reversed(self.call_stack))

    # ------------------------------------------------------------
    def _format_log(
        self,
        level: str,
        message: str,
        tag: Optional[str],
        details: Optional[Dict[str, Any]]
    ) -> str:
        now_utc, now_pst, now_ist = self._get_timezones()
        func_path = self._get_function_path()

        lines = [
            f"[{now_utc.strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]} UTC] "
            f"[{now_pst.strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]} America/Los_Angeles] "
            f"[{now_ist.strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]} IST]",
            f"LEVEL: {level.upper()}",
            f"CALL_PATH: {func_path if func_path else '(root)'}",
            f"TAG: {tag or ''}",
            f"MESSAGE: {message}",
        ]

        if self.correlation_id:
            lines.append(f"CORRELATION_ID: {self.correlation_id}")

        if details:
            lines.append("DETAILS:")
            for k, v in details.items():
                lines.append(f"    {k} = {v}")

        lines.append("-" * 70)
        return "\n".join(lines)

    # ------------------------------------------------------------
    def _log(self, level: str, message: str, tag: str = "", details: Optional[Dict[str, Any]] = None):
        text = self._format_log(level, message, tag, details)
        getattr(self.logger, level.lower())(text)

    # ------------------------------------------------------------
    def push_function(self, func_name: str):
        self.call_stack.append(func_name)

    def pop_function(self):
        if self.call_stack:
            self.call_stack.pop()

    # ------------------------------------------------------------
    # Convenience methods
    def info(self, message: str, tag: str = "", details: Optional[Dict[str, Any]] = None):
        self._log("INFO", message, tag, details)

    def debug(self, message: str, tag: str = "", details: Optional[Dict[str, Any]] = None):
        self._log("DEBUG", message, tag, details)

    def warning(self, message: str, tag: str = "", details: Optional[Dict[str, Any]] = None):
        self._log("WARNING", message, tag, details)

    def error(self, message: str, tag: str = "", details: Optional[Dict[str, Any]] = None):
        self._log("ERROR", message, tag, details)

    def critical(self, message: str, tag: str = "", details: Optional[Dict[str, Any]] = None):
        self._log("CRITICAL", message, tag, details)


# ------------------------------------------------------------
# Decorator to automatically push/pop function name
def trace(logger_attr_name="logger"):
    def decorator(func):
        def wrapper(*args, **kwargs):
            logger: CustomLogger = kwargs.get(logger_attr_name) or args[0]
            func_name = func.__name__
            logger.push_function(func_name)
            try:
                return func(*args, **kwargs)
            finally:
                logger.pop_function()
        return wrapper
    return decorator
