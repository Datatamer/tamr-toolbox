"""Tasks related to logging within scripts"""
import os
import sys
import logging
import datetime
from typing import Optional


def _get_log_filename(log_prefix: str = "", date_format: str = "%Y-%m-%d") -> str:
    """Generate standard format log-file names.

    Args:
        log_prefix:  prefix for log filename
        date_format: the date format to be used in the file name

    Returns:
        filename in format {log_prefix}_{current_date}
    """

    # When log_prefix is populated, append an underscore and
    # ignore any trailing underscore provided by the user
    if log_prefix != "":
        log_prefix = f"{log_prefix.rstrip('_')}_"

    date = datetime.datetime.now().strftime(date_format)
    log_filename = f"{log_prefix}{date}.log"
    return log_filename


def _add_handler(logger: logging.Logger, log_directory: Optional[str] = None, **kwargs) -> None:
    """Adds a handler to a logger, either a logging.StreamHandler if log_directory is None
    otherwise a logging.FileHandler piped to the directory specified.

    Args:
        logger: the logging.Logger class to which you would like to add a handler
        log_directory: Optional log directory to pass. If not None a FileHandler is added,
            otherwise a StreamHandler
        **kwargs: Keyword arguments for the _get_log_filename
     """
    if log_directory is None:
        handler = logging.StreamHandler()
    else:
        if not os.path.exists(log_directory):
            os.makedirs(log_directory)

        handler = logging.FileHandler(os.path.join(log_directory, _get_log_filename(**kwargs)))

    # for some reason you need to set both of these - setting to same value to avoid confusion
    logger.setLevel(logging.INFO)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "%(levelname)s <%(thread)d> [%(asctime)s] %(name)s <%(filename)s:%(lineno)d> %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def create(
    name: str,
    *,
    log_to_terminal: bool = True,
    log_directory: Optional[str] = None,
    log_prefix: str = "",
    date_format: str = "%Y-%m-%d",
) -> logging.Logger:
    """Return logger object with pre-defined format.
    Log file will be located under log_directory with file name
    <log_prefix>_<date>.log, quashing extra separating underscores. Defaults to <date>.log.

    For use in scripts only. To log in module files, use the standard library `logging` module with
    a module-level logger and enable package logging.  See
    https://docs.python.org/3/howto/logging.html#advanced-logging-tutorial

    >>> log = logging.getLogger(__name__)

    Args:
        name: This sets the name of your logger instance. It does not affect the file name.
            To change the filename use log_prefix
        log_to_terminal: Boolean indicating whether or not to log messages to the terminal.
        log_directory: The directory to place log files inside
        log_prefix: The string to prepend to the date in the log file name.
        date_format: format string for date suffix on log file name
    Returns:
        Logger object
    """
    if type(name) is not str:
        raise TypeError
    if name == "":
        raise ValueError

    logger = logging.getLogger(name)
    if log_to_terminal:
        _add_handler(logger, log_prefix=log_prefix, date_format=date_format)
    if log_directory is not None:
        _add_handler(
            logger, log_directory=log_directory, log_prefix=log_prefix, date_format=date_format
        )

    # enable logging of uncaught exceptions
    def log_exception(exc_type, exc_value, exc_traceback) -> None:
        logger.error("Uncaught exception:", exc_info=(exc_type, exc_value, exc_traceback))

    sys.excepthook = log_exception

    return logger


def set_logging_level(logger_name: str, level: str) -> None:
    """A useful method for setting logging level for all a given logger and its handlers.

    Args:
        logger_name: the name of the logger for which to set the level
        level: log level to use. The set available from core logging package is 'debug', 'info',
            'warning', 'error'
    """
    log = logging.getLogger(logger_name)
    logging_level = logging.getLevelName(level.upper())
    log.setLevel(logging_level)
    for x in log.handlers:
        x.setLevel(logging_level)


def enable_package_logging(
    package_name: str,
    *,
    log_to_terminal: bool = True,
    log_directory: Optional[str] = None,
    level: Optional[str] = None,
    log_prefix: str = "",
    date_format: str = "%Y-%m-%d",
) -> None:
    """A helper function to enable package logging for any package following
    python best practices for logging names (i.e. logger name == package.module.submodule).

    Args:
        package_name: the name of the package for which to enable logging
        log_to_terminal: Boolean indicating whether or not to log messages to the terminal
        log_directory: optional log directory which the package will write logs
        level: optional level to specify, default is WARNING (inherited from base logging package)
        log_prefix: Optional prefix for log files, if None will be blank string
        date_format: Optional date format for log file
    """

    package_logger = logging.getLogger(package_name)
    _add_handler(package_logger, log_directory, log_prefix=log_prefix, date_format=date_format)
    if log_to_terminal:
        _add_handler(package_logger, log_prefix=log_prefix, date_format=date_format)

    if level is not None:
        set_logging_level(package_name, level)


def enable_toolbox_logging(
    *,
    log_to_terminal: bool = True,
    log_directory: Optional[str] = None,
    level: Optional[str] = None,
    log_prefix: str = "",
    date_format: str = "%Y-%m-%d",
) -> None:
    """A simple wrapper to enable_package_logging to give friendly call for users.

    Args:
        log_to_terminal: Boolean indicating whether or not to log messages to the terminal
        log_directory: optional directory to which to write tamr_toolbox logs
        level: Optional logging level to specify, default is WARNING
            (inherited from base logging package)
        log_prefix: Optional prefix for log files, if None will be blank string
        date_format: Optional date format for log file
    """

    enable_package_logging(
        "tamr_toolbox",
        log_to_terminal=log_to_terminal,
        log_directory=log_directory,
        level=level,
        log_prefix=log_prefix,
        date_format=date_format,
    )
