"""tests for tasks related to logging within scripts"""

import pytest
import logging
import datetime
import tempfile
import os

import tamr_toolbox.utils.logger


def test_logger_name():
    # make sure non-strings break
    with pytest.raises(TypeError):
        tamr_toolbox.utils.logger.create(42)
    with pytest.raises(TypeError):
        tamr_toolbox.utils.logger.create([])
    with pytest.raises(TypeError):
        tamr_toolbox.utils.logger.create({})
    with pytest.raises(ValueError):
        tamr_toolbox.utils.logger.create("")


def test_filename():
    date_format = "%Y-%m-%d"
    date = datetime.datetime.now().strftime(date_format)
    assert tamr_toolbox.utils.logger._get_log_filename() == f"{date}.log"
    assert tamr_toolbox.utils.logger._get_log_filename("") == f"{date}.log"
    assert tamr_toolbox.utils.logger._get_log_filename("test_") == f"test_{date}.log"


def test_create_logger_with_stream_and_file_handler():
    # Create temp directory. Remember to cleanup!
    tempdir = tempfile.TemporaryDirectory()
    logger = tamr_toolbox.utils.logger.create(
        "test_logging_stream_and_file", log_to_terminal=True, log_directory=tempdir.name
    )

    assert len(logger.handlers) == 2

    found_file_handler = False
    found_stream_handler = False
    for handler in logger.handlers:
        found_file_handler = found_file_handler or type(handler) == logging.FileHandler
        found_stream_handler = found_stream_handler or type(handler) == logging.StreamHandler

    assert found_file_handler and found_stream_handler

    # Cleanup temp directory
    try:
        tempdir.cleanup()
    except (PermissionError, NotADirectoryError):
        # Windows sometimes fails so try one more time
        tempdir.cleanup()


def test_create_logger_with_only_stream_handler():
    logger = tamr_toolbox.utils.logger.create(
        "test_logging_stream_only", log_to_terminal=True, log_directory=None
    )

    assert len(logger.handlers) == 1

    found_file_handler = False
    found_stream_handler = False
    for handler in logger.handlers:
        found_file_handler = found_file_handler or type(handler) == logging.FileHandler
        found_stream_handler = found_stream_handler or type(handler) == logging.StreamHandler

    assert not found_file_handler and found_stream_handler


def test_create_logger_with_only_file_handler():
    # Create temp directory. Remember to cleanup!
    tempdir = tempfile.TemporaryDirectory()
    logger = tamr_toolbox.utils.logger.create(
        "test_logging_file_only", log_to_terminal=False, log_directory=tempdir.name
    )

    assert len(logger.handlers) == 1

    found_file_handler = False
    found_stream_handler = False
    for handler in logger.handlers:
        found_file_handler = found_file_handler or type(handler) == logging.FileHandler
        found_stream_handler = found_stream_handler or type(handler) == logging.StreamHandler

    assert found_file_handler and not found_stream_handler

    # Cleanup temp directory
    try:
        tempdir.cleanup()
    except (PermissionError, NotADirectoryError):
        # Windows sometimes fails so try one more time
        tempdir.cleanup()


def test_log_uncaught_exception():
    """
    The logger will only record uncaught exceptions. Test these by
    running a separate script that raises and logs an exception
    """
    with tempfile.TemporaryDirectory() as tempdir:

        script_path = os.path.join(tempdir, "my_error_script.py")
        log_prefix = "uncaught_exception"
        log_file_path = os.path.join(
            tempdir, f"{log_prefix}_{tamr_toolbox.utils.logger._get_log_filename()}"
        )

        # start with a clean log file
        if os.path.exists(log_file_path):
            os.remove(log_file_path)

        with open(script_path, "w") as f:
            f.write(
                f"""from pathlib import Path
import tamr_toolbox

logger = tamr_toolbox.utils.logger.create(
    __file__,
    log_to_terminal=False,
    log_directory=str(Path(__file__).parent),
    log_prefix="{log_prefix}",
)
1/0
"""
            )
        os.system(f"python {script_path}")

        assert "ZeroDivisionError" in open(log_file_path).read()


def test_enable_toolbox_logging_with_stream_and_file_handler():
    # Create temp directory. Remember to cleanup!
    tempdir = tempfile.TemporaryDirectory()
    package_logger = logging.getLogger("tamr_toolbox")
    # Reset package logger to have no handlers
    package_logger.handlers.clear()
    tamr_toolbox.utils.logger.enable_toolbox_logging(
        log_to_terminal=True, log_directory=tempdir.name
    )

    assert len(package_logger.handlers) == 2

    found_file_handler = False
    found_stream_handler = False
    for handler in package_logger.handlers:
        found_file_handler = found_file_handler or type(handler) == logging.FileHandler
        found_stream_handler = found_stream_handler or type(handler) == logging.StreamHandler

    assert found_file_handler and found_stream_handler

    # Cleanup temp directory
    try:
        tempdir.cleanup()
    except (PermissionError, NotADirectoryError):
        # Windows sometimes fails so try one more time
        tempdir.cleanup()


def test_enable_toolbox_logging_with_only_file_handler():
    # Create temp directory. Remember to cleanup!
    tempdir = tempfile.TemporaryDirectory()
    package_logger = logging.getLogger("tamr_toolbox")
    # Reset package logger to have no handlers
    package_logger.handlers.clear()
    tamr_toolbox.utils.logger.enable_toolbox_logging(
        log_to_terminal=False, log_directory=tempdir.name
    )

    assert len(package_logger.handlers) == 1

    found_file_handler = False
    found_stream_handler = False
    for handler in package_logger.handlers:
        found_file_handler = found_file_handler or type(handler) == logging.FileHandler
        found_stream_handler = found_stream_handler or type(handler) == logging.StreamHandler

    assert found_file_handler and not found_stream_handler

    # Cleanup temp directory
    try:
        tempdir.cleanup()
    except (PermissionError, NotADirectoryError):
        # Windows sometimes fails so try one more time
        tempdir.cleanup()
