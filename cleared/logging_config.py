"""Logging configuration for Cleared framework."""

import logging
import sys


class FormattedErrorFilter(logging.Filter):
    """Filter to suppress logging of formatted DataFrame errors."""

    def filter(self, record: logging.LogRecord) -> bool:
        """Filter out formatted error messages."""
        # Check if the log message contains formatted error indicators
        message = record.getMessage()
        if "Missing Column Error" in message or "\n  Table:" in message:
            return False
        return True


def setup_logging(
    level: int = logging.INFO,
    use_colors: bool = True,
    format_string: str | None = None,
) -> None:
    """
    Set up colored logging for the Cleared framework.

    Args:
        level: Logging level (default: INFO)
        use_colors: Whether to use colored output (default: True)
        format_string: Custom format string (optional)

    """
    try:
        import colorlog

        # Create a colored formatter
        if format_string is None:
            format_string = "%(log_color)s%(levelname)-8s%(reset)s %(message)s"

        # Use stderr for errors to separate from normal output
        handler = colorlog.StreamHandler(sys.stderr)
        handler.setFormatter(
            colorlog.ColoredFormatter(
                format_string,
                datefmt=None,
                reset=True,
                log_colors={
                    "DEBUG": "cyan",
                    "INFO": "green",
                    "WARNING": "yellow",
                    "ERROR": "red",
                    "CRITICAL": "red,bg_white",
                },
                secondary_log_colors={},
                style="%",
            )
        )

        # Get root logger and configure it
        root_logger = logging.getLogger()
        root_logger.setLevel(level)

        # Remove existing handlers to avoid duplicates
        root_logger.handlers.clear()

        # Add filter to suppress formatted errors
        handler.addFilter(FormattedErrorFilter())

        # Add our colored handler
        root_logger.addHandler(handler)

    except ImportError:
        # Fallback to standard logging if colorlog is not available
        if use_colors:
            logging.warning(
                "colorlog not installed. Install it with 'pip install colorlog' for colored output."
            )

        # Use standard logging configuration
        if format_string is None:
            format_string = "%(levelname)-8s %(message)s"

        logging.basicConfig(
            level=level,
            format=format_string,
            stream=sys.stderr,
        )


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance with the given name.

    Args:
        name: Logger name (typically __name__)

    Returns:
        Logger instance

    """
    return logging.getLogger(name)
