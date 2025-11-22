"""Main CLI application for Cleared data de-identification framework."""

import typer
import logging

# Set up colored logging before importing commands
from cleared.logging_config import setup_logging

# Initialize logging with colors
setup_logging(level=logging.INFO, use_colors=True)

# Import all command modules (must be after setup_logging)
from cleared.cli.cmds import (  # noqa: E402
    check_syntax,
    verify,
    describe,
    format as format_cmd,
    info,
    init,
    lint,
    report_verify,
    reverse,
    run,
    setup,
    test,
    validate,
)

# Create the main Typer app
app = typer.Typer(
    name="cleared",
    help="Cleared - A data de-identification framework for Python",
    add_completion=False,
    no_args_is_help=True,
)

# Register all commands
run.register_run_command(app)
test.register_test_command(app)
reverse.register_reverse_command(app)
verify.register_verify_command(app)
check_syntax.register_check_syntax_command(app)
validate.register_validate_command(app)
init.register_init_command(app)
setup.register_setup_command(app)
lint.register_lint_command(app)
format_cmd.register_format_command(app)
describe.register_describe_command(app)
report_verify.register_report_verify_command(app)
info.register_info_command(app)


if __name__ == "__main__":
    app()
