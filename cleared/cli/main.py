"""Main CLI application for Cleared data de-identification framework."""

import typer

# Import all command modules
from cleared.cli.cmds import (
    check_syntax,
    format as format_cmd,
    info,
    init,
    lint,
    run,
    setup,
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
check_syntax.register_check_syntax_command(app)
validate.register_validate_command(app)
init.register_init_command(app)
setup.register_setup_command(app)
lint.register_lint_command(app)
format_cmd.register_format_command(app)
info.register_info_command(app)


if __name__ == "__main__":
    app()
