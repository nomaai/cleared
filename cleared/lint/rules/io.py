"""IO configuration-related linting rules for Cleared configuration files."""

from cleared.config.structure import ClearedConfig
from cleared.lint.types import LintIssue


def rule_io_configuration_validation(config: ClearedConfig) -> list[LintIssue]:
    """
    Rule cleared-013: Validate IO configuration.

    - Validate io_type is one of: "filesystem", "sql" (if supported)
    - Check that file_format is valid for filesystem IO (csv, parquet, json, excel, pickle)
    - Warn if input and output paths are the same (data loss risk)
    - Check that required IO config keys exist (base_path for filesystem)

    Args:
        config: Loaded ClearedConfig object

    Returns:
        List of LintIssue objects

    """
    issues: list[LintIssue] = []

    valid_io_types = ["filesystem", "sql"]
    valid_file_formats = ["csv", "parquet", "json", "excel", "xlsx", "xls", "pickle"]

    # Validate data IO configuration
    if config.io.data:
        # Validate input_config
        if config.io.data.input_config:
            io_type = config.io.data.input_config.io_type
            if io_type not in valid_io_types:
                issues.append(
                    LintIssue(
                        "cleared-013",
                        f"Invalid io_type '{io_type}' in data.input_config. "
                        f"Valid types are: {', '.join(valid_io_types)}",
                    )
                )

            if io_type == "filesystem":
                configs = config.io.data.input_config.configs or {}
                # Check for base_path
                if "base_path" not in configs:
                    issues.append(
                        LintIssue(
                            "cleared-013",
                            "Missing required 'base_path' in data.input_config.configs for filesystem IO.",
                        )
                    )
                # Check file_format if provided
                if "file_format" in configs:
                    file_format = configs["file_format"]
                    if file_format not in valid_file_formats:
                        issues.append(
                            LintIssue(
                                "cleared-013",
                                f"Invalid file_format '{file_format}' in data.input_config.configs. "
                                f"Valid formats are: {', '.join(valid_file_formats)}",
                            )
                        )

        # Validate output_config
        if config.io.data.output_config:
            io_type = config.io.data.output_config.io_type
            if io_type not in valid_io_types:
                issues.append(
                    LintIssue(
                        "cleared-013",
                        f"Invalid io_type '{io_type}' in data.output_config. "
                        f"Valid types are: {', '.join(valid_io_types)}",
                    )
                )

            if io_type == "filesystem":
                configs = config.io.data.output_config.configs or {}
                # Check for base_path
                if "base_path" not in configs:
                    issues.append(
                        LintIssue(
                            "cleared-013",
                            "Missing required 'base_path' in data.output_config.configs for filesystem IO.",
                        )
                    )
                # Check file_format if provided
                if "file_format" in configs:
                    file_format = configs["file_format"]
                    if file_format not in valid_file_formats:
                        issues.append(
                            LintIssue(
                                "cleared-013",
                                f"Invalid file_format '{file_format}' in data.output_config.configs. "
                                f"Valid formats are: {', '.join(valid_file_formats)}",
                            )
                        )

        # Warn if input and output paths are the same
        if (
            config.io.data.input_config
            and config.io.data.output_config
            and config.io.data.input_config.io_type == "filesystem"
            and config.io.data.output_config.io_type == "filesystem"
        ):
            input_path = (
                config.io.data.input_config.configs.get("base_path")
                if config.io.data.input_config.configs
                else None
            )
            output_path = (
                config.io.data.output_config.configs.get("base_path")
                if config.io.data.output_config.configs
                else None
            )

            if input_path and output_path and input_path == output_path:
                issues.append(
                    LintIssue(
                        "cleared-013",
                        f"Data input and output paths are the same ('{input_path}'). "
                        f"This may cause data loss as output files will overwrite input files.",
                        severity="warning",
                    )
                )

    # Validate deid_ref IO configuration
    if config.io.deid_ref:
        # Validate input_config (if present)
        if config.io.deid_ref.input_config:
            io_type = config.io.deid_ref.input_config.io_type
            if io_type not in valid_io_types:
                issues.append(
                    LintIssue(
                        "cleared-013",
                        f"Invalid io_type '{io_type}' in deid_ref.input_config. "
                        f"Valid types are: {', '.join(valid_io_types)}",
                    )
                )

            if io_type == "filesystem":
                configs = config.io.deid_ref.input_config.configs or {}
                if "base_path" not in configs:
                    issues.append(
                        LintIssue(
                            "cleared-013",
                            "Missing required 'base_path' in deid_ref.input_config.configs for filesystem IO.",
                        )
                    )

        # Validate output_config
        if config.io.deid_ref.output_config:
            io_type = config.io.deid_ref.output_config.io_type
            if io_type not in valid_io_types:
                issues.append(
                    LintIssue(
                        "cleared-013",
                        f"Invalid io_type '{io_type}' in deid_ref.output_config. "
                        f"Valid types are: {', '.join(valid_io_types)}",
                    )
                )

            if io_type == "filesystem":
                configs = config.io.deid_ref.output_config.configs or {}
                if "base_path" not in configs:
                    issues.append(
                        LintIssue(
                            "cleared-013",
                            "Missing required 'base_path' in deid_ref.output_config.configs for filesystem IO.",
                        )
                    )

    return issues


def rule_output_paths_system_directories(config: ClearedConfig) -> list[LintIssue]:
    """
    Rule cleared-018: Warn if output paths are in system directories.

    System directories like /tmp, /var, /usr, /etc, etc. are typically:
    - Temporary and may be cleaned up automatically
    - Shared across users and processes
    - May have restricted permissions
    - Not suitable for persistent data storage

    This rule warns if any output paths point to system directories.

    Args:
        config: Loaded ClearedConfig object

    Returns:
        List of LintIssue objects

    """
    issues: list[LintIssue] = []

    # Common system directories that should be avoided for output
    system_directories = {
        "/tmp",
        "/var",
        "/usr",
        "/etc",
        "/bin",
        "/sbin",
        "/lib",
        "/lib64",
        "/opt",
        "/root",
        "/sys",
        "/proc",
        "/dev",
        "/run",
        "/boot",
        "/lost+found",
    }

    def is_system_directory(path: str) -> bool:
        """Check if a path is in a system directory."""
        if not path:
            return False

        # Normalize path (resolve relative paths, remove trailing slashes)
        normalized = path.rstrip("/")

        # Check if path starts with any system directory
        for sys_dir in system_directories:
            if normalized == sys_dir or normalized.startswith(sys_dir + "/"):
                return True

        return False

    # Check data output path
    if config.io.data.output_config.io_type == "filesystem":
        data_output_path = config.io.data.output_config.configs.get("base_path")
        if data_output_path and is_system_directory(data_output_path):
            issues.append(
                LintIssue(
                    "cleared-018",
                    f"Data output path '{data_output_path}' is in a system directory. "
                    f"System directories may be cleaned up automatically or have restricted permissions. "
                    f"Consider using a project-specific directory instead.",
                    severity="warning",
                )
            )

    # Check deid_ref output path
    if config.io.deid_ref.output_config.io_type == "filesystem":
        deid_ref_output_path = config.io.deid_ref.output_config.configs.get("base_path")
        if deid_ref_output_path and is_system_directory(deid_ref_output_path):
            issues.append(
                LintIssue(
                    "cleared-018",
                    f"DeID reference output path '{deid_ref_output_path}' is in a system directory. "
                    f"System directories may be cleaned up automatically or have restricted permissions. "
                    f"Consider using a project-specific directory instead.",
                    severity="warning",
                )
            )

    # Check runtime IO path
    if config.io.runtime_io_path and is_system_directory(config.io.runtime_io_path):
        issues.append(
            LintIssue(
                "cleared-018",
                f"Runtime IO path '{config.io.runtime_io_path}' is in a system directory. "
                f"System directories may be cleaned up automatically or have restricted permissions. "
                f"Consider using a project-specific directory instead.",
                severity="warning",
            )
        )

    return issues


def rule_input_output_path_overlap(config: ClearedConfig) -> list[LintIssue]:
    """
    Rule cleared-019: Warn if input and output paths overlap (data corruption risk).

    When input and output paths overlap (e.g., output is a subdirectory of input),
    there is a risk of data corruption as output files may overwrite input files
    or be read as input during processing.

    Args:
        config: Loaded ClearedConfig object

    Returns:
        List of LintIssue objects

    """
    issues: list[LintIssue] = []

    def normalize_path(path: str) -> str:
        """Normalize a path for comparison."""
        if not path:
            return ""
        # Remove trailing slashes and resolve relative paths
        normalized = path.rstrip("/")
        # Convert to absolute path if possible (for better comparison)
        # But keep relative paths as-is for now
        return normalized

    def paths_overlap(path1: str, path2: str) -> bool:
        """Check if two paths overlap (one is contained in the other)."""
        if not path1 or not path2:
            return False

        norm1 = normalize_path(path1)
        norm2 = normalize_path(path2)

        # Exact match
        if norm1 == norm2:
            return True

        # Check if one path is a prefix of the other
        # Add trailing slash to ensure we match directory boundaries
        norm1_with_slash = norm1 + "/"
        norm2_with_slash = norm2 + "/"

        return norm1_with_slash.startswith(
            norm2_with_slash
        ) or norm2_with_slash.startswith(norm1_with_slash)

    # Check data input/output overlap
    if (
        config.io.data.input_config
        and config.io.data.output_config
        and config.io.data.input_config.io_type == "filesystem"
        and config.io.data.output_config.io_type == "filesystem"
    ):
        input_path = (
            config.io.data.input_config.configs.get("base_path")
            if config.io.data.input_config.configs
            else None
        )
        output_path = (
            config.io.data.output_config.configs.get("base_path")
            if config.io.data.output_config.configs
            else None
        )

        if input_path and output_path and paths_overlap(input_path, output_path):
            issues.append(
                LintIssue(
                    "cleared-019",
                    f"Data input path '{input_path}' and output path '{output_path}' overlap. "
                    f"This may cause data corruption as output files may overwrite input files "
                    f"or be read as input during processing. Use separate, non-overlapping directories.",
                    severity="warning",
                )
            )

    # Check deid_ref input/output overlap
    if (
        config.io.deid_ref.input_config
        and config.io.deid_ref.output_config
        and config.io.deid_ref.input_config.io_type == "filesystem"
        and config.io.deid_ref.output_config.io_type == "filesystem"
    ):
        deid_input_path = (
            config.io.deid_ref.input_config.configs.get("base_path")
            if config.io.deid_ref.input_config.configs
            else None
        )
        deid_output_path = (
            config.io.deid_ref.output_config.configs.get("base_path")
            if config.io.deid_ref.output_config.configs
            else None
        )

        if (
            deid_input_path
            and deid_output_path
            and paths_overlap(deid_input_path, deid_output_path)
        ):
            issues.append(
                LintIssue(
                    "cleared-019",
                    f"DeID reference input path '{deid_input_path}' and output path '{deid_output_path}' overlap. "
                    f"This may cause data corruption as output files may overwrite input files "
                    f"or be read as input during processing. Use separate, non-overlapping directories.",
                    severity="warning",
                )
            )

    # Check if data output overlaps with data input
    # (already checked above, but we can also check deid_ref overlap with data)
    if (
        config.io.data.input_config
        and config.io.deid_ref.output_config
        and config.io.data.input_config.io_type == "filesystem"
        and config.io.deid_ref.output_config.io_type == "filesystem"
    ):
        data_input_path = (
            config.io.data.input_config.configs.get("base_path")
            if config.io.data.input_config.configs
            else None
        )
        deid_output_path = (
            config.io.deid_ref.output_config.configs.get("base_path")
            if config.io.deid_ref.output_config.configs
            else None
        )

        if (
            data_input_path
            and deid_output_path
            and paths_overlap(data_input_path, deid_output_path)
        ):
            issues.append(
                LintIssue(
                    "cleared-019",
                    f"Data input path '{data_input_path}' and DeID reference output path '{deid_output_path}' overlap. "
                    f"This may cause data corruption. Use separate, non-overlapping directories.",
                    severity="warning",
                )
            )

    # Check if data output overlaps with deid_ref input
    if (
        config.io.data.output_config
        and config.io.deid_ref.input_config
        and config.io.data.output_config.io_type == "filesystem"
        and config.io.deid_ref.input_config.io_type == "filesystem"
    ):
        data_output_path = (
            config.io.data.output_config.configs.get("base_path")
            if config.io.data.output_config.configs
            else None
        )
        deid_input_path = (
            config.io.deid_ref.input_config.configs.get("base_path")
            if config.io.deid_ref.input_config.configs
            else None
        )

        if (
            data_output_path
            and deid_input_path
            and paths_overlap(data_output_path, deid_input_path)
        ):
            issues.append(
                LintIssue(
                    "cleared-019",
                    f"Data output path '{data_output_path}' and DeID reference input path '{deid_input_path}' overlap. "
                    f"This may cause data corruption. Use separate, non-overlapping directories.",
                    severity="warning",
                )
            )

    return issues
