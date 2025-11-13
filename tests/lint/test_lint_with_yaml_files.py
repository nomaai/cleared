"""Integration tests for linting with YAML configuration files."""

from pathlib import Path

from cleared.lint import lint_cleared_config
from cleared.cli.utils import load_config_from_file


class TestLintWithYamlFiles:
    """Test linting rules with actual YAML configuration files."""

    def test_lint_errors_yaml_file(self):
        """Test that linting correctly identifies errors in test_lint_errors.yaml."""
        config_path = (
            Path(__file__).parent.parent.parent / "examples" / "test_lint_errors.yaml"
        )

        # Load configuration
        cleared_config = load_config_from_file(config_path)

        # Run linting
        issues = lint_cleared_config(config_path, cleared_config)

        # Extract rule IDs
        rule_ids = {issue.rule for issue in issues}

        # Verify that we catch the expected errors/warnings:
        # - cleared-011: Time shift range invalid (min > max)
        # - cleared-004: Invalid table dependency
        # - cleared-017: Table has no transformers
        # - cleared-018: System directory path

        assert "cleared-011" in rule_ids, (
            "Should detect time shift range error (min > max)"
        )
        assert "cleared-004" in rule_ids, "Should detect invalid table dependency"
        assert "cleared-017" in rule_ids, "Should detect table with no transformers"
        assert "cleared-018" in rule_ids, "Should detect system directory path"

        # Check that we have at least these 4 issues
        assert len(issues) >= 4, f"Expected at least 4 issues, got {len(issues)}"

        # Verify error severities
        errors = [i for i in issues if i.severity == "error"]
        warnings = [i for i in issues if i.severity == "warning"]

        # cleared-011 and cleared-004 should be errors
        error_rules = {i.rule for i in errors}
        assert "cleared-011" in error_rules, "cleared-011 should be an error"
        assert "cleared-004" in error_rules, "cleared-004 should be an error"

        # cleared-017 and cleared-018 should be warnings
        warning_rules = {i.rule for i in warnings}
        assert "cleared-017" in warning_rules, "cleared-017 should be a warning"
        assert "cleared-018" in warning_rules, "cleared-018 should be a warning"

    def test_lint_errors_ignored_yaml_file(self):
        """Test that linting ignores errors when ignore comments are present."""
        config_path = (
            Path(__file__).parent.parent.parent
            / "examples"
            / "test_lint_errors_ignored.yaml"
        )

        # Load configuration
        cleared_config = load_config_from_file(config_path)

        # Run linting
        issues = lint_cleared_config(config_path, cleared_config)

        # Extract rule IDs
        rule_ids = {issue.rule for issue in issues}

        # Verify that ignored rules are NOT in the issues:
        # - cleared-011 should be ignored (has yamllint disable-line rule:cleared-011)
        # - cleared-004 should be ignored (has yamllint disable-line rule:cleared-004)
        # - cleared-017 should be ignored (has yamllint disable-line rule:cleared-017)
        # - cleared-018 should be ignored (has yamllint disable-line rule:cleared-018)

        assert "cleared-011" not in rule_ids, (
            "cleared-011 should be ignored via yamllint disable-line comment"
        )
        assert "cleared-004" not in rule_ids, (
            "cleared-004 should be ignored via yamllint disable-line comment"
        )
        assert "cleared-017" not in rule_ids, (
            "cleared-017 should be ignored via yamllint disable-line comment"
        )
        assert "cleared-018" not in rule_ids, (
            "cleared-018 should be ignored via yamllint disable-line comment"
        )

        # There should be no issues (or only issues not covered by ignore comments)
        # Since we're ignoring all the intentional errors, there should be 0 issues
        # (unless there are other issues we didn't account for)
        ignored_rules = {"cleared-011", "cleared-004", "cleared-017", "cleared-018"}

        # The remaining issues should be minimal (maybe just cleared-009 for random_days)
        # Let's just verify that the ignored rules are not present
        assert all(rule not in rule_ids for rule in ignored_rules), (
            f"All ignored rules should be absent. Found: {rule_ids & ignored_rules}"
        )

    def test_lint_errors_specific_rules(self):
        """Test specific linting rules with detailed assertions."""
        config_path = (
            Path(__file__).parent.parent.parent / "examples" / "test_lint_errors.yaml"
        )

        # Load configuration
        cleared_config = load_config_from_file(config_path)

        # Run linting
        issues = lint_cleared_config(config_path, cleared_config)

        # Test cleared-011: Time shift range validation
        timeshift_issues = [i for i in issues if i.rule == "cleared-011"]
        assert len(timeshift_issues) > 0, "Should have at least one cleared-011 issue"
        timeshift_error = next(
            (i for i in timeshift_issues if i.severity == "error"), None
        )
        assert timeshift_error is not None, "Should have an error for min > max"
        assert (
            "min (30) > max (-30)" in timeshift_error.message
            or "min (30) > max (-30)" in str(timeshift_error)
        )

        # Test cleared-004: Invalid table dependency
        dependency_issues = [i for i in issues if i.rule == "cleared-004"]
        assert len(dependency_issues) > 0, "Should have at least one cleared-004 issue"
        dependency_error = dependency_issues[0]
        assert dependency_error.severity == "error", "cleared-004 should be an error"
        assert "non_existent_table" in dependency_error.message

        # Test cleared-017: Table has no transformers
        transformer_issues = [i for i in issues if i.rule == "cleared-017"]
        assert len(transformer_issues) > 0, "Should have at least one cleared-017 issue"
        transformer_warning = transformer_issues[0]
        assert transformer_warning.severity == "warning", (
            "cleared-017 should be a warning"
        )
        assert (
            "patients" in transformer_warning.message
            or "no transformers" in transformer_warning.message.lower()
        )

        # Test cleared-018: System directory
        system_dir_issues = [i for i in issues if i.rule == "cleared-018"]
        assert len(system_dir_issues) > 0, "Should have at least one cleared-018 issue"
        system_dir_warning = system_dir_issues[0]
        assert system_dir_warning.severity == "warning", (
            "cleared-018 should be a warning"
        )
        assert (
            "/tmp" in system_dir_warning.message
            or "system directory" in system_dir_warning.message.lower()
        )
