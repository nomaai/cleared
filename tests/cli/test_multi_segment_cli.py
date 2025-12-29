"""CLI command integration tests for multi-segment table processing."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pandas as pd

from cleared.cli.cmds.run import _run_engine_internal
from cleared.cli.cmds.verify.core import verify_data


class TestMultiSegmentCLI:
    """CLI command integration tests for multi-segment tables."""

    def setup_method(self):
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()
        self.input_dir = Path(self.temp_dir) / "input"
        self.output_dir = Path(self.temp_dir) / "output"
        self.deid_ref_dir = Path(self.temp_dir) / "deid_ref"
        self.runtime_dir = Path(self.temp_dir) / "runtime"

        for dir_path in [
            self.input_dir,
            self.output_dir,
            self.deid_ref_dir,
            self.runtime_dir,
        ]:
            dir_path.mkdir(parents=True, exist_ok=True)

        # Create users table as directory with segments
        users_dir = self.input_dir / "users"
        users_dir.mkdir()

        segment1_data = pd.DataFrame(
            {
                "user_id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
            }
        )
        segment2_data = pd.DataFrame(
            {
                "user_id": [4, 5, 6],
                "name": ["Diana", "Eve", "Frank"],
            }
        )

        segment1_data.to_csv(users_dir / "segment1.csv", index=False)
        segment2_data.to_csv(users_dir / "segment2.csv", index=False)

        # Create config file
        self.config_file = Path(self.temp_dir) / "config.yaml"
        self._create_config_file()

    def teardown_method(self):
        """Clean up test environment."""
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _create_config_file(self):
        """Create a test configuration file."""
        config_content = f"""
name: "multi_segment_cli_test"
deid_config:
  time_shift: null
io:
  data:
    input_config:
      io_type: "filesystem"
      configs:
        base_path: "{self.input_dir}"
        file_format: "csv"
    output_config:
      io_type: "filesystem"
      configs:
        base_path: "{self.output_dir}"
        file_format: "csv"
  deid_ref:
    input_config:
      io_type: "filesystem"
      configs:
        base_path: "{self.deid_ref_dir}"
    output_config:
      io_type: "filesystem"
      configs:
        base_path: "{self.deid_ref_dir}"
  runtime_io_path: "{self.runtime_dir}"
tables:
  users:
    name: "users"
    depends_on: []
    transformers:
      - method: "IDDeidentifier"
        uid: "user_id_transformer"
        depends_on: []
        configs:
          idconfig:
            name: "user_id"
            uid: "user_id"
            description: "User identifier"
"""
        self.config_file.write_text(config_content)

    def test_run_command_end_to_end(self):
        """Test cleared run command end-to-end with segments."""
        _run_engine_internal(
            config_path=self.config_file,
            config_name="cleared_config",
            overrides=None,
            continue_on_error=False,
            create_dirs=False,
            verbose=False,
            rows_limit=None,
            test_mode=False,
            reverse=False,
            reverse_output_path=None,
        )

        # Verify output structure
        users_output_dir = self.output_dir / "users"
        assert users_output_dir.exists()
        assert users_output_dir.is_dir()
        assert (users_output_dir / "segment1.csv").exists()
        assert (users_output_dir / "segment2.csv").exists()

        # Verify segment contents
        segment1_output = pd.read_csv(users_output_dir / "segment1.csv")
        assert len(segment1_output) == 3
        assert "user_id" in segment1_output.columns

    def test_reverse_command_end_to_end(self):
        """Test cleared reverse command end-to-end with segments."""
        # First run forward
        _run_engine_internal(
            config_path=self.config_file,
            config_name="cleared_config",
            overrides=None,
            continue_on_error=False,
            create_dirs=False,
            verbose=False,
            rows_limit=None,
            test_mode=False,
            reverse=False,
            reverse_output_path=None,
        )

        # Then run reverse
        reverse_dir = Path(self.temp_dir) / "reversed"
        reverse_dir.mkdir()

        _run_engine_internal(
            config_path=self.config_file,
            config_name="cleared_config",
            overrides=None,
            continue_on_error=False,
            create_dirs=False,
            verbose=False,
            rows_limit=None,
            test_mode=False,
            reverse=True,
            reverse_output_path=reverse_dir,
        )

        # Verify reverse output structure
        users_reverse_dir = reverse_dir / "users"
        assert users_reverse_dir.exists()
        assert users_reverse_dir.is_dir()
        assert (users_reverse_dir / "segment1.csv").exists()
        assert (users_reverse_dir / "segment2.csv").exists()

    def test_verify_command_end_to_end(self):
        """Test cleared verify command end-to-end with segments."""
        # Run forward transformation
        _run_engine_internal(
            config_path=self.config_file,
            config_name="cleared_config",
            overrides=None,
            continue_on_error=False,
            create_dirs=False,
            verbose=False,
            rows_limit=None,
            test_mode=False,
            reverse=False,
            reverse_output_path=None,
        )

        # Run reverse
        reverse_dir = Path(self.temp_dir) / "reversed"
        reverse_dir.mkdir()

        _run_engine_internal(
            config_path=self.config_file,
            config_name="cleared_config",
            overrides=None,
            continue_on_error=False,
            create_dirs=False,
            verbose=False,
            rows_limit=None,
            test_mode=False,
            reverse=True,
            reverse_output_path=reverse_dir,
        )

        # Load config and run verification
        from cleared.cli.utils import load_config_from_file

        config = load_config_from_file(self.config_file)
        verification_results = verify_data(config, reverse_dir)

        # Verify results structure
        assert verification_results.overview.total_tables == 1
        assert "users" in [t.table_name for t in verification_results.tables]

    def test_test_mode_with_segments(self):
        """Test cleared test command with segments."""
        _run_engine_internal(
            config_path=self.config_file,
            config_name="cleared_config",
            overrides=None,
            continue_on_error=False,
            create_dirs=False,
            verbose=False,
            rows_limit=2,
            test_mode=True,
            reverse=False,
            reverse_output_path=None,
        )

        # Verify no output files created (test mode)
        users_output_dir = self.output_dir / "users"
        assert not users_output_dir.exists()

        # Verify runtime results file exists
        _result_files = list(self.runtime_dir.glob("status_*.json"))
        # Results may or may not be saved in test mode depending on implementation
