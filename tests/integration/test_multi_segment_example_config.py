"""Integration tests using example multi-segment configuration."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pandas as pd

from cleared.engine import ClearedEngine
from cleared.cli.utils import load_config_from_file
from cleared.cli.cmds.verify.core import verify_data


class TestMultiSegmentExampleConfig:
    """Integration tests using example multi-segment configuration."""

    def setup_method(self):
        """Set up test environment with example config structure."""
        self.temp_dir = tempfile.mkdtemp()

        # Create directory structure matching example config
        self.test_data_dir = Path(self.temp_dir) / "test_data"
        self.test_output_dir = Path(self.temp_dir) / "test_output"
        self.test_deid_ref_input_dir = Path(self.temp_dir) / "test_deid_ref_input"
        self.test_deid_ref_output_dir = Path(self.temp_dir) / "test_deid_ref_output"
        self.test_runtime_dir = Path(self.temp_dir) / "test_runtime"

        for dir_path in [
            self.test_data_dir,
            self.test_output_dir,
            self.test_deid_ref_input_dir,
            self.test_deid_ref_output_dir,
            self.test_runtime_dir,
        ]:
            dir_path.mkdir(parents=True, exist_ok=True)

        # Create users table as directory with segments
        users_dir = self.test_data_dir / "users"
        users_dir.mkdir()

        # Create segment files with realistic data
        segment1_data = pd.DataFrame(
            {
                "user_id": [1, 2, 3, 4, 5],
                "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
                "email": [
                    "alice@example.com",
                    "bob@example.com",
                    "charlie@example.com",
                    "diana@example.com",
                    "eve@example.com",
                ],
                "registration_date": [
                    "2020-01-15",
                    "2020-02-20",
                    "2020-03-10",
                    "2020-04-05",
                    "2020-05-12",
                ],
            }
        )

        segment2_data = pd.DataFrame(
            {
                "user_id": [6, 7, 8, 9, 10],
                "name": ["Frank", "Grace", "Henry", "Ivy", "Jack"],
                "email": [
                    "frank@example.com",
                    "grace@example.com",
                    "henry@example.com",
                    "ivy@example.com",
                    "jack@example.com",
                ],
                "registration_date": [
                    "2020-06-18",
                    "2020-07-22",
                    "2020-08-14",
                    "2020-09-09",
                    "2020-10-25",
                ],
            }
        )

        segment3_data = pd.DataFrame(
            {
                "user_id": [11, 12],
                "name": ["Karen", "Liam"],
                "email": ["karen@example.com", "liam@example.com"],
                "registration_date": ["2020-11-30", "2020-12-15"],
            }
        )

        segment1_data.to_csv(users_dir / "segment1.csv", index=False)
        segment2_data.to_csv(users_dir / "segment2.csv", index=False)
        segment3_data.to_csv(users_dir / "segment3.csv", index=False)

        # Create events table as single file
        events_data = pd.DataFrame(
            {
                "event_id": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                "user_id": [1, 1, 2, 2, 3, 4, 5, 6, 7, 8],
                "event_name": [
                    "login",
                    "logout",
                    "login",
                    "purchase",
                    "login",
                    "logout",
                    "purchase",
                    "login",
                    "logout",
                    "purchase",
                ],
                "event_date": [
                    "2023-01-10",
                    "2023-01-10",
                    "2023-01-11",
                    "2023-01-12",
                    "2023-01-13",
                    "2023-01-14",
                    "2023-01-15",
                    "2023-01-16",
                    "2023-01-17",
                    "2023-01-18",
                ],
            }
        )
        events_data.to_csv(self.test_data_dir / "events.csv", index=False)

        # Create config file
        self.config_file = Path(self.temp_dir) / "multi_segment_config.yaml"
        self._create_config_file()

    def teardown_method(self):
        """Clean up test environment."""
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _create_config_file(self):
        """Create example configuration file."""
        config_content = f"""
name: "multi_segment_example"
deid_config:
  time_shift:
    method: "random_days"
    min: -365
    max: 365
io:
  data:
    input_config:
      io_type: "filesystem"
      configs:
        base_path: "{self.test_data_dir}"
        file_format: "csv"
    output_config:
      io_type: "filesystem"
      configs:
        base_path: "{self.test_output_dir}"
        file_format: "csv"
  deid_ref:
    input_config:
      io_type: "filesystem"
      configs:
        base_path: "{self.test_deid_ref_input_dir}"
    output_config:
      io_type: "filesystem"
      configs:
        base_path: "{self.test_deid_ref_output_dir}"
  runtime_io_path: "{self.test_runtime_dir}"
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
  events:
    name: "events"
    depends_on: []
    transformers:
      - method: "IDDeidentifier"
        uid: "event_id_transformer"
        depends_on: []
        configs:
          idconfig:
            name: "event_id"
            uid: "event_id"
            description: "Event identifier"
"""
        self.config_file.write_text(config_content)

    def test_example_config_loads(self):
        """Test that example config loads without errors."""
        config = load_config_from_file(self.config_file)

        assert config.name == "multi_segment_example"
        assert len(config.tables) == 2
        assert "users" in config.tables
        assert "events" in config.tables

        # Verify table structure detected correctly
        from cleared.io.filesystem import FileSystemDataLoader
        from omegaconf import DictConfig

        loader_config = DictConfig(
            {
                "data_source_type": "filesystem",
                "connection_params": {
                    "base_path": str(self.test_data_dir),
                    "file_format": "csv",
                },
            }
        )
        loader = FileSystemDataLoader(loader_config)

        # Users should be detected as directory
        users_paths = loader.get_table_paths("users")
        assert isinstance(users_paths, list)
        assert len(users_paths) == 3

        # Events should be detected as single file
        events_paths = loader.get_table_paths("events")
        assert isinstance(events_paths, Path)

    def test_example_config_run(self):
        """Test running full de-identification on example config."""
        config = load_config_from_file(self.config_file)
        engine = ClearedEngine.from_config(config)

        results = engine.run()

        # Verify execution succeeded
        assert results.success
        assert len(results.results) == 2

        # Verify users output structure (segments)
        users_output_dir = self.test_output_dir / "users"
        assert users_output_dir.exists()
        assert users_output_dir.is_dir()
        assert (users_output_dir / "segment1.csv").exists()
        assert (users_output_dir / "segment2.csv").exists()
        assert (users_output_dir / "segment3.csv").exists()

        # Verify events output (single file)
        events_output_file = self.test_output_dir / "events.csv"
        assert events_output_file.exists()

        # Verify segment contents
        segment1_output = pd.read_csv(users_output_dir / "segment1.csv")
        assert len(segment1_output) == 5
        assert "user_id" in segment1_output.columns

        # Verify combined data
        all_segments = []
        for segment_file in sorted(users_output_dir.glob("*.csv")):
            all_segments.append(pd.read_csv(segment_file))
        combined = pd.concat(all_segments, ignore_index=True)
        assert len(combined) == 12  # 5 + 5 + 2

    def test_example_config_reverse(self):
        """Test reverse operation on example output."""
        # Run forward transformation first
        config = load_config_from_file(self.config_file)
        engine = ClearedEngine.from_config(config)
        engine.run()

        # Run reverse
        reverse_dir = Path(self.temp_dir) / "reversed"
        reverse_dir.mkdir()

        results = engine.run(reverse=True, reverse_output_path=reverse_dir)

        # Verify execution succeeded
        assert results.success

        # Verify reverse output structure
        users_reverse_dir = reverse_dir / "users"
        assert users_reverse_dir.exists()
        assert users_reverse_dir.is_dir()
        assert (users_reverse_dir / "segment1.csv").exists()
        assert (users_reverse_dir / "segment2.csv").exists()
        assert (users_reverse_dir / "segment3.csv").exists()

        events_reverse_file = reverse_dir / "events.csv"
        assert events_reverse_file.exists()

    def test_example_config_verify(self):
        """Test verification on example data."""
        # Run forward transformation
        config = load_config_from_file(self.config_file)
        engine = ClearedEngine.from_config(config)
        engine.run()

        # Run reverse
        reverse_dir = Path(self.temp_dir) / "reversed"
        reverse_dir.mkdir()
        engine.run(reverse=True, reverse_output_path=reverse_dir)

        # Run verification
        verification_results = verify_data(config, reverse_dir)

        # Verify results structure
        assert verification_results.overview.total_tables == 2
        assert "users" in [t.table_name for t in verification_results.tables]
        assert "events" in [t.table_name for t in verification_results.tables]

        # Verify users table verification (segments combined)
        users_result = next(
            t for t in verification_results.tables if t.table_name == "users"
        )
        assert users_result is not None

    def test_example_config_validate(self):
        """Test config validation passes."""
        # Config should be valid
        # Note: validate_config may need to be imported or tested differently
        # This is a placeholder for the actual validation test
        config = load_config_from_file(self.config_file)
        assert config is not None
        assert config.name == "multi_segment_example"
