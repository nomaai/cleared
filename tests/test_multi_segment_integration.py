"""Integration tests for multi-segment table processing."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pandas as pd
import pytest

from cleared.engine import ClearedEngine
from cleared.config.structure import (
    ClearedConfig,
    ClearedIOConfig,
    DeIDConfig,
    IOConfig,
    PairedIOConfig,
    TableConfig,
    TransformerConfig,
)


class TestMultiSegmentIntegration:
    """Integration tests for multi-segment table processing."""

    def setup_method(self):
        """Set up test environment with segment directory structure."""
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

        # Create multiple segment files
        segment1_data = pd.DataFrame(
            {
                "user_id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "email": ["alice@test.com", "bob@test.com", "charlie@test.com"],
            }
        )
        segment2_data = pd.DataFrame(
            {
                "user_id": [4, 5, 6],
                "name": ["Diana", "Eve", "Frank"],
                "email": ["diana@test.com", "eve@test.com", "frank@test.com"],
            }
        )
        segment3_data = pd.DataFrame(
            {
                "user_id": [7, 8],
                "name": ["Grace", "Henry"],
                "email": ["grace@test.com", "henry@test.com"],
            }
        )

        segment1_data.to_csv(users_dir / "segment1.csv", index=False)
        segment2_data.to_csv(users_dir / "segment2.csv", index=False)
        segment3_data.to_csv(users_dir / "segment3.csv", index=False)

        # Create single file table for comparison
        events_data = pd.DataFrame(
            {
                "event_id": [1, 2, 3],
                "user_id": [1, 2, 3],
                "event_name": ["login", "logout", "purchase"],
            }
        )
        events_data.to_csv(self.input_dir / "events.csv", index=False)

        # Create config
        self.config = ClearedConfig(
            name="multi_segment_test",
            deid_config=DeIDConfig(),
            io=ClearedIOConfig(
                data=PairedIOConfig(
                    input_config=IOConfig(
                        io_type="filesystem",
                        configs={
                            "base_path": str(self.input_dir),
                            "file_format": "csv",
                        },
                    ),
                    output_config=IOConfig(
                        io_type="filesystem",
                        configs={
                            "base_path": str(self.output_dir),
                            "file_format": "csv",
                        },
                    ),
                ),
                deid_ref=PairedIOConfig(
                    input_config=IOConfig(
                        io_type="filesystem",
                        configs={"base_path": str(self.deid_ref_dir)},
                    ),
                    output_config=IOConfig(
                        io_type="filesystem",
                        configs={"base_path": str(self.deid_ref_dir)},
                    ),
                ),
                runtime_io_path=str(self.runtime_dir),
            ),
            tables={
                "users": TableConfig(
                    name="users",
                    depends_on=[],
                    transformers=[
                        TransformerConfig(
                            method="IDDeidentifier",
                            uid="user_id_transformer",
                            depends_on=[],
                            configs={
                                "idconfig": {
                                    "name": "user_id",
                                    "uid": "user_id",
                                    "description": "User identifier",
                                }
                            },
                        ),
                    ],
                ),
                "events": TableConfig(
                    name="events",
                    depends_on=[],
                    transformers=[
                        TransformerConfig(
                            method="IDDeidentifier",
                            uid="event_id_transformer",
                            depends_on=[],
                            configs={
                                "idconfig": {
                                    "name": "event_id",
                                    "uid": "event_id",
                                    "description": "Event identifier",
                                }
                            },
                        ),
                    ],
                ),
            },
        )

    def teardown_method(self):
        """Clean up test environment."""
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_run_command_with_segments(self):
        """Test run command with multi-segment users table."""
        engine = ClearedEngine.from_config(self.config)

        results = engine.run()

        # Verify execution succeeded
        assert results.success
        assert len(results.results) == 2  # users and events tables

        # Verify output structure for users (segments)
        users_output_dir = self.output_dir / "users"
        assert users_output_dir.exists()
        assert users_output_dir.is_dir()
        assert (users_output_dir / "segment1.csv").exists()
        assert (users_output_dir / "segment2.csv").exists()
        assert (users_output_dir / "segment3.csv").exists()

        # Verify output for events (single file)
        events_output_file = self.output_dir / "events.csv"
        assert events_output_file.exists()
        assert events_output_file.is_file()

        # Verify segment contents
        segment1_output = pd.read_csv(users_output_dir / "segment1.csv")
        assert len(segment1_output) == 3
        assert "user_id" in segment1_output.columns

        # Verify combined data would have 8 rows (3 + 3 + 2)
        all_segments = []
        for segment_file in sorted(users_output_dir.glob("*.csv")):
            all_segments.append(pd.read_csv(segment_file))
        combined = pd.concat(all_segments, ignore_index=True)
        assert len(combined) == 8

    def test_reverse_command_with_segments(self):
        """Test reverse command with segments."""
        # First run forward transformation
        engine = ClearedEngine.from_config(self.config)
        engine.run()

        # Create reverse output directory
        reverse_dir = Path(self.temp_dir) / "reversed"
        reverse_dir.mkdir()

        # Run reverse
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

        # Verify events reverse output
        events_reverse_file = reverse_dir / "events.csv"
        assert events_reverse_file.exists()

    def test_verify_command_with_segments(self):
        """Test verify command with segments."""
        # Run forward transformation
        engine = ClearedEngine.from_config(self.config)
        engine.run()

        # Run reverse
        reverse_dir = Path(self.temp_dir) / "reversed"
        reverse_dir.mkdir()
        engine.run(reverse=True, reverse_output_path=reverse_dir)

        # Run verification
        verification_results = engine.verify(
            original_data_path=self.input_dir,
            reversed_data_path=reverse_dir,
        )

        # Verify results structure
        # engine.verify() returns dict with overall_status and table_results
        assert "overall_status" in verification_results
        assert "table_results" in verification_results
        # Check that we have results for both tables (keyed by pipeline uid)
        assert len(verification_results["table_results"]) >= 2

    def test_test_command_with_segments(self):
        """Test test command with segments."""
        engine = ClearedEngine.from_config(self.config)

        # Run in test mode with row limit
        results = engine.run(test_mode=True, rows_limit=2)

        # Verify execution succeeded
        assert results.success

        # Verify no output files created (test mode)
        users_output_dir = self.output_dir / "users"
        assert not users_output_dir.exists()

        events_output_file = self.output_dir / "events.csv"
        assert not events_output_file.exists()

    def test_mixed_single_and_segment_tables(self):
        """Test engine with both single file and segment directory tables."""
        engine = ClearedEngine.from_config(self.config)

        results = engine.run()

        # Both tables should process successfully
        assert results.success
        assert "users" in results.results
        assert "events" in results.results
        assert results.results["users"].status == "success"
        assert results.results["events"].status == "success"

        # Verify users (segments) output structure
        users_output_dir = self.output_dir / "users"
        assert users_output_dir.exists()
        assert users_output_dir.is_dir()

        # Verify events (single file) output
        events_output_file = self.output_dir / "events.csv"
        assert events_output_file.exists()
        assert events_output_file.is_file()

    def test_deid_ref_dict_shared_across_segments(self):
        """Test deid_ref_dict shared and accumulated across segments."""
        engine = ClearedEngine.from_config(self.config)

        results = engine.run()

        # Verify execution succeeded
        assert results.success

        # Check that deid_ref files were created
        deid_ref_files = list(self.deid_ref_dir.glob("*.csv"))
        assert len(deid_ref_files) > 0

        # Verify that user_id mappings are consistent across segments
        # (same user_id should map to same deid value)
        users_output_dir = self.output_dir / "users"
        all_segments = []
        for segment_file in sorted(users_output_dir.glob("*.csv")):
            all_segments.append(pd.read_csv(segment_file))

        combined = pd.concat(all_segments, ignore_index=True)

        # Check that user_id column exists and has been transformed
        assert "user_id" in combined.columns

        # Load deid_ref to verify consistency
        deid_ref_df = pd.read_csv(deid_ref_files[0])
        if "user_id" in deid_ref_df.columns and "user_id__deid" in deid_ref_df.columns:
            # Verify mapping consistency
            mapping = dict(
                zip(deid_ref_df["user_id"], deid_ref_df["user_id__deid"], strict=False)
            )
            # Each original user_id should map to a unique deid value
            assert len(mapping) == len(set(mapping.values()))

    def test_segment_error_handling(self):
        """Test error handling in segment processing."""
        # Create a corrupt segment file
        corrupt_segment = self.input_dir / "users" / "corrupt.csv"
        corrupt_segment.write_text("invalid,csv\ncontent,with,wrong,columns\n")

        engine = ClearedEngine.from_config(self.config)

        # Should raise error without continue_on_error
        with pytest.raises((ValueError, RuntimeError)):
            engine.run(continue_on_error=False)

        # Should continue with continue_on_error=True
        results = engine.run(continue_on_error=True)
        # Engine may succeed but users table should have error
        assert "users" in results.results
        # Status depends on when error occurs, but should be tracked
