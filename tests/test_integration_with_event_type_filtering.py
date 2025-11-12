"""Integration tests for event type filtering in multi-table de-identification."""

import os
import pandas as pd
import pytest
from datetime import datetime
from cleared.engine import ClearedEngine
from cleared.config.structure import (
    ClearedConfig,
    TableConfig,
    TransformerConfig,
    IOConfig,
    PairedIOConfig,
    ClearedIOConfig,
    DeIDConfig,
    TimeShiftConfig,
    FilterConfig,
)


class TestEventTypeFilteringIntegration:
    """Test integration of event type filtering in multi-table de-identification."""

    def setup_method(self):
        """Set up test data and configuration."""
        # Create test users data
        self.users_df = pd.DataFrame(
            {
                "user_id": [101, 202, 303, 404, 505],
                "name": [
                    "Alice Johnson",
                    "Bob Smith",
                    "Charlie Brown",
                    "Diana Prince",
                    "Eve Wilson",
                ],
                "reg_date_time": [
                    datetime(2020, 1, 15, 10, 30),
                    datetime(2019, 6, 22, 14, 45),
                    datetime(2021, 3, 8, 9, 15),
                    datetime(2018, 11, 12, 16, 20),
                    datetime(2022, 7, 3, 11, 55),
                ],
                "zipcode": ["10001", "90210", "60601", "33101", "98101"],
            }
        )

        # Create test events with time data
        self.events_df = pd.DataFrame(
            {
                "user_id": [
                    101,
                    101,
                    202,
                    202,
                    303,
                    303,
                    404,
                    505,
                    505,
                    505,
                    101,
                    202,
                    303,
                ],
                "event_name": [
                    "login",
                    "purchase",
                    "login",
                    "logout",
                    "login",
                    "purchase",
                    "login",
                    "login",
                    "purchase",
                    "logout",
                    "delivery_time",
                    "delivery_time",
                    "delivery_time",
                ],
                "event_value": [
                    100.0,
                    250.0,
                    50.0,
                    0.0,
                    75.0,
                    300.0,
                    25.0,
                    150.0,
                    400.0,
                    0.0,
                    0.0,
                    0.0,
                    0.0,
                ],
                "event_date_time": [
                    datetime(2023, 1, 10, 8, 30),
                    datetime(2023, 1, 15, 14, 20),
                    datetime(2023, 2, 5, 9, 45),
                    datetime(2023, 2, 5, 17, 30),
                    datetime(2023, 3, 12, 10, 15),
                    datetime(2023, 3, 12, 15, 45),
                    datetime(2023, 4, 8, 11, 20),
                    datetime(2023, 5, 20, 13, 10),
                    datetime(2023, 5, 25, 16, 30),
                    datetime(2023, 5, 25, 18, 45),
                    datetime(2023, 1, 20, 10, 15),  # delivery_time event
                    datetime(2023, 2, 12, 14, 30),  # delivery_time event
                    datetime(2023, 3, 18, 9, 45),  # delivery_time event
                ],
            }
        )

        # Create test configuration
        self.config = self._create_test_config()

    def _create_test_config(self) -> ClearedConfig:
        """Create test configuration with filtered de-identification."""
        # IO configuration
        data_input_config = IOConfig(
            io_type="filesystem",
            configs={"base_path": "./test_input", "file_format": "csv"},
        )
        data_output_config = IOConfig(
            io_type="filesystem",
            configs={"base_path": "./test_output", "file_format": "csv"},
        )
        data_config = PairedIOConfig(
            input_config=data_input_config, output_config=data_output_config
        )

        deid_input_config = IOConfig(
            io_type="filesystem", configs={"base_path": "./test_deid_ref"}
        )
        deid_output_config = IOConfig(
            io_type="filesystem", configs={"base_path": "./test_deid_ref"}
        )
        deid_ref_config = PairedIOConfig(
            input_config=deid_input_config, output_config=deid_output_config
        )

        io_config = ClearedIOConfig(
            data=data_config, deid_ref=deid_ref_config, runtime_io_path="./test_runtime"
        )

        # Users table configuration
        users_table = TableConfig(
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
                            "uid": "user_uid",
                            "description": "User identifier",
                        }
                    },
                ),
                TransformerConfig(
                    method="DateTimeDeidentifier",
                    uid="users_datetime_transformer",
                    depends_on=["user_id_transformer"],
                    configs={
                        "idconfig": {
                            "name": "user_id",
                            "uid": "user_uid",
                            "description": "User identifier",
                        },
                        "datetime_column": "reg_date_time",
                    },
                ),
                TransformerConfig(
                    method="ColumnDropper",
                    uid="name_drop_transformer",
                    depends_on=["users_datetime_transformer"],
                    configs={
                        "idconfig": {
                            "name": "name",
                            "uid": "name_drop",
                            "description": "User name to drop",
                        }
                    },
                ),
            ],
        )

        # Events with time data table configuration
        events_table = TableConfig(
            name="events_with_time_data",
            depends_on=["users"],
            transformers=[
                TransformerConfig(
                    method="IDDeidentifier",
                    uid="events_user_id_transformer",
                    depends_on=[],
                    configs={
                        "idconfig": {
                            "name": "user_id",
                            "uid": "user_uid",
                            "description": "User identifier",
                        }
                    },
                ),
                TransformerConfig(
                    method="DateTimeDeidentifier",
                    uid="events_datetime_transformer",
                    depends_on=["events_user_id_transformer"],
                    configs={
                        "idconfig": {
                            "name": "user_id",
                            "uid": "user_uid",
                            "description": "User identifier",
                        },
                        "datetime_column": "event_date_time",
                    },
                    filter=FilterConfig(
                        where_condition="event_name == 'delivery_time'"
                    ),
                ),
            ],
        )

        return ClearedConfig(
            name="test_filtered_deid_pipeline",
            deid_config=DeIDConfig(
                time_shift=TimeShiftConfig(method="shift_by_days", min=30, max=90)
            ),
            io=io_config,
            tables={"users": users_table, "events_with_time_data": events_table},
        )

    def _create_test_config_with_paths(self, base_path: str) -> ClearedConfig:
        """Create test configuration with specific paths."""
        # IO configuration
        data_input_config = IOConfig(
            io_type="filesystem",
            configs={
                "base_path": os.path.join(base_path, "input"),
                "file_format": "csv",
            },
        )
        data_output_config = IOConfig(
            io_type="filesystem",
            configs={
                "base_path": os.path.join(base_path, "output"),
                "file_format": "csv",
            },
        )
        data_config = PairedIOConfig(
            input_config=data_input_config, output_config=data_output_config
        )

        deid_input_config = IOConfig(
            io_type="filesystem",
            configs={"base_path": os.path.join(base_path, "deid_ref")},
        )
        deid_output_config = IOConfig(
            io_type="filesystem",
            configs={"base_path": os.path.join(base_path, "deid_ref")},
        )
        deid_ref_config = PairedIOConfig(
            input_config=deid_input_config, output_config=deid_output_config
        )

        io_config = ClearedIOConfig(
            data=data_config,
            deid_ref=deid_ref_config,
            runtime_io_path=os.path.join(base_path, "runtime"),
        )

        # Users table configuration
        users_table = TableConfig(
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
                            "uid": "user_uid",
                            "description": "User identifier",
                        }
                    },
                ),
                TransformerConfig(
                    method="DateTimeDeidentifier",
                    uid="users_datetime_transformer",
                    depends_on=["user_id_transformer"],
                    configs={
                        "idconfig": {
                            "name": "user_id",
                            "uid": "user_uid",
                            "description": "User identifier",
                        },
                        "datetime_column": "reg_date_time",
                    },
                ),
                TransformerConfig(
                    method="ColumnDropper",
                    uid="name_drop_transformer",
                    depends_on=["users_datetime_transformer"],
                    configs={
                        "idconfig": {
                            "name": "name",
                            "uid": "name_drop",
                            "description": "User name to drop",
                        }
                    },
                ),
            ],
        )

        # Events with time data table configuration
        events_table = TableConfig(
            name="events_with_time_data",
            depends_on=["users"],
            transformers=[
                TransformerConfig(
                    method="IDDeidentifier",
                    uid="events_user_id_transformer",
                    depends_on=[],
                    configs={
                        "idconfig": {
                            "name": "user_id",
                            "uid": "user_uid",
                            "description": "User identifier",
                        }
                    },
                ),
                TransformerConfig(
                    method="DateTimeDeidentifier",
                    uid="events_datetime_transformer",
                    depends_on=["events_user_id_transformer"],
                    configs={
                        "idconfig": {
                            "name": "user_id",
                            "uid": "user_uid",
                            "description": "User identifier",
                        },
                        "datetime_column": "event_date_time",
                    },
                    filter=FilterConfig(
                        where_condition="event_name == 'delivery_time'"
                    ),
                ),
            ],
        )

        return ClearedConfig(
            name="test_filtered_deid_pipeline",
            deid_config=DeIDConfig(
                time_shift=TimeShiftConfig(method="shift_by_days", min=30, max=90)
            ),
            io=io_config,
            tables={"users": users_table, "events_with_time_data": events_table},
        )

    def test_filtered_deidentification_integration(self):
        """Test that filtered de-identification works correctly in integration."""
        import os
        import tempfile

        # Create temporary directories for test data
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create test directories
            input_dir = os.path.join(temp_dir, "input")
            deid_ref_dir = os.path.join(temp_dir, "deid_ref")
            runtime_dir = os.path.join(temp_dir, "runtime")
            os.makedirs(input_dir, exist_ok=True)
            os.makedirs(deid_ref_dir, exist_ok=True)
            os.makedirs(runtime_dir, exist_ok=True)

            # Save test data to CSV files
            self.users_df.to_csv(os.path.join(input_dir, "users.csv"), index=False)
            self.events_df.to_csv(
                os.path.join(input_dir, "events_with_time_data.csv"), index=False
            )

            # Update config to use temporary directory
            config = self._create_test_config_with_paths(temp_dir)

            # Create engine
            engine = ClearedEngine.from_config(config)

            # Run de-identification
            results = engine.run()

            # Verify results
            assert results.success
            assert len(results.results) == 2  # Should have 2 pipelines

            # Check that output files were created
            output_dir = os.path.join(temp_dir, "output")
            assert os.path.exists(os.path.join(output_dir, "users.csv"))
            assert os.path.exists(os.path.join(output_dir, "events_with_time_data.csv"))

            # Read the output files to verify results
            users_output = pd.read_csv(os.path.join(output_dir, "users.csv"))
            events_output = pd.read_csv(
                os.path.join(output_dir, "events_with_time_data.csv")
            )

            # Check users table - all data should be de-identified
            assert users_output.shape[0] == 5
            assert "name" not in users_output.columns  # Name should be dropped
            assert "user_id" in users_output.columns
            assert "reg_date_time" in users_output.columns

            # Check events table - only delivery_time events should have de-identified timestamps
            assert events_output.shape[0] == 13
            assert "user_id" in events_output.columns
            assert "event_name" in events_output.columns
            assert "event_date_time" in events_output.columns

            # Verify user_id de-identification consistency
            users_user_ids = set(users_output["user_id"])
            events_user_ids = set(events_output["user_id"])
            assert users_user_ids == events_user_ids  # Should be consistent

            # Verify filtered datetime de-identification
            delivery_events = events_output[
                events_output["event_name"] == "delivery_time"
            ]
            other_events = events_output[events_output["event_name"] != "delivery_time"]

            # Delivery events should have de-identified timestamps
            original_delivery_events = self.events_df[
                self.events_df["event_name"] == "delivery_time"
            ]
            assert len(delivery_events) == len(original_delivery_events)

            # Check that delivery event timestamps are different from original
            for i, (_, row) in enumerate(delivery_events.iterrows()):
                original_time = original_delivery_events.iloc[i]["event_date_time"]
                deid_time = pd.to_datetime(row["event_date_time"])
                assert original_time != deid_time  # Should be different

            # Other events should have original timestamps
            original_other_events = self.events_df[
                self.events_df["event_name"] != "delivery_time"
            ]
            assert len(other_events) == len(original_other_events)

            # Check that other event timestamps are the same as original
            for i, (_, row) in enumerate(other_events.iterrows()):
                original_time = original_other_events.iloc[i]["event_date_time"]
                deid_time = pd.to_datetime(row["event_date_time"])
                assert original_time == deid_time  # Should be the same

    def test_filtered_deidentification_preserves_row_order(self):
        """Test that filtered de-identification preserves original row order."""
        import os
        import tempfile

        with tempfile.TemporaryDirectory() as temp_dir:
            input_dir = os.path.join(temp_dir, "input")
            deid_ref_dir = os.path.join(temp_dir, "deid_ref")
            runtime_dir = os.path.join(temp_dir, "runtime")
            os.makedirs(input_dir, exist_ok=True)
            os.makedirs(deid_ref_dir, exist_ok=True)
            os.makedirs(runtime_dir, exist_ok=True)

            self.users_df.to_csv(os.path.join(input_dir, "users.csv"), index=False)
            self.events_df.to_csv(
                os.path.join(input_dir, "events_with_time_data.csv"), index=False
            )

            config = self._create_test_config_with_paths(temp_dir)
            engine = ClearedEngine.from_config(config)
            engine.run()

            # Read output to check row order
            events_output = pd.read_csv(
                os.path.join(temp_dir, "output", "events_with_time_data.csv")
            )

            # Check that row order is preserved
            original_event_names = self.events_df["event_name"].tolist()
            result_event_names = events_output["event_name"].tolist()
            assert original_event_names == result_event_names

    def test_filtered_deidentification_with_empty_filter_results(self):
        """Test filtered de-identification when filter results in empty DataFrame."""
        import os
        import tempfile

        # Create events data with no delivery_time events
        events_no_delivery = self.events_df[
            self.events_df["event_name"] != "delivery_time"
        ].copy()

        with tempfile.TemporaryDirectory() as temp_dir:
            input_dir = os.path.join(temp_dir, "input")
            deid_ref_dir = os.path.join(temp_dir, "deid_ref")
            runtime_dir = os.path.join(temp_dir, "runtime")
            os.makedirs(input_dir, exist_ok=True)
            os.makedirs(deid_ref_dir, exist_ok=True)
            os.makedirs(runtime_dir, exist_ok=True)

            self.users_df.to_csv(os.path.join(input_dir, "users.csv"), index=False)
            events_no_delivery.to_csv(
                os.path.join(input_dir, "events_with_time_data.csv"), index=False
            )

            config = self._create_test_config_with_paths(temp_dir)
            engine = ClearedEngine.from_config(config)
            engine.run()

            # Read output to verify all events have original timestamps
            events_output = pd.read_csv(
                os.path.join(temp_dir, "output", "events_with_time_data.csv")
            )

            # All events should have original timestamps (no delivery_time events to de-identify)
            original_times = events_no_delivery["event_date_time"]
            result_times = pd.to_datetime(events_output["event_date_time"])

            pd.testing.assert_series_equal(
                original_times, result_times, check_names=False
            )

    def test_filtered_deidentification_with_all_delivery_events(self):
        """Test filtered de-identification when all events are delivery_time events."""
        import os
        import tempfile

        # Create events data with only delivery_time events
        events_all_delivery = self.events_df[
            self.events_df["event_name"] == "delivery_time"
        ].copy()

        with tempfile.TemporaryDirectory() as temp_dir:
            input_dir = os.path.join(temp_dir, "input")
            deid_ref_dir = os.path.join(temp_dir, "deid_ref")
            runtime_dir = os.path.join(temp_dir, "runtime")
            os.makedirs(input_dir, exist_ok=True)
            os.makedirs(deid_ref_dir, exist_ok=True)
            os.makedirs(runtime_dir, exist_ok=True)

            self.users_df.to_csv(os.path.join(input_dir, "users.csv"), index=False)
            events_all_delivery.to_csv(
                os.path.join(input_dir, "events_with_time_data.csv"), index=False
            )

            config = self._create_test_config_with_paths(temp_dir)
            engine = ClearedEngine.from_config(config)
            engine.run()

            # Read output to verify all events have de-identified timestamps
            events_output = pd.read_csv(
                os.path.join(temp_dir, "output", "events_with_time_data.csv")
            )

            # All events should have de-identified timestamps
            original_times = events_all_delivery["event_date_time"]
            result_times = pd.to_datetime(events_output["event_date_time"])

            # All timestamps should be different
            for orig_time, result_time in zip(original_times, result_times):  # noqa: B905
                assert orig_time != result_time

    def test_filtered_deidentification_consistency_across_runs(self):
        """Test that filtered de-identification is consistent across multiple runs."""
        import os
        import tempfile

        with tempfile.TemporaryDirectory() as temp_dir:
            input_dir = os.path.join(temp_dir, "input")
            deid_ref_dir = os.path.join(temp_dir, "deid_ref")
            runtime_dir = os.path.join(temp_dir, "runtime")
            os.makedirs(input_dir, exist_ok=True)
            os.makedirs(deid_ref_dir, exist_ok=True)
            os.makedirs(runtime_dir, exist_ok=True)

            self.users_df.to_csv(os.path.join(input_dir, "users.csv"), index=False)
            self.events_df.to_csv(
                os.path.join(input_dir, "events_with_time_data.csv"), index=False
            )

            config = self._create_test_config_with_paths(temp_dir)
            engine = ClearedEngine.from_config(config)

            # First run
            results1 = engine.run()

            # Second run
            results2 = engine.run()

            # Both runs should be successful
            assert results1.success
            assert results2.success

    def test_filtered_deidentification_with_complex_filter(self):
        """Test filtered de-identification with complex filter conditions."""
        import os
        import tempfile

        # Create configuration with complex filter
        complex_config = self._create_test_config()
        events_table = complex_config.tables["events_with_time_data"]
        datetime_transformer = next(
            t
            for t in events_table.transformers
            if t.uid == "events_datetime_transformer"
        )
        datetime_transformer.filter = FilterConfig(
            where_condition="event_name == 'delivery_time' and event_value == 0"
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            input_dir = os.path.join(temp_dir, "input")
            deid_ref_dir = os.path.join(temp_dir, "deid_ref")
            runtime_dir = os.path.join(temp_dir, "runtime")
            os.makedirs(input_dir, exist_ok=True)
            os.makedirs(deid_ref_dir, exist_ok=True)
            os.makedirs(runtime_dir, exist_ok=True)

            self.users_df.to_csv(os.path.join(input_dir, "users.csv"), index=False)
            self.events_df.to_csv(
                os.path.join(input_dir, "events_with_time_data.csv"), index=False
            )

            # Update config paths
            config = self._create_test_config_with_paths(temp_dir)
            events_table = config.tables["events_with_time_data"]
            datetime_transformer = next(
                t
                for t in events_table.transformers
                if t.uid == "events_datetime_transformer"
            )
            datetime_transformer.filter = FilterConfig(
                where_condition="event_name == 'delivery_time' and event_value == 0"
            )

            engine = ClearedEngine.from_config(config)
            engine.run()

            # Read output to verify complex filtering
            events_output = pd.read_csv(
                os.path.join(temp_dir, "output", "events_with_time_data.csv")
            )

            # Only delivery_time events with event_value == 0 should have de-identified timestamps
            filtered_events = events_output[
                (events_output["event_name"] == "delivery_time")
                & (events_output["event_value"] == 0)
            ]
            other_events = events_output[
                ~(
                    (events_output["event_name"] == "delivery_time")
                    & (events_output["event_value"] == 0)
                )
            ]

            # Check that filtered events have de-identified timestamps
            original_filtered = self.events_df[
                (self.events_df["event_name"] == "delivery_time")
                & (self.events_df["event_value"] == 0)
            ]
            assert len(filtered_events) == len(original_filtered)

            # Check that other events have original timestamps
            original_other = self.events_df[
                ~(
                    (self.events_df["event_name"] == "delivery_time")
                    & (self.events_df["event_value"] == 0)
                )
            ]
            assert len(other_events) == len(original_other)

    def test_filtered_deidentification_preserves_other_columns(self):
        """Test that filtered de-identification preserves all other columns."""
        import os
        import tempfile

        with tempfile.TemporaryDirectory() as temp_dir:
            input_dir = os.path.join(temp_dir, "input")
            deid_ref_dir = os.path.join(temp_dir, "deid_ref")
            runtime_dir = os.path.join(temp_dir, "runtime")
            os.makedirs(input_dir, exist_ok=True)
            os.makedirs(deid_ref_dir, exist_ok=True)
            os.makedirs(runtime_dir, exist_ok=True)

            self.users_df.to_csv(os.path.join(input_dir, "users.csv"), index=False)
            self.events_df.to_csv(
                os.path.join(input_dir, "events_with_time_data.csv"), index=False
            )

            config = self._create_test_config_with_paths(temp_dir)
            engine = ClearedEngine.from_config(config)
            engine.run()

            # Read output to verify column preservation
            events_output = pd.read_csv(
                os.path.join(temp_dir, "output", "events_with_time_data.csv")
            )

            # All original columns should be preserved
            original_columns = set(self.events_df.columns)
            result_columns = set(events_output.columns)
            assert original_columns == result_columns

            # Non-datetime columns should be unchanged
            for col in ["event_name", "event_value"]:
                original_values = self.events_df[col]
                result_values = events_output[col]
                pd.testing.assert_series_equal(
                    original_values, result_values, check_names=False
                )

    def test_filtered_deidentification_with_missing_filter_column(self):
        """Test filtered de-identification with missing filter column."""
        import os
        import tempfile

        # Create events data without event_name column
        events_no_name = self.events_df.drop(columns=["event_name"])

        with tempfile.TemporaryDirectory() as temp_dir:
            input_dir = os.path.join(temp_dir, "input")
            deid_ref_dir = os.path.join(temp_dir, "deid_ref")
            runtime_dir = os.path.join(temp_dir, "runtime")
            os.makedirs(input_dir, exist_ok=True)
            os.makedirs(deid_ref_dir, exist_ok=True)
            os.makedirs(runtime_dir, exist_ok=True)

            self.users_df.to_csv(os.path.join(input_dir, "users.csv"), index=False)
            events_no_name.to_csv(
                os.path.join(input_dir, "events_with_time_data.csv"), index=False
            )

            config = self._create_test_config_with_paths(temp_dir)
            engine = ClearedEngine.from_config(config)

            # Should raise an error due to missing column in filter condition
            with pytest.raises(RuntimeError, match="Invalid filter condition"):
                engine.run()

    def test_filtered_deidentification_with_invalid_filter_condition(self):
        """Test filtered de-identification with invalid filter condition."""
        import os
        import tempfile

        with tempfile.TemporaryDirectory() as temp_dir:
            input_dir = os.path.join(temp_dir, "input")
            deid_ref_dir = os.path.join(temp_dir, "deid_ref")
            runtime_dir = os.path.join(temp_dir, "runtime")
            os.makedirs(input_dir, exist_ok=True)
            os.makedirs(deid_ref_dir, exist_ok=True)
            os.makedirs(runtime_dir, exist_ok=True)

            self.users_df.to_csv(os.path.join(input_dir, "users.csv"), index=False)
            self.events_df.to_csv(
                os.path.join(input_dir, "events_with_time_data.csv"), index=False
            )

            # Create configuration with invalid filter
            config = self._create_test_config_with_paths(temp_dir)
            events_table = config.tables["events_with_time_data"]
            datetime_transformer = next(
                t
                for t in events_table.transformers
                if t.uid == "events_datetime_transformer"
            )
            datetime_transformer.filter = FilterConfig(
                where_condition="invalid_column == 'delivery_time'"
            )

            engine = ClearedEngine.from_config(config)

            # Should raise an error due to invalid filter condition
            with pytest.raises(RuntimeError, match="Invalid filter condition"):
                engine.run()
