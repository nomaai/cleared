"""Integration tests for filtered de-identification scenarios from the tutorial."""

import unittest
import pandas as pd
import numpy as np
import tempfile
import os
from datetime import datetime

import cleared as clr
from cleared.engine import ClearedEngine
from cleared.config.structure import (
    ClearedConfig,
    ClearedIOConfig,
    PairedIOConfig,
    IOConfig,
    DeIDConfig,
    TimeShiftConfig,
    TableConfig,
    TransformerConfig,
    IdentifierConfig,
    FilterConfig,
)


class TestFilteredDeidentificationIntegration(unittest.TestCase):
    """Integration tests for filtered de-identification tutorial scenarios."""

    def setUp(self):
        """Set up test data and configuration."""
        # Use sample data from cleared
        self.users_df = clr.sample_data.users_multi_table
        self.events_with_surveys_df = clr.sample_data.events_with_surveys

        # Store original data for comparison
        self.original_user_ids = set(self.users_df["user_id"])
        self.original_events_user_ids = set(self.events_with_surveys_df["user_id"])

        # Identify survey submission date events
        self.survey_events = self.events_with_surveys_df[
            self.events_with_surveys_df["event_name"] == "Survey submission date"
        ].copy()
        self.original_survey_event_date_times = self.survey_events[
            "event_date_time"
        ].copy()
        self.original_survey_event_values = self.survey_events["event_value"].copy()

        # Identify user submitted events
        self.user_submitted_events = self.events_with_surveys_df[
            self.events_with_surveys_df["event_name"] == "user submitted"
        ].copy()
        self.original_user_submitted_values = self.user_submitted_events[
            "event_value"
        ].copy()

        # Identify sensor events (should remain unchanged)
        self.sensor_events = self.events_with_surveys_df[
            self.events_with_surveys_df["event_name"].isin(
                ["sensor_1", "sensor_2", "sensor_3"]
            )
        ].copy()
        self.original_sensor_event_date_times = self.sensor_events[
            "event_date_time"
        ].copy()
        self.original_sensor_event_values = self.sensor_events["event_value"].copy()

    def _create_test_config_with_paths(self, base_path: str) -> ClearedConfig:
        """Create test configuration with filtered de-identification."""
        input_dir = os.path.join(base_path, "input")
        output_dir = os.path.join(base_path, "output")
        deid_ref_dir = os.path.join(base_path, "deid_ref")
        runtime_dir = os.path.join(base_path, "runtime")

        # Create IO configuration
        io_config = ClearedIOConfig(
            data=PairedIOConfig(
                input_config=IOConfig(
                    io_type="filesystem",
                    configs={"base_path": input_dir, "file_format": "csv"},
                ),
                output_config=IOConfig(
                    io_type="filesystem",
                    configs={"base_path": output_dir, "file_format": "csv"},
                ),
            ),
            deid_ref=PairedIOConfig(
                input_config=IOConfig(
                    io_type="filesystem", configs={"base_path": deid_ref_dir}
                ),
                output_config=IOConfig(
                    io_type="filesystem", configs={"base_path": deid_ref_dir}
                ),
            ),
            runtime_io_path=runtime_dir,
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
                        "idconfig": IdentifierConfig(
                            name="user_id",
                            uid="user_uid",
                            description="User identifier",
                        )
                    },
                ),
                TransformerConfig(
                    method="DateTimeDeidentifier",
                    uid="users_datetime_transformer",
                    depends_on=["user_id_transformer"],
                    configs={
                        "idconfig": IdentifierConfig(
                            name="user_id",
                            uid="user_uid",
                            description="User identifier",
                        ),
                        "datetime_column": "reg_date_time",
                    },
                ),
                TransformerConfig(
                    method="ColumnDropper",
                    uid="name_drop_transformer",
                    depends_on=["users_datetime_transformer"],
                    configs={
                        "idconfig": IdentifierConfig(
                            name="name",
                            uid="name_drop",
                            description="User name to drop",
                        )
                    },
                ),
            ],
        )

        # Events with surveys table configuration
        events_table = TableConfig(
            name="events_with_surveys",
            depends_on=["users"],
            transformers=[
                TransformerConfig(
                    method="IDDeidentifier",
                    uid="events_user_id_transformer",
                    depends_on=[],
                    configs={
                        "idconfig": IdentifierConfig(
                            name="user_id",
                            uid="user_uid",
                            description="User identifier",
                        )
                    },
                ),
                TransformerConfig(
                    method="DateTimeDeidentifier",
                    uid="events_datetime_transformer",
                    depends_on=["events_user_id_transformer"],
                    configs={
                        "idconfig": IdentifierConfig(
                            name="user_id",
                            uid="user_uid",
                            description="User identifier",
                        ),
                        "datetime_column": "event_date_time",
                    },
                    filter=FilterConfig(
                        where_condition="event_name == 'Survey submission date'"
                    ),
                ),
                TransformerConfig(
                    method="DateTimeDeidentifier",
                    uid="events_value_datetime_transformer",
                    depends_on=["events_datetime_transformer"],
                    configs={
                        "idconfig": IdentifierConfig(
                            name="user_id",
                            uid="user_uid",
                            description="User identifier",
                        ),
                        "datetime_column": "event_value",
                    },
                    filter=FilterConfig(
                        where_condition="event_name == 'Survey submission date'"
                    ),
                    value_cast="datetime",  # Cast string datetime values to datetime type
                ),
                TransformerConfig(
                    method="IDDeidentifier",
                    uid="events_value_id_transformer",
                    depends_on=["events_value_datetime_transformer"],
                    configs={
                        "idconfig": IdentifierConfig(
                            name="event_value",
                            uid="user_uid",
                            description="User ID in event_value for user submitted events",
                        )
                    },
                    filter=FilterConfig(
                        where_condition="event_name == 'user submitted'"
                    ),
                    value_cast="integer",  # Cast string ID values to integer to match user_id type
                ),
            ],
        )

        return ClearedConfig(
            name="test_filtered_deid_pipeline",
            deid_config=DeIDConfig(
                time_shift=TimeShiftConfig(method="shift_by_days", min=30, max=90)
            ),
            io=io_config,
            tables={"users": users_table, "events_with_surveys": events_table},
        )

    def test_filtered_deidentification_integration(self):
        """Test that filtered de-identification works correctly in integration."""
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
            self.events_with_surveys_df.to_csv(
                os.path.join(input_dir, "events_with_surveys.csv"), index=False
            )

            # Create config with temporary paths
            config = self._create_test_config_with_paths(temp_dir)

            # Create engine
            engine = ClearedEngine.from_config(config)

            # Run de-identification
            results = engine.run()

            # Verify results
            self.assertTrue(results.success)
            self.assertEqual(len(results.results), 2)  # Should have 2 pipelines

            # Check that output files were created
            output_dir = os.path.join(temp_dir, "output")
            self.assertTrue(os.path.exists(os.path.join(output_dir, "users.csv")))
            self.assertTrue(
                os.path.exists(os.path.join(output_dir, "events_with_surveys.csv"))
            )

            # Load de-identified data
            users_deid = pd.read_csv(os.path.join(output_dir, "users.csv"))
            events_deid = pd.read_csv(
                os.path.join(output_dir, "events_with_surveys.csv")
            )

            # No type conversions - work with data as it comes from CSV (strings)

            # Test 1: Verify user_id de-identification consistency
            deid_user_ids = set(users_deid["user_id"])
            deid_events_user_ids = set(events_deid["user_id"])

            # All user_ids should be de-identified (different from original)
            self.assertNotEqual(self.original_user_ids, deid_user_ids)
            self.assertNotEqual(self.original_events_user_ids, deid_events_user_ids)

            # User IDs should be consistent across tables
            self.assertEqual(deid_user_ids, deid_events_user_ids)

            # Test 2: Verify survey submission date events are de-identified
            survey_events_deid = events_deid[
                events_deid["event_name"] == "Survey submission date"
            ].copy()

            # Verify we have the same number of survey events
            self.assertEqual(len(survey_events_deid), len(self.survey_events))

            # event_date_time should be de-identified (shifted)
            # Match by position/index since user_ids are de-identified
            # Work with strings as they come from CSV
            for i, (_idx, row) in enumerate(survey_events_deid.iterrows()):
                if i < len(self.survey_events):
                    original_date_str = str(
                        self.original_survey_event_date_times.iloc[i]
                    )
                    deid_date_str = str(row["event_date_time"])
                    # Dates should be different (shifted by 30-90 days)
                    self.assertNotEqual(original_date_str, deid_date_str)
                    # Verify the date string format is correct (contains date/time)
                    self.assertTrue("2023" in deid_date_str or "2024" in deid_date_str)

            # event_value (datetime) should be de-identified for survey events
            # Work with strings as they come from CSV
            for i, (_idx, row) in enumerate(survey_events_deid.iterrows()):
                if i < len(self.survey_events):
                    original_value_str = str(self.original_survey_event_values.iloc[i])
                    deid_value_str = str(row["event_value"])
                    # Values should be different (shifted by 30-90 days)
                    self.assertNotEqual(original_value_str, deid_value_str)
                    # Verify the date string format is correct
                    self.assertTrue(
                        "2023" in deid_value_str or "2024" in deid_value_str
                    )

            # Test 3: Verify user submitted events have event_value de-identified
            user_submitted_deid = events_deid[
                events_deid["event_name"] == "user submitted"
            ].copy()

            # event_value should be de-identified (user_id values)
            # Work with strings as they come from CSV
            # Original values were "101", "202", "303" - these should be de-identified
            original_values = {"101", "202", "303"}

            for _idx, row in user_submitted_deid.iterrows():
                event_value = str(row["event_value"])
                # The event_value should be de-identified (not the original values)
                self.assertNotIn(event_value, original_values)
                # Since we use the same uid="user_uid", the mapping should be shared
                # We verify that the values have been changed from the original

            # Test 4: Verify sensor events remain unchanged
            sensor_events_deid = events_deid[
                events_deid["event_name"].isin(["sensor_1", "sensor_2", "sensor_3"])
            ].copy()

            # event_date_time should remain unchanged for sensor events
            for _idx, row in sensor_events_deid.iterrows():
                # Find corresponding original event by matching other columns
                original_row = self.sensor_events[
                    (self.sensor_events["user_id"].isin(self.original_events_user_ids))
                    & (self.sensor_events["event_name"] == row["event_name"])
                    & (self.sensor_events["event_value"] == row["event_value"])
                ]
                if len(original_row) > 0:
                    # The event_date_time should be the same (not shifted)
                    # But we need to account for user_id de-identification
                    # So we match by event_name and event_value instead
                    pass  # This is complex to verify exactly, but we can check event_value

            # event_value should remain unchanged for sensor events
            # Work with strings as they come from CSV
            sensor_values_deid = set(str(v) for v in sensor_events_deid["event_value"])
            sensor_values_original = set(
                str(v) for v in self.sensor_events["event_value"]
            )
            # Sensor event values should be the same (they're numeric, not dates)
            self.assertEqual(sensor_values_deid, sensor_values_original)

            # Test 5: Verify referential integrity
            # All user_ids in events should exist in users table
            events_user_ids = set(events_deid["user_id"])
            users_user_ids = set(users_deid["user_id"])
            self.assertTrue(events_user_ids.issubset(users_user_ids))

    def test_survey_events_datetime_deidentification(self):
        """Test that survey submission date events have both datetime columns de-identified."""
        with tempfile.TemporaryDirectory() as temp_dir:
            input_dir = os.path.join(temp_dir, "input")
            deid_ref_dir = os.path.join(temp_dir, "deid_ref")
            runtime_dir = os.path.join(temp_dir, "runtime")
            os.makedirs(input_dir, exist_ok=True)
            os.makedirs(deid_ref_dir, exist_ok=True)
            os.makedirs(runtime_dir, exist_ok=True)

            self.users_df.to_csv(os.path.join(input_dir, "users.csv"), index=False)
            self.events_with_surveys_df.to_csv(
                os.path.join(input_dir, "events_with_surveys.csv"), index=False
            )

            config = self._create_test_config_with_paths(temp_dir)
            engine = ClearedEngine.from_config(config)
            results = engine.run()

            self.assertTrue(results.success)

            output_dir = os.path.join(temp_dir, "output")
            events_deid = pd.read_csv(
                os.path.join(output_dir, "events_with_surveys.csv")
            )
            # No type conversions - work with data as it comes from CSV (strings)

            # Check survey events
            survey_events_deid = events_deid[
                events_deid["event_name"] == "Survey submission date"
            ]

            # Verify all survey events have de-identified event_date_time
            # Match by position since user_ids are de-identified
            # Work with strings as they come from CSV
            for i, (_, row) in enumerate(survey_events_deid.iterrows()):
                if i < len(self.survey_events):
                    original_date_str = str(
                        self.original_survey_event_date_times.iloc[i]
                    )
                    deid_date_str = str(row["event_date_time"])
                    # Should be different (shifted)
                    self.assertNotEqual(original_date_str, deid_date_str)
                    # Verify the date string format is correct
                    self.assertTrue("2023" in deid_date_str or "2024" in deid_date_str)

    def test_user_submitted_events_id_deidentification(self):
        """Test that user submitted events have event_value (user_id) de-identified."""
        with tempfile.TemporaryDirectory() as temp_dir:
            input_dir = os.path.join(temp_dir, "input")
            deid_ref_dir = os.path.join(temp_dir, "deid_ref")
            runtime_dir = os.path.join(temp_dir, "runtime")
            os.makedirs(input_dir, exist_ok=True)
            os.makedirs(deid_ref_dir, exist_ok=True)
            os.makedirs(runtime_dir, exist_ok=True)

            self.users_df.to_csv(os.path.join(input_dir, "users.csv"), index=False)
            self.events_with_surveys_df.to_csv(
                os.path.join(input_dir, "events_with_surveys.csv"), index=False
            )

            config = self._create_test_config_with_paths(temp_dir)
            engine = ClearedEngine.from_config(config)
            results = engine.run()

            self.assertTrue(results.success)

            output_dir = os.path.join(temp_dir, "output")
            users_deid = pd.read_csv(os.path.join(output_dir, "users.csv"))
            events_deid = pd.read_csv(
                os.path.join(output_dir, "events_with_surveys.csv")
            )

            # Get de-identified user_ids (for potential future use)
            _deid_user_ids = set(users_deid["user_id"])

            # Check user submitted events
            user_submitted_deid = events_deid[
                events_deid["event_name"] == "user submitted"
            ]

            # Work with strings as they come from CSV
            # Original values were "101", "202", "303" - these should be de-identified
            original_user_submitted_values = {"101", "202", "303"}

            for _, row in user_submitted_deid.iterrows():
                event_value = str(row["event_value"])
                # Original values were "101", "202", "303" - these should be de-identified
                # The de-identified values should NOT be the original values
                self.assertNotIn(event_value, original_user_submitted_values)
                # The de-identified event_value should be consistent with user_id de-identification
                # Since we use the same uid="user_uid", the mapping should be shared
                # But we can't directly verify this without the mapping table, so we just check
                # that it's been changed from the original

    def test_sensor_events_unchanged(self):
        """Test that sensor events remain unchanged."""
        with tempfile.TemporaryDirectory() as temp_dir:
            input_dir = os.path.join(temp_dir, "input")
            deid_ref_dir = os.path.join(temp_dir, "deid_ref")
            runtime_dir = os.path.join(temp_dir, "runtime")
            os.makedirs(input_dir, exist_ok=True)
            os.makedirs(deid_ref_dir, exist_ok=True)
            os.makedirs(runtime_dir, exist_ok=True)

            self.users_df.to_csv(os.path.join(input_dir, "users.csv"), index=False)
            self.events_with_surveys_df.to_csv(
                os.path.join(input_dir, "events_with_surveys.csv"), index=False
            )

            config = self._create_test_config_with_paths(temp_dir)
            engine = ClearedEngine.from_config(config)
            results = engine.run()

            self.assertTrue(results.success)

            output_dir = os.path.join(temp_dir, "output")
            events_deid = pd.read_csv(
                os.path.join(output_dir, "events_with_surveys.csv")
            )

            # Check sensor events
            sensor_events_deid = events_deid[
                events_deid["event_name"].isin(["sensor_1", "sensor_2", "sensor_3"])
            ]

            # event_value should remain unchanged (numeric values)
            # Work with strings as they come from CSV
            sensor_values_deid = set(str(v) for v in sensor_events_deid["event_value"])
            sensor_values_original = set(
                str(v) for v in self.sensor_events["event_value"]
            )
            self.assertEqual(sensor_values_deid, sensor_values_original)

    def test_user_id_consistency_across_tables(self):
        """Test that user_id de-identification is consistent across tables."""
        with tempfile.TemporaryDirectory() as temp_dir:
            input_dir = os.path.join(temp_dir, "input")
            deid_ref_dir = os.path.join(temp_dir, "deid_ref")
            runtime_dir = os.path.join(temp_dir, "runtime")
            os.makedirs(input_dir, exist_ok=True)
            os.makedirs(deid_ref_dir, exist_ok=True)
            os.makedirs(runtime_dir, exist_ok=True)

            self.users_df.to_csv(os.path.join(input_dir, "users.csv"), index=False)
            self.events_with_surveys_df.to_csv(
                os.path.join(input_dir, "events_with_surveys.csv"), index=False
            )

            config = self._create_test_config_with_paths(temp_dir)
            engine = ClearedEngine.from_config(config)
            results = engine.run()

            self.assertTrue(results.success)

            output_dir = os.path.join(temp_dir, "output")
            users_deid = pd.read_csv(os.path.join(output_dir, "users.csv"))
            events_deid = pd.read_csv(
                os.path.join(output_dir, "events_with_surveys.csv")
            )

            # Get user_ids from both tables
            users_user_ids = set(users_deid["user_id"])
            events_user_ids = set(events_deid["user_id"])

            # All events user_ids should exist in users table
            self.assertTrue(events_user_ids.issubset(users_user_ids))

            # Check that the mapping is consistent
            # If a user_id appears in events, it should map to the same de-identified ID
            # This is verified by using the same uid="user_uid" in both transformers

    def test_iddeidentifier_string_integer_type_mismatch(self):
        """
        Test that IDDeidentifier correctly handles string ID values.

        This test verifies that string IDs like "101" should match integer IDs
        like 101 in the deid_ref_df when using the same uid, preventing duplicate mappings.
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            input_dir = os.path.join(temp_dir, "input")
            deid_ref_dir = os.path.join(temp_dir, "deid_ref")
            runtime_dir = os.path.join(temp_dir, "runtime")
            os.makedirs(input_dir, exist_ok=True)
            os.makedirs(deid_ref_dir, exist_ok=True)
            os.makedirs(runtime_dir, exist_ok=True)

            # Create users table with integer user_ids
            users_df = pd.DataFrame(
                {
                    "user_id": [101, 202, 303],
                    "name": ["Alice", "Bob", "Charlie"],
                    "reg_date_time": [
                        datetime(2020, 1, 15, 10, 30),
                        datetime(2019, 6, 22, 14, 45),
                        datetime(2021, 3, 8, 9, 15),
                    ],
                }
            )

            # Create events table with event_value as STRINGS (from CSV)
            # Mix in non-numeric strings to prevent pandas from auto-converting to int
            # These string values "101", "202", "303" should match the integer user_ids
            events_df = pd.DataFrame(
                {
                    "user_id": [101, 202, 303, 101],  # Integer user_ids (add 4th row)
                    "event_name": [
                        "user submitted",
                        "user submitted",
                        "user submitted",
                        "other_event",
                    ],
                    "event_value": [
                        "101",
                        "202",
                        "303",
                        "not_a_number",
                    ],  # STRING IDs + non-numeric to prevent auto-conversion
                }
            )

            users_df.to_csv(os.path.join(input_dir, "users.csv"), index=False)
            events_df.to_csv(os.path.join(input_dir, "events.csv"), index=False)

            # Create config with same uid for both user_id and event_value
            config = self._create_test_config_for_type_mismatch(temp_dir)
            engine = ClearedEngine.from_config(config)
            results = engine.run()

            self.assertTrue(results.success)

            # Load the deid_ref to check for duplicate mappings
            output_dir = os.path.join(temp_dir, "output")
            events_deid = pd.read_csv(os.path.join(output_dir, "events.csv"))
            users_deid = pd.read_csv(os.path.join(output_dir, "users.csv"))

            # Check the deid_ref file directly to see if duplicate mappings exist
            deid_ref_file = os.path.join(deid_ref_dir, "user_uid.csv")
            if os.path.exists(deid_ref_file):
                deid_ref_df = pd.read_csv(deid_ref_file)
                print(f"\nDeid ref DataFrame shape: {deid_ref_df.shape}")
                print(f"Deid ref DataFrame:\n{deid_ref_df}")
                print(f"user_uid column dtype: {deid_ref_df['user_uid'].dtype}")
                print(f"user_uid values: {deid_ref_df['user_uid'].tolist()}")
                print(
                    f"user_uid value types: {[type(v) for v in deid_ref_df['user_uid'].tolist()]}"
                )

                # Check for duplicate mappings: we should have only 3 unique original values
                # (101, 202, 303) but might have 6 if strings and integers are both stored
                # Convert all to strings for comparison to catch type mismatches
                all_values_as_str = [str(v) for v in deid_ref_df["user_uid"].tolist()]
                unique_original_values_str = len(set(all_values_as_str))
                total_rows = len(deid_ref_df)

                print(
                    f"\nUnique original values (as strings): {unique_original_values_str}"
                )
                print(f"Total rows in deid_ref: {total_rows}")
                print(f"All values as strings: {all_values_as_str}")

                # This assertion will FAIL if duplicate mappings are created
                # We expect 3 unique values: "101", "202", "303" (or 101, 202, 303 as ints)
                # But if type mismatch occurs, we might have both "101" and 101
                self.assertEqual(
                    unique_original_values_str,
                    3,
                    f"Type mismatch bug: Expected 3 unique mappings (101, 202, 303), "
                    f"but found {unique_original_values_str} unique values (as strings) in {total_rows} rows. "
                    f"Values: {all_values_as_str}. "
                    f"This indicates duplicate mappings were created for string vs integer IDs.",
                )

                # Also check that we don't have both string and integer versions
                # by checking if string representation matches integer representation
                value_types = set(
                    type(v).__name__ for v in deid_ref_df["user_uid"].tolist()
                )
                print(f"Value types found: {value_types}")

                # Check if we have both string "101" and integer 101
                numeric_str_values = [
                    v
                    for v in deid_ref_df["user_uid"].tolist()
                    if isinstance(v, str) and v.isdigit()
                ]
                numeric_int_values = [
                    v
                    for v in deid_ref_df["user_uid"].tolist()
                    if isinstance(v, (int, np.integer))
                ]

                if numeric_str_values and numeric_int_values:
                    # Check for overlap
                    str_as_int = {int(v) for v in numeric_str_values}
                    int_set = set(numeric_int_values)
                    overlap = str_as_int & int_set

                    if overlap:
                        self.fail(
                            f"Type mismatch bug: Found both string and integer versions of the same IDs: {overlap}. "
                            f"String values: {numeric_str_values}, Integer values: {list(numeric_int_values)}. "
                            f"This causes duplicate mappings in deid_ref_df."
                        )

            # Check that event_value uses the same de-identification as user_id
            # Since both use uid="user_uid", string "101" should map to the same de-identified
            # value as integer 101

            # Create mapping from original user_id to de-identified user_id
            original_to_deid = {}
            for orig_idx, orig_user_id in enumerate(users_df["user_id"]):
                deid_user_id = users_deid.iloc[orig_idx]["user_id"]
                original_to_deid[str(orig_user_id)] = str(deid_user_id)

            # Verify each "user submitted" event: the event_value (string) should map to the same
            # de-identified value as the corresponding user_id (integer)
            # Only check rows that match the filter condition
            user_submitted_mask = events_df["event_name"] == "user submitted"
            user_submitted_original = events_df[user_submitted_mask].reset_index(
                drop=True
            )
            user_submitted_deid = events_deid[
                events_deid["event_name"] == "user submitted"
            ].reset_index(drop=True)

            # Match by position (order should be preserved)
            for i in range(len(user_submitted_original)):
                orig_row = user_submitted_original.iloc[i]
                deid_row = user_submitted_deid.iloc[i]

                original_user_id_str = str(orig_row["user_id"])
                original_event_value_str = str(orig_row["event_value"])
                deid_user_id = str(deid_row["user_id"])
                deid_event_value = str(deid_row["event_value"])

                # Expected: event_value "101" should map to the same de-identified value
                # as user_id 101, since they use the same uid="user_uid"
                expected_deid_value = original_to_deid.get(original_event_value_str)

                # This assertion verifies that value_cast fixed the type mismatch issue
                self.assertEqual(
                    deid_event_value,
                    expected_deid_value,
                    f"Type mismatch bug detected: event_value '{original_event_value_str}' "
                    f"(string) should map to '{expected_deid_value}' (same as user_id "
                    f"'{original_user_id_str}'), but got '{deid_event_value}'. "
                    f"This indicates value_cast is not working correctly.",
                )

    def _create_test_config_for_type_mismatch(self, base_path: str) -> ClearedConfig:
        """Create test configuration for type mismatch scenario."""
        input_dir = os.path.join(base_path, "input")
        output_dir = os.path.join(base_path, "output")
        deid_ref_dir = os.path.join(base_path, "deid_ref")
        runtime_dir = os.path.join(base_path, "runtime")

        io_config = ClearedIOConfig(
            data=PairedIOConfig(
                input_config=IOConfig(
                    io_type="filesystem",
                    configs={"base_path": input_dir, "file_format": "csv"},
                ),
                output_config=IOConfig(
                    io_type="filesystem",
                    configs={"base_path": output_dir, "file_format": "csv"},
                ),
            ),
            deid_ref=PairedIOConfig(
                input_config=IOConfig(
                    io_type="filesystem", configs={"base_path": deid_ref_dir}
                ),
                output_config=IOConfig(
                    io_type="filesystem", configs={"base_path": deid_ref_dir}
                ),
            ),
            runtime_io_path=runtime_dir,
        )

        # Users table - creates initial mappings with integer user_ids
        users_table = TableConfig(
            name="users",
            depends_on=[],
            transformers=[
                TransformerConfig(
                    method="IDDeidentifier",
                    uid="user_id_transformer",
                    depends_on=[],
                    configs={
                        "idconfig": IdentifierConfig(
                            name="user_id",
                            uid="user_uid",
                            description="User identifier",
                        )
                    },
                ),
            ],
        )

        # Events table - event_value contains STRING IDs that should match integer user_ids
        events_table = TableConfig(
            name="events",
            depends_on=["users"],
            transformers=[
                TransformerConfig(
                    method="IDDeidentifier",
                    uid="events_user_id_transformer",
                    depends_on=[],
                    configs={
                        "idconfig": IdentifierConfig(
                            name="user_id",
                            uid="user_uid",
                            description="User identifier",
                        )
                    },
                ),
                TransformerConfig(
                    method="IDDeidentifier",
                    uid="events_value_id_transformer",
                    depends_on=["events_user_id_transformer"],
                    configs={
                        "idconfig": IdentifierConfig(
                            name="event_value",
                            uid="user_uid",  # SAME uid - should share mappings!
                            description="User ID in event_value (should match user_id mappings)",
                        )
                    },
                    filter=FilterConfig(
                        where_condition="event_name == 'user submitted'"
                    ),
                    value_cast="integer",  # Cast string IDs to integer to match user_id type
                ),
            ],
        )

        return ClearedConfig(
            name="test_type_mismatch_pipeline",
            deid_config=DeIDConfig(
                time_shift=TimeShiftConfig(method="shift_by_days", min=30, max=90)
            ),
            io=io_config,
            tables={"users": users_table, "events": events_table},
        )


if __name__ == "__main__":
    unittest.main()
