"""
Integration test for the quickstart tutorial example.

This test verifies that the exact code from docs/quickstart.md works correctly.
"""

import unittest
import pandas as pd
import tempfile
import os
import shutil
import cleared as clr


class TestQuickstartTutorial(unittest.TestCase):
    """Test the exact quickstart tutorial example."""

    def setUp(self):
        """Set up test environment."""
        # Create temporary directory
        self.temp_dir = tempfile.mkdtemp()
        self.original_cwd = os.getcwd()
        os.chdir(self.temp_dir)

    def tearDown(self):
        """Clean up test environment."""
        os.chdir(self.original_cwd)
        shutil.rmtree(self.temp_dir)

    def test_quickstart_tutorial_example(self):
        """Test the complete quickstart tutorial example from docs/quickstart.md."""
        # Get sample data (exact same as tutorial)
        users_df = clr.sample_data.users_single_table

        # Configure de-identification (exact same as tutorial)
        user_id_config = clr.IdentifierConfig(
            name="user_id", uid="user_uid", description="User identifier"
        )

        time_shift_config = clr.TimeShiftConfig(method="shift_by_years", min=-5, max=5)

        deid_config = clr.DeIDConfig(time_shift=time_shift_config)

        # Create transformers for the pipeline (exact same as tutorial)
        transformers = [
            clr.IDDeidentifier(idconfig=user_id_config),
            clr.DateTimeDeidentifier(
                idconfig=user_id_config,
                global_deid_config=deid_config,
                datetime_column="reg_date_time",
            ),
            clr.ColumnDropper(
                idconfig=clr.IdentifierConfig(
                    name="name", uid="name_drop", description="User name to drop"
                )
            ),
        ]

        # Create a simple IO config (exact same as tutorial)
        io_config = clr.PairedIOConfig(
            input_config=clr.IOConfig(io_type="filesystem", configs={"base_path": "."}),
            output_config=clr.IOConfig(
                io_type="filesystem", configs={"base_path": "."}
            ),
        )

        # Create table pipeline (exact same as tutorial)
        table_pipeline = clr.TablePipeline(
            table_name="users",
            io_config=io_config,
            deid_config=deid_config,
            transformers=transformers,
        )

        # Apply transformations (exact same as tutorial)
        users_df_deid, deid_ref_dict = table_pipeline.transform(users_df)

        # Verify results match tutorial expectations
        self.assertEqual(
            users_df_deid.shape, (5, 3)
        )  # Should have 3 columns after dropping name
        self.assertEqual(
            list(users_df_deid.columns), ["user_id", "reg_date_time", "zipcode"]
        )

        # Verify user_id de-identification
        original_user_ids = set(users_df["user_id"])
        deid_user_ids = set(users_df_deid["user_id"])
        self.assertNotEqual(original_user_ids, deid_user_ids)  # Should be different
        self.assertEqual(len(deid_user_ids), len(original_user_ids))  # Same count

        # Check that de-identified IDs are sequential integers starting from 1
        deid_user_ids_list = sorted(list(deid_user_ids))
        expected_deid_ids = list(range(1, len(original_user_ids) + 1))
        self.assertEqual(deid_user_ids_list, expected_deid_ids)

        # Verify datetime shifting
        original_dates = users_df["reg_date_time"]
        deid_dates = users_df_deid["reg_date_time"]
        self.assertFalse(original_dates.equals(deid_dates))  # Should be different

        # Verify name column was dropped
        self.assertNotIn("name", users_df_deid.columns)

        # Verify zipcode remains unchanged
        pd.testing.assert_series_equal(users_df["zipcode"], users_df_deid["zipcode"])

        # Verify de-identification reference dictionaries
        self.assertIn("user_uid", deid_ref_dict)
        self.assertIn("user_uid_shift", deid_ref_dict)

        # Check ID mapping structure
        id_mapping = deid_ref_dict["user_uid"]
        self.assertIn("user_uid", id_mapping.columns)
        self.assertIn("user_uid__deid", id_mapping.columns)
        self.assertEqual(len(id_mapping), 5)  # Should have 5 unique user IDs

        # Check time shift mapping structure
        time_shift_mapping = deid_ref_dict["user_uid_shift"]
        self.assertIn("user_uid", time_shift_mapping.columns)
        self.assertIn("user_uid_shift", time_shift_mapping.columns)
        self.assertEqual(len(time_shift_mapping), 5)  # Should have 5 unique user IDs

        # Verify time shifts are within expected range (-5 to 5 years)
        time_shift_values = time_shift_mapping["user_uid_shift"]
        for shift in time_shift_values:
            self.assertGreaterEqual(shift, -5)  # At least -5 years
            self.assertLessEqual(shift, 5)  # At most 5 years

    def test_quickstart_step_by_step(self):
        """Test the quickstart tutorial step by step to verify each transformation."""
        # Get sample data
        users_df = clr.sample_data.users_single_table

        # Step 1: ID de-identification
        user_id_config = clr.IdentifierConfig(
            name="user_id", uid="user_uid", description="User identifier"
        )
        id_deid = clr.IDDeidentifier(idconfig=user_id_config)
        users_df_deid, deid_ref_dict = id_deid.transform(users_df, {})

        # Verify ID de-identification
        self.assertEqual(
            users_df_deid.shape, (5, 4)
        )  # Same shape, no columns dropped yet
        self.assertIn("user_id", users_df_deid.columns)
        self.assertIn("name", users_df_deid.columns)
        self.assertIn("reg_date_time", users_df_deid.columns)
        self.assertIn("zipcode", users_df_deid.columns)

        # Step 2: DateTime de-identification
        time_shift_config = clr.TimeShiftConfig(method="shift_by_years", min=-5, max=5)
        deid_config = clr.DeIDConfig(time_shift=time_shift_config)

        datetime_deid = clr.DateTimeDeidentifier(
            idconfig=user_id_config,
            global_deid_config=deid_config,
            datetime_column="reg_date_time",
        )
        users_df_deid, deid_ref_dict = datetime_deid.transform(
            users_df_deid, deid_ref_dict
        )

        # Verify DateTime de-identification
        self.assertEqual(users_df_deid.shape, (5, 4))  # Still same shape
        original_dates = users_df["reg_date_time"]
        deid_dates = users_df_deid["reg_date_time"]
        self.assertFalse(original_dates.equals(deid_dates))  # Should be different

        # Step 3: Column dropping
        name_drop = clr.ColumnDropper(
            idconfig=clr.IdentifierConfig(
                name="name", uid="name_drop", description="User name to drop"
            )
        )
        users_df_deid, deid_ref_dict = name_drop.transform(users_df_deid, deid_ref_dict)

        # Verify column dropping
        self.assertEqual(users_df_deid.shape, (5, 3))  # Now 3 columns
        self.assertNotIn("name", users_df_deid.columns)
        self.assertIn("user_id", users_df_deid.columns)
        self.assertIn("reg_date_time", users_df_deid.columns)
        self.assertIn("zipcode", users_df_deid.columns)


if __name__ == "__main__":
    unittest.main()
