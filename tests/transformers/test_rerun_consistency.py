"""Integration tests for rerun de-identification consistency tutorial."""

import unittest
import tempfile
import shutil
import pandas as pd
from datetime import datetime
from pathlib import Path

import cleared as clr
from cleared.cli.utils import load_config_from_file


class TestRerunConsistencyTutorial(unittest.TestCase):
    """Test the rerun de-identification consistency tutorial."""

    def setUp(self):
        """Set up test environment."""
        # Create temporary directory
        self.test_dir = Path(tempfile.mkdtemp())
        self.input_dir = self.test_dir / "input"
        self.output_dir = self.test_dir / "output"
        self.deid_ref_v1_dir = self.test_dir / "deid_ref_tables" / "v1"
        self.deid_ref_v2_dir = self.test_dir / "deid_ref_tables" / "v2"
        self.runtime_dir = self.test_dir / "runtime"

        # Create directories
        for dir_path in [
            self.input_dir,
            self.output_dir,
            self.deid_ref_v1_dir,
            self.deid_ref_v2_dir,
            self.runtime_dir,
        ]:
            dir_path.mkdir(parents=True, exist_ok=True)

        # For v1, the deid_ref directory should exist but be empty initially
        # For v2, it will read from v1 directory
        # Ensure v1 directory exists (required for first run)
        self.deid_ref_v1_dir.mkdir(parents=True, exist_ok=True)

        # Copy configuration files and update paths to be absolute
        examples_dir = Path(__file__).parent.parent.parent / "examples"

        # Copy v1 config and update paths
        with open(examples_dir / "users_config_v1.yaml") as f:
            v1_content = f.read()
        v1_content = v1_content.replace("./input", str(self.input_dir))
        v1_content = v1_content.replace("./output", str(self.output_dir))
        v1_content = v1_content.replace(
            "./deid_ref_tables/v1", str(self.deid_ref_v1_dir)
        )
        v1_content = v1_content.replace("./runtime", str(self.runtime_dir))
        with open(self.test_dir / "users_config_v1.yaml", "w") as f:
            f.write(v1_content)

        # Copy v2 config and update paths
        with open(examples_dir / "users_config_v2.yaml") as f:
            v2_content = f.read()
        v2_content = v2_content.replace("./input", str(self.input_dir))
        v2_content = v2_content.replace("./output", str(self.output_dir))
        v2_content = v2_content.replace(
            "./deid_ref_tables/v1", str(self.deid_ref_v1_dir)
        )
        v2_content = v2_content.replace(
            "./deid_ref_tables/v2", str(self.deid_ref_v2_dir)
        )
        v2_content = v2_content.replace("./runtime", str(self.runtime_dir))
        with open(self.test_dir / "users_config_v2.yaml", "w") as f:
            f.write(v2_content)

        # Create initial data
        self.initial_users = clr.sample_data.users_single_table
        self.initial_users.to_csv(str(self.input_dir / "users.csv"), index=False)

        # Create new users data
        self.new_users = pd.DataFrame(
            {
                "user_id": [606, 707, 808, 909, 1010],
                "name": [
                    "Frank Miller",
                    "Grace Lee",
                    "Henry Davis",
                    "Ivy Chen",
                    "Jack Wilson",
                ],
                "reg_date_time": [
                    datetime(2023, 2, 14, 8, 30),
                    datetime(2023, 4, 10, 12, 15),
                    datetime(2023, 6, 5, 16, 45),
                    datetime(2023, 8, 20, 9, 20),
                    datetime(2023, 10, 12, 14, 10),
                ],
                "zipcode": ["20001", "30002", "40003", "50004", "60005"],
            }
        )

    def tearDown(self):
        """Clean up test environment."""
        shutil.rmtree(self.test_dir)

    def test_initial_deidentification_v1(self):
        """Test initial de-identification with v1 configuration."""
        # Load v1 configuration
        config = load_config_from_file(self.test_dir / "users_config_v1.yaml")

        # Verify configuration
        self.assertEqual(config.name, "users_deid_pipeline_v1")
        self.assertIn("users", config.tables)

        # Create engine and run
        engine = clr.ClearedEngine.from_config(config)
        results = engine.run()

        # Verify results
        self.assertTrue(results.success)
        # Results use pipeline UIDs as keys, not table names
        self.assertEqual(len(results.results), 1)

        # Check output file exists
        output_file = self.output_dir / "users.csv"
        self.assertTrue(output_file.exists())

        # Check de-identification reference files exist
        v1_user_ref = self.deid_ref_v1_dir / "user_uid.csv"
        v1_shift_ref = self.deid_ref_v1_dir / "user_uid_shift.csv"
        self.assertTrue(v1_user_ref.exists())
        self.assertTrue(v1_shift_ref.exists())

        # Verify de-identified data structure
        deid_data = pd.read_csv(output_file)
        self.assertEqual(len(deid_data), 5)  # 5 original users
        self.assertIn("user_id", deid_data.columns)
        self.assertIn("reg_date_time", deid_data.columns)
        self.assertIn("zipcode", deid_data.columns)
        self.assertNotIn("name", deid_data.columns)  # Should be dropped

        # Verify user ID mappings
        user_ref = pd.read_csv(v1_user_ref)
        self.assertEqual(len(user_ref), 5)
        self.assertEqual(set(user_ref["user_uid"]), {101, 202, 303, 404, 505})
        self.assertEqual(set(user_ref["user_uid__deid"]), {1, 2, 3, 4, 5})

    def test_add_new_users_and_rerun_v2(self):
        """Test adding new users and rerunning with v2 configuration."""
        # First run v1
        config_v1 = load_config_from_file(self.test_dir / "users_config_v1.yaml")
        engine_v1 = clr.ClearedEngine.from_config(config_v1)
        results_v1 = engine_v1.run()
        self.assertTrue(results_v1.success)

        # Add new users to input data
        combined_users = pd.concat(
            [self.initial_users, self.new_users], ignore_index=True
        )
        combined_users.to_csv(str(self.input_dir / "users.csv"), index=False)

        # Load v2 configuration
        config_v2 = load_config_from_file(self.test_dir / "users_config_v2.yaml")

        # Verify v2 configuration reads from v1 and writes to v2
        self.assertEqual(
            config_v2.io.deid_ref.input_config.configs["base_path"],
            str(self.deid_ref_v1_dir),
        )
        self.assertEqual(
            config_v2.io.deid_ref.output_config.configs["base_path"],
            str(self.deid_ref_v2_dir),
        )

        # Run v2
        engine_v2 = clr.ClearedEngine.from_config(config_v2)
        results_v2 = engine_v2.run()

        # Verify results
        self.assertTrue(results_v2.success)
        # Results use pipeline UIDs as keys, not table names
        self.assertEqual(len(results_v2.results), 1)

        # Check output file
        output_file = self.output_dir / "users.csv"
        self.assertTrue(output_file.exists())

        # Verify de-identified data has all 10 users
        deid_data = pd.read_csv(output_file)
        self.assertEqual(len(deid_data), 10)  # 5 original + 5 new users

        # Check v2 reference files exist
        v2_user_ref = self.deid_ref_v2_dir / "user_uid.csv"
        v2_shift_ref = self.deid_ref_v2_dir / "user_uid_shift.csv"
        self.assertTrue(v2_user_ref.exists())
        self.assertTrue(v2_shift_ref.exists())

    def test_consistency_between_versions(self):
        """Test that existing user IDs remain consistent between v1 and v2."""
        # Run v1
        config_v1 = load_config_from_file(self.test_dir / "users_config_v1.yaml")
        engine_v1 = clr.ClearedEngine.from_config(config_v1)
        results_v1 = engine_v1.run()
        self.assertTrue(results_v1.success)

        # Add new users and run v2
        combined_users = pd.concat(
            [self.initial_users, self.new_users], ignore_index=True
        )
        combined_users.to_csv(str(self.input_dir / "users.csv"), index=False)

        config_v2 = load_config_from_file(self.test_dir / "users_config_v2.yaml")
        engine_v2 = clr.ClearedEngine.from_config(config_v2)
        results_v2 = engine_v2.run()
        self.assertTrue(results_v2.success)

        # Load reference tables
        v1_user_ref = pd.read_csv(self.deid_ref_v1_dir / "user_uid.csv")
        v2_user_ref = pd.read_csv(self.deid_ref_v2_dir / "user_uid.csv")

        # Create mapping dictionaries
        # The deid_ref files use idconfig.uid for original values and idconfig.deid_uid() for de-identified values
        v1_mappings = dict(
            zip(v1_user_ref["user_uid"], v1_user_ref["user_uid__deid"], strict=False)
        )
        v2_mappings = dict(
            zip(v2_user_ref["user_uid"], v2_user_ref["user_uid__deid"], strict=False)
        )

        # Check consistency for original users
        original_user_ids = [101, 202, 303, 404, 505]
        for orig_id in original_user_ids:
            v1_deid_id = v1_mappings.get(orig_id)
            v2_deid_id = v2_mappings.get(orig_id)
            self.assertEqual(
                v1_deid_id,
                v2_deid_id,
                f"User {orig_id} mapping inconsistent: {v1_deid_id} vs {v2_deid_id}",
            )

        # Check that new users got new sequential IDs
        new_user_ids = [606, 707, 808, 909, 1010]
        # The actual order depends on how the data is processed, so we just check they got IDs 6-10
        new_user_deid_ids = [v2_mappings.get(new_id) for new_id in new_user_ids]
        expected_range = set(range(6, 11))  # IDs 6-10
        actual_range = set(new_user_deid_ids)

        self.assertEqual(
            actual_range,
            expected_range,
            f"New users got unexpected de-identified IDs: {actual_range} vs expected {expected_range}",
        )

        # All new users should have de-identified IDs
        for new_id in new_user_ids:
            actual_deid = v2_mappings.get(new_id)
            self.assertIsNotNone(
                actual_deid, f"New user {new_id} missing de-identified ID"
            )

        # Verify total mappings
        self.assertEqual(len(v2_mappings), 10)  # 5 original + 5 new

    def test_tutorial_step_by_step(self):
        """Test the complete tutorial step by step."""
        # Step 1: Initial de-identification
        print("Step 1: Running initial de-identification (v1)...")
        config_v1 = load_config_from_file(self.test_dir / "users_config_v1.yaml")
        engine_v1 = clr.ClearedEngine.from_config(config_v1)
        results_v1 = engine_v1.run()

        self.assertTrue(results_v1.success)
        print(f"  ✅ v1 completed: {len(results_v1.results)} tables processed")

        # Step 2: Add new users
        print("Step 2: Adding new users...")
        combined_users = pd.concat(
            [self.initial_users, self.new_users], ignore_index=True
        )
        combined_users.to_csv(str(self.input_dir / "users.csv"), index=False)
        print(
            f"  ✅ Added {len(self.new_users)} new users, total: {len(combined_users)}"
        )

        # Step 3: Rerun with v2
        print("Step 3: Running rerun de-identification (v2)...")
        config_v2 = load_config_from_file(self.test_dir / "users_config_v2.yaml")
        engine_v2 = clr.ClearedEngine.from_config(config_v2)
        results_v2 = engine_v2.run()

        self.assertTrue(results_v2.success)
        print(f"  ✅ v2 completed: {len(results_v2.results)} tables processed")

        # Step 4: Verify consistency
        print("Step 4: Verifying consistency...")
        v1_user_ref = pd.read_csv(self.deid_ref_v1_dir / "user_uid.csv")
        v2_user_ref = pd.read_csv(self.deid_ref_v2_dir / "user_uid.csv")

        v1_mappings = dict(
            zip(v1_user_ref["user_uid"], v1_user_ref["user_uid__deid"], strict=False)
        )
        v2_mappings = dict(
            zip(v2_user_ref["user_uid"], v2_user_ref["user_uid__deid"], strict=False)
        )

        # Check consistency
        original_user_ids = [101, 202, 303, 404, 505]
        consistent_count = 0
        for orig_id in original_user_ids:
            if v1_mappings.get(orig_id) == v2_mappings.get(orig_id):
                consistent_count += 1

        print(
            f"  ✅ Consistency check: {consistent_count}/{len(original_user_ids)} original users consistent"
        )
        self.assertEqual(consistent_count, len(original_user_ids))

        # Check new users
        new_user_ids = [606, 707, 808, 909, 1010]
        new_mappings_count = sum(1 for new_id in new_user_ids if new_id in v2_mappings)
        print(
            f"  ✅ New users: {new_mappings_count}/{len(new_user_ids)} properly mapped"
        )
        self.assertEqual(new_mappings_count, len(new_user_ids))

        print("  ✅ Tutorial completed successfully!")


if __name__ == "__main__":
    unittest.main()
