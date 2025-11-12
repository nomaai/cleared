"""Integration tests for transformer pipelines with multiple tables."""

import unittest
import pandas as pd
from datetime import datetime
import random
from cleared.transformers.id import IDDeidentifier
from cleared.transformers.temporal import DateTimeDeidentifier
from cleared.transformers.simple import ColumnDropper
from cleared.config.structure import (
    IdentifierConfig,
    DeIDConfig,
    TimeShiftConfig,
    ClearedConfig,
    ClearedIOConfig,
    PairedIOConfig,
    IOConfig,
)


class TestMultiTableDeidentification(unittest.TestCase):
    """Test de-identification pipeline with multiple related tables."""

    def setUp(self):
        """Set up test data and configuration."""
        # Set random seed for reproducible results
        random.seed(42)

        # Create test data for Users table
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

        # Create test data for Events table
        self.events_df = pd.DataFrame(
            {
                "user_id": [101, 101, 202, 202, 303, 303, 404, 505, 505, 505],
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
                ],
            }
        )

        # Create test data for Orders table
        self.orders_df = pd.DataFrame(
            {
                "user_id": [101, 202, 303, 404, 505, 101, 202, 303],
                "order_id": [1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008],
                "order_name": [
                    "Laptop",
                    "Mouse",
                    "Keyboard",
                    "Monitor",
                    "Headphones",
                    "Charger",
                    "Desk",
                    "Chair",
                ],
                "order_date_time": [
                    datetime(2023, 1, 20, 10, 15),
                    datetime(2023, 2, 10, 14, 30),
                    datetime(2023, 3, 15, 9, 45),
                    datetime(2023, 4, 12, 16, 20),
                    datetime(2023, 5, 30, 11, 55),
                    datetime(2023, 6, 5, 13, 25),
                    datetime(2023, 6, 15, 15, 40),
                    datetime(2023, 7, 2, 12, 10),
                ],
            }
        )

        # Create de-identification configuration
        self.user_id_config = IdentifierConfig(
            name="user_id", uid="user_uid", description="User identifier"
        )

        self.time_shift_config = TimeShiftConfig(method="shift_by_years", min=-5, max=5)

        self.deid_config = DeIDConfig(time_shift=self.time_shift_config)

        # Create IO configuration
        self.io_config = PairedIOConfig(
            input_config=IOConfig(
                io_type="filesystem", configs={"base_path": "/tmp/test_data"}
            ),
            output_config=IOConfig(
                io_type="filesystem", configs={"base_path": "/tmp/test_output"}
            ),
        )

        # Create main configuration
        self.cleared_config = ClearedConfig(
            name="multi_table_test",
            deid_config=self.deid_config,
            io=ClearedIOConfig(
                data=self.io_config,
                deid_ref=self.io_config,
                runtime_io_path="/tmp/runtime",
            ),
        )

    def test_multi_table_deidentification_pipeline(self):
        """Test complete de-identification pipeline with 3 tables."""
        # Create transformers
        id_deid = IDDeidentifier(idconfig=self.user_id_config)
        datetime_deid_reg = DateTimeDeidentifier(
            idconfig=self.user_id_config,
            global_deid_config=self.deid_config,
            datetime_column="reg_date_time",
        )
        datetime_deid_event = DateTimeDeidentifier(
            idconfig=self.user_id_config,
            global_deid_config=self.deid_config,
            datetime_column="event_date_time",
        )
        datetime_deid_order = DateTimeDeidentifier(
            idconfig=self.user_id_config,
            global_deid_config=self.deid_config,
            datetime_column="order_date_time",
        )
        name_drop = ColumnDropper(
            idconfig=IdentifierConfig(
                name="name", uid="name_drop", description="User name to drop"
            )
        )

        # Process Users table
        users_df_deid, users_ref = id_deid.transform(self.users_df, {})
        users_df_deid, users_ref = datetime_deid_reg.transform(users_df_deid, users_ref)
        users_df_deid, users_ref = name_drop.transform(users_df_deid, users_ref)

        # Process Events table
        events_df_deid, events_ref = id_deid.transform(self.events_df, users_ref)
        events_df_deid, events_ref = datetime_deid_event.transform(
            events_df_deid, events_ref
        )

        # Process Orders table
        orders_df_deid, orders_ref = id_deid.transform(self.orders_df, events_ref)
        orders_df_deid, orders_ref = datetime_deid_order.transform(
            orders_df_deid, orders_ref
        )

        # Prepare results in the same format as the engine
        results = {
            "users": {"transformed_data": users_df_deid, "deid_ref_dict": users_ref},
            "events": {"transformed_data": events_df_deid, "deid_ref_dict": events_ref},
            "orders": {"transformed_data": orders_df_deid, "deid_ref_dict": orders_ref},
        }

        # Verify results
        self.assertIsInstance(results, dict)
        self.assertIn("users", results)
        self.assertIn("events", results)
        self.assertIn("orders", results)

        # Check Users table results
        users_result = results["users"]
        self.assertIsInstance(users_result, dict)
        self.assertIn("transformed_data", users_result)
        self.assertIn("deid_ref_dict", users_result)

        users_df_deid = users_result["transformed_data"]
        users_deid_ref = users_result["deid_ref_dict"]

        # Verify Users table transformations
        self.assertEqual(len(users_df_deid), len(self.users_df))
        self.assertNotIn("name", users_df_deid.columns)  # Name column should be dropped
        self.assertIn(
            "user_id", users_df_deid.columns
        )  # user_id should be de-identified
        self.assertIn(
            "reg_date_time", users_df_deid.columns
        )  # reg_date_time should be shifted
        self.assertIn(
            "zipcode", users_df_deid.columns
        )  # zipcode should remain unchanged

        # Verify user_id de-identification
        original_user_ids = set(self.users_df["user_id"])
        deid_user_ids = set(users_df_deid["user_id"])
        # The de-identified IDs should be different from original (they are sequential integers)
        self.assertNotEqual(original_user_ids, deid_user_ids)  # Should be different
        self.assertEqual(len(deid_user_ids), len(original_user_ids))  # Same count

        # Check that de-identified IDs are sequential integers starting from 1
        deid_user_ids_list = sorted(list(deid_user_ids))
        expected_deid_ids = list(range(1, len(original_user_ids) + 1))
        self.assertEqual(deid_user_ids_list, expected_deid_ids)

        # Verify datetime shifting
        original_dates = self.users_df["reg_date_time"]
        deid_dates = users_df_deid["reg_date_time"]
        self.assertFalse(original_dates.equals(deid_dates))  # Should be different

        # Check Events table results
        events_result = results["events"]
        events_df_deid = events_result["transformed_data"]
        events_deid_ref = events_result["deid_ref_dict"]

        # Verify Events table transformations
        self.assertEqual(len(events_df_deid), len(self.events_df))
        self.assertIn("user_id", events_df_deid.columns)
        self.assertIn("event_date_time", events_df_deid.columns)

        # Verify user_id consistency between tables
        events_user_ids = set(events_df_deid["user_id"])
        self.assertEqual(events_user_ids, deid_user_ids)  # Should match users table

        # Check Orders table results
        orders_result = results["orders"]
        orders_df_deid = orders_result["transformed_data"]
        orders_deid_ref = orders_result["deid_ref_dict"]

        # Verify Orders table transformations
        self.assertEqual(len(orders_df_deid), len(self.orders_df))
        self.assertIn("user_id", orders_df_deid.columns)
        self.assertIn("order_date_time", orders_df_deid.columns)

        # Verify user_id consistency between tables
        orders_user_ids = set(orders_df_deid["user_id"])
        self.assertEqual(orders_user_ids, deid_user_ids)  # Should match users table

        # Verify that all user_ids in events and orders exist in users table
        all_events_users = set(events_df_deid["user_id"])
        all_orders_users = set(orders_df_deid["user_id"])
        all_users = set(users_df_deid["user_id"])

        self.assertTrue(all_events_users.issubset(all_users))
        self.assertTrue(all_orders_users.issubset(all_users))

        # Verify that de-identification reference dictionaries are consistent
        # All tables should have the same user_id mappings
        users_id_ref = users_deid_ref.get("user_uid")
        events_id_ref = events_deid_ref.get("user_uid")
        orders_id_ref = orders_deid_ref.get("user_uid")

        self.assertIsNotNone(users_id_ref)
        self.assertIsNotNone(events_id_ref)
        self.assertIsNotNone(orders_id_ref)

        # All should have the same mappings
        pd.testing.assert_frame_equal(users_id_ref, events_id_ref)
        pd.testing.assert_frame_equal(users_id_ref, orders_id_ref)

        # Verify datetime reference dictionaries are consistent
        users_time_ref = users_deid_ref.get("user_uid_shift")
        events_time_ref = events_deid_ref.get("user_uid_shift")
        orders_time_ref = orders_deid_ref.get("user_uid_shift")

        self.assertIsNotNone(users_time_ref)
        self.assertIsNotNone(events_time_ref)
        self.assertIsNotNone(orders_time_ref)

        # All should have the same time shift mappings
        pd.testing.assert_frame_equal(users_time_ref, events_time_ref)
        pd.testing.assert_frame_equal(users_time_ref, orders_time_ref)

        # Verify that time shifts are within the expected range (-5 to 5 years)
        time_shift_values = users_time_ref["user_uid_shift"]
        for shift in time_shift_values:
            self.assertGreaterEqual(shift, -5)  # At least -5 years
            self.assertLessEqual(shift, 5)  # At most 5 years

        print("\n=== De-identification Results ===")
        print(
            f"Users table: {len(users_df_deid)} rows, columns: {list(users_df_deid.columns)}"
        )
        print(
            f"Events table: {len(events_df_deid)} rows, columns: {list(events_df_deid.columns)}"
        )
        print(
            f"Orders table: {len(orders_df_deid)} rows, columns: {list(orders_df_deid.columns)}"
        )
        print(f"Original user IDs: {sorted(original_user_ids)}")
        print(f"De-identified user IDs: {sorted(deid_user_ids)}")
        print(
            f"Time shift range: {time_shift_values.min()} to {time_shift_values.max()} years"
        )

    def test_data_consistency_across_tables(self):
        """Test that data relationships are preserved after de-identification."""
        # This test focuses on ensuring referential integrity is maintained

        # Create a simpler test case with known relationships
        users_simple = pd.DataFrame(
            {
                "user_id": [1, 2],
                "name": ["Alice", "Bob"],
                "reg_date_time": [datetime(2020, 1, 1), datetime(2021, 1, 1)],
                "zipcode": ["10001", "20001"],
            }
        )

        events_simple = pd.DataFrame(
            {
                "user_id": [1, 1, 2],
                "event_name": ["login", "purchase", "login"],
                "event_value": [100.0, 200.0, 150.0],
                "event_date_time": [
                    datetime(2023, 1, 1),
                    datetime(2023, 1, 2),
                    datetime(2023, 1, 3),
                ],
            }
        )

        orders_simple = pd.DataFrame(
            {
                "user_id": [1, 2],
                "order_id": [1001, 1002],
                "order_name": ["Laptop", "Mouse"],
                "order_date_time": [datetime(2023, 2, 1), datetime(2023, 2, 2)],
            }
        )

        # Create transformers
        id_deid = IDDeidentifier(idconfig=self.user_id_config)
        datetime_deid = DateTimeDeidentifier(
            idconfig=self.user_id_config,
            global_deid_config=self.deid_config,
            datetime_column="reg_date_time",
        )
        name_drop = ColumnDropper(
            idconfig=IdentifierConfig(
                name="name", uid="name_drop", description="Name to drop"
            )
        )

        # Process users table
        users_df_deid, users_ref = id_deid.transform(users_simple, {})
        users_df_deid, users_ref = datetime_deid.transform(users_df_deid, users_ref)
        users_df_deid, users_ref = name_drop.transform(users_df_deid, users_ref)

        # Process events table
        events_df_deid, events_ref = id_deid.transform(events_simple, users_ref)
        events_datetime_deid = DateTimeDeidentifier(
            idconfig=self.user_id_config,
            global_deid_config=self.deid_config,
            datetime_column="event_date_time",
        )
        events_df_deid, events_ref = events_datetime_deid.transform(
            events_df_deid, events_ref
        )

        # Process orders table
        orders_df_deid, orders_ref = id_deid.transform(orders_simple, events_ref)
        orders_datetime_deid = DateTimeDeidentifier(
            idconfig=self.user_id_config,
            global_deid_config=self.deid_config,
            datetime_column="order_date_time",
        )
        orders_df_deid, orders_ref = orders_datetime_deid.transform(
            orders_df_deid, orders_ref
        )

        # Verify referential integrity
        users_user_ids = set(users_df_deid["user_id"])
        events_user_ids = set(events_df_deid["user_id"])
        orders_user_ids = set(orders_df_deid["user_id"])

        # All user_ids in events and orders should exist in users
        self.assertTrue(events_user_ids.issubset(users_user_ids))
        self.assertTrue(orders_user_ids.issubset(users_user_ids))

        # Check that events user_ids are correctly mapped
        for event_user_id in events_user_ids:
            self.assertIn(event_user_id, users_user_ids)

        # Check that orders user_ids are correctly mapped
        for order_user_id in orders_user_ids:
            self.assertIn(order_user_id, users_user_ids)

        print("\n=== Referential Integrity Test ===")
        print(f"Users user_ids: {sorted(users_user_ids)}")
        print(f"Events user_ids: {sorted(events_user_ids)}")
        print(f"Orders user_ids: {sorted(orders_user_ids)}")
        print(
            f"All events users exist in users: {events_user_ids.issubset(users_user_ids)}"
        )
        print(
            f"All orders users exist in users: {orders_user_ids.issubset(users_user_ids)}"
        )


if __name__ == "__main__":
    unittest.main()
