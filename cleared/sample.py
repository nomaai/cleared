"""
Sample data for Cleared tutorials and examples.

This module provides sample datasets for demonstrating Cleared functionality
in tutorials and examples.
"""

import pandas as pd
from datetime import datetime


class SampleData:
    """Sample datasets for Cleared tutorials and examples."""

    @property
    def users_single_table(self) -> pd.DataFrame:
        """
        Sample users data for single-table tutorials.

        Returns:
            DataFrame with user data including ID, name, registration datetime, and zipcode

        """
        return pd.DataFrame(
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

    @property
    def users_multi_table(self) -> pd.DataFrame:
        """
        Sample users data for multi-table tutorials.

        Returns:
            DataFrame with user data including ID, name, registration datetime, and zipcode

        """
        return pd.DataFrame(
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

    @property
    def events(self) -> pd.DataFrame:
        """
        Sample events data for multi-table tutorials.

        Returns:
            DataFrame with event data including user_id, event_name, event_value, and event_date_time

        """
        return pd.DataFrame(
            {
                "user_id": [101, 101, 202, 202, 303, 303, 404, 505, 505, 505],
                "event_name": [
                    "sensor_1",
                    "sensor_2",
                    "sensor_1",
                    "sensor_3",
                    "sensor_1",
                    "sensor_2",
                    "sensor_1",
                    "sensor_1",
                    "sensor_2",
                    "sensor_3",
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

    @property
    def events_with_surveys(self) -> pd.DataFrame:
        """
        Sample events data with survey submission dates for filtered de-identification tutorials.

        Returns:
            DataFrame with event data including user_id, event_name, event_value (datetime for surveys), and event_date_time

        """
        return pd.DataFrame(
            {
                "user_id": [
                    101,
                    101,
                    202,
                    202,
                    202,
                    303,
                    303,
                    404,
                    505,
                    505,
                    505,
                    505,
                    303,
                    303,
                    303,
                    303,
                ],
                "event_name": [
                    "sensor_1",
                    "sensor_2",
                    "Survey submission date",
                    "user submitted",
                    "sensor_1",
                    "sensor_3",
                    "sensor_1",
                    "sensor_2",
                    "sensor_1",
                    "sensor_1",
                    "Survey submission date",
                    "user submitted",
                    "sensor_2",
                    "sensor_3",
                    "Survey submission date",
                    "user submitted",
                ],
                "event_value": [
                    "100.0",
                    "250.0",
                    "2023-01-20 10:15:00",  # Survey submission date
                    "101",  # user submitted - contains user_id
                    "50.0",
                    "0.0",
                    "75.0",
                    "300.0",
                    "25.0",
                    "150.0",
                    "2023-02-12 14:30:00",  # Survey submission date
                    "202",  # user submitted - contains user_id
                    "400.0",
                    "0.0",
                    "2023-03-18 09:45:00",  # Survey submission date
                    "303",  # user submitted - contains user_id
                ],
                "event_date_time": [
                    datetime(2023, 1, 10, 8, 30),
                    datetime(2023, 1, 15, 14, 20),
                    datetime(2023, 1, 20, 10, 15),  # Survey submission date
                    datetime(
                        2023, 1, 20, 10, 16
                    ),  # user submitted - right after survey
                    datetime(2023, 2, 5, 9, 45),
                    datetime(2023, 2, 5, 17, 30),
                    datetime(2023, 3, 12, 10, 15),
                    datetime(2023, 3, 12, 15, 45),
                    datetime(2023, 4, 8, 11, 20),
                    datetime(2023, 5, 20, 13, 10),
                    datetime(2023, 2, 12, 14, 30),  # Survey submission date
                    datetime(
                        2023, 2, 12, 14, 31
                    ),  # user submitted - right after survey
                    datetime(2023, 5, 25, 16, 30),
                    datetime(2023, 5, 25, 18, 45),
                    datetime(2023, 3, 18, 9, 45),  # Survey submission date
                    datetime(2023, 3, 18, 9, 46),  # user submitted - right after survey
                ],
            }
        )

    @property
    def orders(self) -> pd.DataFrame:
        """
        Sample orders data for multi-table tutorials.

        Returns:
            DataFrame with order data including user_id, order_id, order_name, and order_date_time

        """
        return pd.DataFrame(
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

    @property
    def multi_table_datasets(self) -> dict[str, pd.DataFrame]:
        """
        All multi-table datasets as a dictionary.

        Returns:
            Dictionary mapping table names to their DataFrames

        """
        return {
            "users": self.users_multi_table,
            "events": self.events,
            "orders": self.orders,
        }


# Create a global instance for easy access
sample_data = SampleData()
