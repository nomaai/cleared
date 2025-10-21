"""
Example usage of data loaders in cleared.

This example demonstrates how to use the BaseDataLoader and its concrete
implementations for file system and SQL-based data sources.
"""

from omegaconf import OmegaConf
import pandas as pd
from cleared.io import FileSystemDataLoader, SQLDataLoader
from cleared.config.structure import IOConfig


def example_filesystem_loader():
    """Demonstrate FileSystemDataLoader usage."""
    print("=== FileSystemDataLoader Example ===")

    # Load configuration
    config = OmegaConf.load("examples/data_loader_config.yaml")
    fs_config = IOConfig(**config[0])  # First config is for filesystem

    # Create sample data
    import os

    os.makedirs("data", exist_ok=True)
    sample_data = pd.DataFrame(
        {
            "patient_id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
            "age": [25, 30, 35, 28, 42],
        }
    )
    sample_data.to_csv("data/patients.csv", index=False)

    # Create loader
    with FileSystemDataLoader(fs_config) as loader:
        print(f"Data source type: {loader.data_source_type}")
        print(f"Available tables: {loader.list_tables()}")

        # Example: Read a table
        try:
            df = loader.read_table("patients")
            print(f"Read {len(df)} rows from patients table")
            print(df.head())
        except Exception as e:
            print(f"Error reading table: {e}")

        # Example: Write de-identified data
        try:
            deid_df = df.copy()  # In real usage, this would be de-identified
            deid_df["patient_id"] = deid_df["patient_id"] + 10000  # Simple example
            loader.write_deid_table(deid_df, "patients_deid")
            print("Successfully wrote de-identified data")
        except Exception as e:
            print(f"Error writing table: {e}")

    # Cleanup
    import shutil

    if os.path.exists("data"):
        shutil.rmtree("data")


def example_sql_loader():
    """Demonstrate SQLDataLoader usage."""
    print("\n=== SQLDataLoader Example ===")

    # Load configuration
    config = OmegaConf.load("examples/data_loader_config.yaml")
    sql_config = IOConfig(**config[1])  # Second config is for SQL

    # Create loader
    with SQLDataLoader(sql_config) as loader:
        print(f"Data source type: {loader.data_source_type}")

        try:
            # List available tables
            tables = loader.list_tables()
            print(f"Available tables: {tables}")

            # Example: Read a table
            df = loader.read_table("patients")
            print(f"Read {len(df)} rows from patients table")
            print(df.head())

            # Example: Write de-identified data
            deid_df = df.copy()
            deid_df["patient_id"] = deid_df["patient_id"] + 10000
            loader.write_deid_table(deid_df, "patients_deid")
            print("Successfully wrote de-identified data to SQL")

        except Exception as e:
            print(f"Error with SQL operations: {e}")


if __name__ == "__main__":
    # Run examples
    example_filesystem_loader()
    example_sql_loader()
