"""
Example usage of TablePipeline and GlobalPipeline classes.

This example demonstrates how to use the new pipeline classes for
data de-identification workflows with different scopes and configurations.
"""

import pandas as pd
from omegaconf import OmegaConf
from cleared.transformers import TablePipeline, GlobalPipeline
from cleared.transformers import IDDeidentifier, DateTimeDeidentifier
from cleared.config.structure import ClearedConfig, IdentifierConfig


def example_table_pipeline():
    """Demonstrate TablePipeline for single table processing."""
    print("=== TablePipeline Example ===")

    # Load configuration
    config = OmegaConf.load("examples/pipeline_config.yaml")
    cleared_config = ClearedConfig(**config[0])  # First config is for TablePipeline

    # Create sample data
    sample_data = pd.DataFrame(
        {
            "patient_id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
            "admission_date": [
                "2023-01-15",
                "2023-02-20",
                "2023-03-10",
                "2023-04-05",
                "2023-05-12",
            ],
            "age": [25, 30, 35, 28, 42],
        }
    )

    # Save sample data to file (simulating data source)
    import os

    os.makedirs("data", exist_ok=True)
    sample_data.to_csv("data/patients.csv", index=False)

    try:
        # Create table pipeline using the new config structure
        table_config = cleared_config.tables["patients"]
        with TablePipeline(table_config, cleared_config.io) as pipeline:
            print(f"Processing table: {pipeline.table_name}")
            print(f"IO type: {pipeline.io_config.io_type}")

            # Create transformers using the new config structure
            id_config = table_config.transformers[0].configs
            date_config = table_config.transformers[1].configs

            id_transformer = IDDeidentifier(id_config["idconfig"])
            date_transformer = DateTimeDeidentifier(
                idconfig=date_config["idconfig"],
                time_shift_method=date_config["time_shift_method"],
                datetime_column=date_config["datetime_column"],
            )

            pipeline.add_transformer(id_transformer)
            pipeline.add_transformer(date_transformer)

            # Transform data (will read from file)
            deid_df, deid_ref_dict = pipeline.transform()

            print(f"Original data shape: {sample_data.shape}")
            print(f"De-identified data shape: {deid_df.shape}")
            print(f"De-identification reference keys: {list(deid_ref_dict.keys())}")

            print("\nOriginal data:")
            print(sample_data.head())

            print("\nDe-identified data:")
            print(deid_df.head())

            print("\nDe-identification reference:")
            for key, ref_df in deid_ref_dict.items():
                print(f"{key}: {ref_df.shape}")
                print(ref_df.head())

            # Write de-identified data back
            pipeline.write_deid_table(deid_df)
            print("\nDe-identified data written to data source")

    except Exception as e:
        print(f"Error in TablePipeline example: {e}")
        import traceback

        traceback.print_exc()

    finally:
        # Cleanup
        import shutil

        if os.path.exists("data"):
            shutil.rmtree("data")


def example_global_pipeline():
    """Demonstrate GlobalPipeline for global de-identification."""
    print("\n=== GlobalPipeline Example ===")

    # Load configuration
    config = OmegaConf.load("examples/pipeline_config.yaml")
    cleared_config = ClearedConfig(**config[1])  # Second config is for GlobalPipeline

    # Create sample data
    sample_data = pd.DataFrame(
        {
            "patient_id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
            "admission_date": [
                "2023-01-15",
                "2023-02-20",
                "2023-03-10",
                "2023-04-05",
                "2023-05-12",
            ],
            "age": [25, 30, 35, 28, 42],
        }
    )

    # Save sample data to file
    import os

    os.makedirs("data", exist_ok=True)
    sample_data.to_csv("data/patients.csv", index=False)

    try:
        # Create global pipeline using the new config structure
        with GlobalPipeline(cleared_config) as pipeline:
            print(f"Global pipeline with IO type: {pipeline.io_config.io_type}")
            print(f"De-identification reference table: {pipeline.deid_ref_table_name}")

            # Create transformers using the new config structure
            table_config = cleared_config.tables["patients"]
            id_config = table_config.transformers[0].configs
            date_config = table_config.transformers[1].configs

            id_transformer = IDDeidentifier(id_config["idconfig"])
            date_transformer = DateTimeDeidentifier(
                idconfig=date_config["idconfig"],
                time_shift_method=date_config["time_shift_method"],
                datetime_column=date_config["datetime_column"],
            )

            pipeline.add_transformer(id_transformer)
            pipeline.add_transformer(date_transformer)

            # Transform data
            deid_df, deid_ref_dict = pipeline.transform(sample_data)

            print(f"Original data shape: {sample_data.shape}")
            print(f"De-identified data shape: {deid_df.shape}")
            print(
                f"Global de-identification reference keys: {list(deid_ref_dict.keys())}"
            )

            print("\nOriginal data:")
            print(sample_data.head())

            print("\nDe-identified data:")
            print(deid_df.head())

            print("\nGlobal de-identification reference:")
            for key, ref_df in deid_ref_dict.items():
                print(f"{key}: {ref_df.shape}")
                print(ref_df.head())

            # Test loading existing reference
            loaded_ref = pipeline.load_deid_ref()
            if loaded_ref is not None:
                print(f"\nLoaded existing reference: {loaded_ref.shape}")
            else:
                print("\nNo existing reference found")

    except Exception as e:
        print(f"Error in GlobalPipeline example: {e}")
        import traceback

        traceback.print_exc()

    finally:
        # Cleanup
        import shutil

        if os.path.exists("data"):
            shutil.rmtree("data")


def example_sql_pipeline():
    """Demonstrate GlobalPipeline with SQL configuration."""
    print("\n=== SQL GlobalPipeline Example ===")

    # Load configuration
    config = OmegaConf.load("examples/pipeline_config.yaml")
    cleared_config = ClearedConfig(
        **config[2]
    )  # Third config is for SQL GlobalPipeline

    print("SQL GlobalPipeline configuration:")
    print(f"IO type: {cleared_config.io.data.input_config.io_type}")
    print(
        f"Database URL: {cleared_config.io.data.input_config.configs['database_url']}"
    )
    print(
        f"De-identification reference table: {cleared_config.tables['patients'].name}"
    )

    # Note: This would require a real database connection to work
    print("Note: This example requires a real database connection to execute")


def example_custom_io_config():
    """Demonstrate creating custom IO configuration."""
    print("\n=== Custom IO Configuration Example ===")

    from cleared.config.structure import (
        IOConfig,
        DeIDConfig,
        ClearedConfig,
        PairedIOConfig,
        ClearedIOConfig,
    )

    # Create custom IO configuration
    input_io_config = IOConfig(
        io_type="filesystem",
        configs={
            "base_path": "./custom_data",
            "file_format": "parquet",
            "encoding": "utf-8",
        },
    )

    output_io_config = IOConfig(
        io_type="filesystem",
        configs={
            "base_path": "./custom_data",
            "file_format": "parquet",
            "encoding": "utf-8",
        },
    )

    # Create paired IO configuration
    paired_io_config = PairedIOConfig(
        input_config=input_io_config, output_config=output_io_config
    )

    # Create cleared IO configuration
    cleared_io_config = ClearedIOConfig(
        data=paired_io_config, deid_ref=paired_io_config
    )

    # Create de-identification configuration
    deid_config = DeIDConfig(
        global_uids={
            "patient_id": IdentifierConfig(
                name="patient_id",
                uid="patient_id",
                description="Unique patient identifier",
            )
        },
        time_shift=None,
    )

    # Create complete cleared configuration
    cleared_config = ClearedConfig(
        deid_config=deid_config, io=cleared_io_config, tables={}
    )

    print("Custom IO configuration created:")
    print(f"IO type: {cleared_config.io.data.input_config.io_type}")
    print(f"Base path: {cleared_config.io.data.input_config.configs['base_path']}")
    print(f"File format: {cleared_config.io.data.input_config.configs['file_format']}")
    print(
        f"De-identification config global UIDs: {list(cleared_config.deid_config.global_uids.keys())}"
    )


if __name__ == "__main__":
    # Run examples
    example_table_pipeline()
    example_global_pipeline()
    example_sql_pipeline()
    example_custom_io_config()
