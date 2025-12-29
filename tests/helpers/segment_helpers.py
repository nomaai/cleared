"""Helper functions for creating multi-segment test data."""

from __future__ import annotations

from pathlib import Path
import pandas as pd

from cleared.config.structure import ClearedConfig, ClearedIOConfig, DeIDConfig


def create_segment_directory(
    base_path: Path,
    table_name: str,
    segments: list[pd.DataFrame],
    file_format: str = "csv",
) -> Path:
    """
    Create a directory with multiple segment files.

    Args:
        base_path: Base directory path
        table_name: Name of the table (directory name)
        segments: List of DataFrames, one per segment
        file_format: File format for segments (csv, parquet, json, etc.)

    Returns:
        Path to the created directory

    """
    table_dir = base_path / table_name
    table_dir.mkdir(parents=True, exist_ok=True)

    for idx, segment_df in enumerate(segments, start=1):
        if file_format == "csv":
            segment_file = table_dir / f"segment{idx}.csv"
            segment_df.to_csv(segment_file, index=False)
        elif file_format == "parquet":
            segment_file = table_dir / f"segment{idx}.parquet"
            segment_df.to_parquet(segment_file, index=False)
        elif file_format == "json":
            segment_file = table_dir / f"segment{idx}.json"
            segment_df.to_json(segment_file, orient="records", index=False)
        else:
            raise ValueError(f"Unsupported file format: {file_format}")

    return table_dir


def create_multi_segment_test_data(
    num_segments: int = 3,
    rows_per_segment: int = 100,
    start_user_id: int = 1,
) -> list[pd.DataFrame]:
    """
    Generate realistic test data split across segments.

    Args:
        num_segments: Number of segments to create
        rows_per_segment: Number of rows per segment
        start_user_id: Starting user_id value

    Returns:
        List of DataFrames, one per segment

    """
    segments = []
    current_user_id = start_user_id

    for _segment_idx in range(num_segments):
        segment_data = []
        for row_idx in range(rows_per_segment):
            user_id = current_user_id + row_idx
            segment_data.append(
                {
                    "user_id": user_id,
                    "name": f"User_{user_id}",
                    "email": f"user_{user_id}@example.com",
                    "age": 20 + (user_id % 50),
                }
            )
        segments.append(pd.DataFrame(segment_data))
        current_user_id += rows_per_segment

    return segments


def create_example_config(
    base_path: Path,
    name: str = "multi_segment_example",
) -> ClearedConfig:
    """
    Generate example ClearedConfig for multi-segment testing.

    Args:
        base_path: Base path for test data
        name: Name of the configuration

    Returns:
        ClearedConfig instance

    """
    from cleared.config.structure import (
        IOConfig,
        PairedIOConfig,
        TableConfig,
        TransformerConfig,
    )

    input_dir = base_path / "input"
    output_dir = base_path / "output"
    deid_ref_input_dir = base_path / "deid_ref_input"
    deid_ref_output_dir = base_path / "deid_ref_output"
    runtime_dir = base_path / "runtime"

    return ClearedConfig(
        name=name,
        deid_config=DeIDConfig(),
        io=ClearedIOConfig(
            data=PairedIOConfig(
                input_config=IOConfig(
                    io_type="filesystem",
                    configs={
                        "base_path": str(input_dir),
                        "file_format": "csv",
                    },
                ),
                output_config=IOConfig(
                    io_type="filesystem",
                    configs={
                        "base_path": str(output_dir),
                        "file_format": "csv",
                    },
                ),
            ),
            deid_ref=PairedIOConfig(
                input_config=IOConfig(
                    io_type="filesystem",
                    configs={"base_path": str(deid_ref_input_dir)},
                ),
                output_config=IOConfig(
                    io_type="filesystem",
                    configs={"base_path": str(deid_ref_output_dir)},
                ),
            ),
            runtime_io_path=str(runtime_dir),
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
        },
    )
