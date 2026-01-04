"""
Unit tests for DataPipeline
"""

import pytest
import pandas as pd
from data_pipeline import DataPipeline


def test_initialization():
    """Test pipeline initialization."""
    pipeline = DataPipeline()
    assert pipeline is not None
    assert hasattr(pipeline, "engine")


def test_extract(temp_csv_file):
    """Test data extraction."""
    pipeline = DataPipeline()
    df = pipeline.extract(temp_csv_file)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 3
    assert "customer_id" in df.columns


def test_transform(pipeline, sample_dataframe):
    """Test data transformation."""
    transformed = pipeline.transform(sample_dataframe)
    assert "total_value" in transformed.columns
    assert "processed_at" in transformed.columns


def test_load(pipeline, sample_dataframe):
    """Test data loading."""
    transformed = pipeline.transform(sample_dataframe)
    success = pipeline.load(transformed, "test_table")
    assert success is True


def test_full_pipeline(pipeline, temp_csv_file):
    """Test complete pipeline."""
    results = pipeline.run_pipeline(temp_csv_file, "test_output")
    assert results["success"] is True
    assert results["rows_processed"] == 3
