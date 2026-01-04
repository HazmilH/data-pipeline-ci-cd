"""
Unit tests for DataPipeline
"""

import pandas as pd
import pytest
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


def test_extract_file_not_found():
    """Test extraction with non-existent file."""
    pipeline = DataPipeline()
    with pytest.raises(FileNotFoundError):
        pipeline.extract("/nonexistent/file.csv")


def test_transform(pipeline, sample_dataframe):
    """Test data transformation."""
    transformed = pipeline.transform(sample_dataframe)

    assert "total_value" in transformed.columns
    assert "processed_at" in transformed.columns

    # Check calculation
    expected = transformed["amount"] * transformed["quantity"]
    pd.testing.assert_series_equal(
        transformed["total_value"], expected, check_names=False
    )


def test_load(pipeline, sample_dataframe):
    """Test data loading."""
    transformed = pipeline.transform(sample_dataframe)
    success = pipeline.load(transformed, "test_table")

    assert success is True

    # Verify data was loaded
    query = "SELECT COUNT(*) as count FROM test_table"
    result = pd.read_sql(query, pipeline.engine)
    assert result.iloc[0]["count"] == 3


def test_full_pipeline(pipeline, temp_csv_file):
    """Test complete pipeline."""
    results = pipeline.run_pipeline(temp_csv_file, "test_output")

    assert results["success"] is True
    assert results["rows_processed"] == 3
    assert "duration_seconds" in results


def test_validation(pipeline, sample_dataframe):
    """Test output validation."""
    transformed = pipeline.transform(sample_dataframe)
    pipeline.load(transformed, "validation_table")

    is_valid, message = pipeline.validate_output("validation_table")

    assert is_valid is True
    assert "passed" in message.lower()


def test_validation_empty():
    """Test validation with empty table."""
    pipeline = DataPipeline(db_url="sqlite:///:memory:")

    # Create empty table
    empty_df = pd.DataFrame({"col": []})
    empty_df.to_sql("empty_table", pipeline.engine, if_exists="replace")

    is_valid, message = pipeline.validate_output("empty_table")

    assert is_valid is False
    assert "no data" in message.lower() or "failed" in message.lower()
