"""
Pytest fixtures
"""
import pytest
import pandas as pd
import numpy as np
import tempfile
import os


@pytest.fixture
def sample_dataframe():
    """Create sample DataFrame."""
    return pd.DataFrame({
        'customer_id': [1, 2, 3],
        'amount': [100.0, 200.0, 300.0],
        'quantity': [2, 1, 3],
        'product': ['A', 'B', 'C']
    })


@pytest.fixture
def temp_csv_file(sample_dataframe):
    """Create temporary CSV file."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        sample_dataframe.to_csv(f.name, index=False)
        yield f.name
    if os.path.exists(f.name):
        os.remove(f.name)


@pytest.fixture
def pipeline():
    """Create DataPipeline instance."""
    from data_pipeline import DataPipeline
    return DataPipeline(db_url='sqlite:///:memory:')
