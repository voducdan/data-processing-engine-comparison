"""Pytest configuration and fixtures."""

import pytest
import sys
from pathlib import Path

# Add src to Python path for imports
src_path = Path(__file__).parent / "src"
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

def pytest_collection_modifyitems(config, items):
    """Configure files to ignore during test collection."""
    # This hook can be used to modify the collected items if needed
    pass

@pytest.fixture
def sample_data():
    """Provide sample data for testing."""
    return {
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
        'age': [25, 30, 35, 40, 45],
        'salary': [50000, 60000, 70000, 80000, 90000]
    }

@pytest.fixture  
def temp_data_file(tmp_path, sample_data):
    """Create a temporary CSV file with sample data."""
    import pandas as pd
    file_path = tmp_path / "test_data.csv"
    df = pd.DataFrame(sample_data)
    df.to_csv(file_path, index=False)
    return str(file_path)
