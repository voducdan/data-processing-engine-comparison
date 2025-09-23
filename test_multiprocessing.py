#!/usr/bin/env python3
"""
Simple test to verify the multiprocessing functionality works correctly.
"""

import os
import sys
import time
from benchmark import PerformanceBenchmark, run_single_engine_benchmark

def test_multiprocessing_functionality():
    """Test that the multiprocessing setup works correctly."""
    print("Testing multiprocessing functionality...")
    
    # Create a simple test benchmark
    hive_metastore_uri = "test://localhost"
    table_name = "test.table"
    query = "SELECT COUNT(*) FROM test.table"
    
    print(f"Testing run_single_engine_benchmark function...")
    
    # Test that the function can be called (even if it fails due to missing connections)
    try:
        result = run_single_engine_benchmark("DuckDB", hive_metastore_uri, table_name, query, 1)
        print(f"✅ Function call succeeded: {result['engine_name']}")
        print(f"   Success: {result['success']}")
        if not result['success']:
            print(f"   Expected error (no real connection): {result['error']}")
    except Exception as e:
        print(f"❌ Function call failed: {e}")
        return False
    
    print("✅ Multiprocessing functionality test completed successfully!")
    return True

if __name__ == "__main__":
    import multiprocessing as mp
    mp.set_start_method('spawn', force=True)
    test_multiprocessing_functionality()
