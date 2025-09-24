"""Unit tests for benchmark models."""

import pytest
from datetime import datetime
import json
from src.models.benchmark import (
    EngineResult, BenchmarkConfig, BenchmarkRun, 
    BenchmarkSummary, PerformanceReport
)


class TestEngineResult:
    """Test EngineResult model."""
    
    def test_successful_result_creation(self):
        """Test creating a successful engine result."""
        result = EngineResult(
            engine_name="TestEngine",
            execution_time_ms=1500.5,
            memory_usage_mb=256.7,
            memory_baseline_mb=128.3,
            success=True,
            row_count=10000
        )
        
        assert result.engine_name == "TestEngine"
        assert result.execution_time_ms == 1500.5
        assert result.memory_usage_mb == 256.7
        assert result.memory_baseline_mb == 128.3
        assert result.success is True
        assert result.error_message is None
        assert result.row_count == 10000
    
    def test_failed_result_creation(self):
        """Test creating a failed engine result."""
        result = EngineResult(
            engine_name="FailedEngine",
            execution_time_ms=0.0,
            memory_usage_mb=0.0,
            memory_baseline_mb=64.0,
            success=False,
            error_message="Connection timeout"
        )
        
        assert result.engine_name == "FailedEngine"
        assert result.success is False
        assert result.error_message == "Connection timeout"
        assert result.row_count is None
    
    def test_to_dict_serialization(self):
        """Test EngineResult serialization to dictionary."""
        result = EngineResult(
            engine_name="TestEngine",
            execution_time_ms=1000.0,
            memory_usage_mb=128.0,
            memory_baseline_mb=64.0,
            success=True,
            row_count=5000
        )
        
        result_dict = result.to_dict()
        
        expected_keys = {
            'engine_name', 'execution_time_ms', 'memory_usage_mb',
            'memory_baseline_mb', 'success', 'error_message', 'row_count'
        }
        assert set(result_dict.keys()) == expected_keys
        assert result_dict['engine_name'] == "TestEngine"
        assert result_dict['success'] is True


class TestBenchmarkConfig:
    """Test BenchmarkConfig model."""
    
    def test_default_config_creation(self):
        """Test creating config with minimal parameters."""
        config = BenchmarkConfig(
            hive_metastore_uri="thrift://localhost:9083",
            table_name="test.table",
            query="SELECT COUNT(*) FROM test.table"
        )
        
        assert config.hive_metastore_uri == "thrift://localhost:9083"
        assert config.table_name == "test.table"
        assert config.query == "SELECT COUNT(*) FROM test.table"
        assert config.iterations == 10  # default
        assert config.use_multiprocessing is True  # default
        assert config.timeout_seconds == 600  # default
        assert config.engines == ["Polar", "Arrow", "Daft", "DuckDB", "StarRocks"]  # default
    
    def test_custom_config_creation(self):
        """Test creating config with custom parameters."""
        config = BenchmarkConfig(
            hive_metastore_uri="thrift://remote:9083",
            table_name="custom.table",
            query="SELECT * FROM custom.table",
            iterations=5,
            use_multiprocessing=False,
            timeout_seconds=300,
            engines=["DuckDB", "Arrow"]
        )
        
        assert config.iterations == 5
        assert config.use_multiprocessing is False
        assert config.timeout_seconds == 300
        assert config.engines == ["DuckDB", "Arrow"]


class TestPerformanceReport:
    """Test PerformanceReport model."""
    
    def test_report_creation_and_serialization(self):
        """Test creating and serializing a complete performance report."""
        # Create test data
        benchmark_run = BenchmarkRun(
            table_name="test.table",
            query="SELECT COUNT(*) FROM test.table",
            hive_metastore_uri="thrift://localhost:9083",
            timestamp="2025-09-24 10:00:00",
            iterations=10,
            multiprocessing=True
        )
        
        result1 = EngineResult("Engine1", 1000.0, 128.0, 64.0, True, row_count=1000)
        result2 = EngineResult("Engine2", 1500.0, 256.0, 128.0, True, row_count=1000)
        
        summary = BenchmarkSummary(
            fastest_engine="Engine1",
            most_memory_efficient="Engine1",
            success_count=2,
            total_engines=2
        )
        
        report = PerformanceReport(
            benchmark_run=benchmark_run,
            aggregated_results=[result1, result2],
            all_iterations=[result1, result2],
            summary=summary
        )
        
        # Test serialization
        report_dict = report.to_dict()
        assert "benchmark_run" in report_dict
        assert "aggregated_results" in report_dict
        assert "all_iterations" in report_dict
        assert "summary" in report_dict
        
        # Test JSON serialization
        json_str = report.to_json()
        parsed = json.loads(json_str)
        assert parsed["summary"]["fastest_engine"] == "Engine1"
    
    def test_report_file_saving(self, tmp_path):
        """Test saving report to file."""
        benchmark_run = BenchmarkRun(
            table_name="test.table",
            query="SELECT COUNT(*) FROM test.table", 
            hive_metastore_uri="thrift://localhost:9083",
            timestamp="2025-09-24 10:00:00",
            iterations=10,
            multiprocessing=True
        )
        
        result = EngineResult("TestEngine", 1000.0, 128.0, 64.0, True, row_count=1000)
        summary = BenchmarkSummary("TestEngine", "TestEngine", 1, 1)
        
        report = PerformanceReport(
            benchmark_run=benchmark_run,
            aggregated_results=[result],
            all_iterations=[result],
            summary=summary
        )
        
        # Save to temporary file
        output_file = tmp_path / "test_results.json"
        report.save_to_file(str(output_file))
        
        # Verify file exists and contains valid JSON
        assert output_file.exists()
        
        with open(output_file, 'r') as f:
            loaded_data = json.load(f)
        
        assert loaded_data["summary"]["fastest_engine"] == "TestEngine"
        assert loaded_data["benchmark_run"]["table_name"] == "test.table"
