"""Integration tests for the benchmark system."""

import pytest
import tempfile
import os
import json
from unittest.mock import patch, Mock

from src.benchmark_runner import BenchmarkOrchestrator
from src.models.benchmark import BenchmarkConfig
from src.utils.config import ConfigLoader
from src.engines.factory import EngineFactory


class TestFullBenchmarkIntegration:
    """Integration tests for complete benchmark workflow."""
    
    @pytest.fixture
    def sample_config(self):
        """Create a sample benchmark configuration."""
        return BenchmarkConfig(
            hive_metastore_uri="thrift://localhost:9083",
            table_name="test.sample_table",
            query="SELECT COUNT(*) FROM test.sample_table",
            iterations=2,
            engines=["DuckDB"],  # Use only DuckDB for faster tests
            use_multiprocessing=False,  # Disable multiprocessing for easier testing
            timeout_seconds=60
        )
    
    @patch('src.engines.duckdb_engine.duckdb')
    def test_end_to_end_benchmark_execution(self, mock_duckdb, sample_config):
        """Test complete end-to-end benchmark execution."""
        # Mock DuckDB connection and execution
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = [{"count": 1000}]
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.execute.return_value = None
        mock_duckdb.connect.return_value = mock_conn
        
        # Run the benchmark
        orchestrator = BenchmarkOrchestrator(sample_config)
        report = orchestrator.run()
        
        # Verify report structure
        assert report is not None
        assert report.benchmark_run.table_name == "test.sample_table"
        assert report.benchmark_run.iterations == 2
        assert len(report.aggregated_results) == 1
        assert len(report.all_iterations) == 2
        
        # Verify DuckDB engine result
        duckdb_result = report.aggregated_results[0]
        assert duckdb_result.engine_name == "DuckDB"
        assert duckdb_result.success is True
        assert duckdb_result.execution_time_ms > 0
        assert duckdb_result.row_count == 1000
        
        # Verify summary
        assert report.summary.fastest_engine == "DuckDB"
        assert report.summary.most_memory_efficient == "DuckDB"
        assert report.summary.success_count == 1
        assert report.summary.total_engines == 1
    
    def test_config_loading_integration(self):
        """Test configuration loading from different sources."""
        # Test YAML config loading
        config_data = {
            "hive_metastore_uri": "thrift://yaml-test:9083",
            "table_name": "yaml.test_table",
            "query": "SELECT * FROM yaml.test_table",
            "iterations": 3,
            "engines": ["Arrow", "DuckDB"]
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            import yaml
            yaml.dump(config_data, f)
            temp_file = f.name
        
        try:
            config = ConfigLoader.load_from_file(temp_file)
            
            # Create orchestrator with loaded config
            orchestrator = BenchmarkOrchestrator(config)
            
            assert orchestrator.config.hive_metastore_uri == "thrift://yaml-test:9083"
            assert orchestrator.config.table_name == "yaml.test_table"
            assert orchestrator.config.iterations == 3
            assert orchestrator.config.engines == ["Arrow", "DuckDB"]
        finally:
            os.unlink(temp_file)
    
    def test_engine_factory_integration(self):
        """Test engine factory creates all available engines."""
        available_engines = EngineFactory.get_available_engines()
        
        # Test creating each available engine
        for engine_name in available_engines:
            engine = EngineFactory.create_engine(
                engine_name,
                "thrift://test:9083",
                "test.table",
                "SELECT 1"
            )
            
            assert engine.engine_name == engine_name
            assert engine.hive_metastore_uri == "thrift://test:9083"
            assert engine.table_name == "test.table"
            assert engine.query == "SELECT 1"
    
    @patch('src.engines.duckdb_engine.duckdb')
    @patch('src.engines.arrow_engine.pyarrow')
    def test_multi_engine_benchmark(self, mock_arrow, mock_duckdb, sample_config):
        """Test benchmark with multiple engines."""
        # Update config for multiple engines
        sample_config.engines = ["DuckDB", "Arrow"]
        
        # Mock DuckDB
        mock_duckdb_conn = Mock()
        mock_duckdb_cursor = Mock()
        mock_duckdb_cursor.fetchall.return_value = [{"count": 1000}]
        mock_duckdb_conn.cursor.return_value = mock_duckdb_cursor
        mock_duckdb.connect.return_value = mock_duckdb_conn
        
        # Mock Arrow
        mock_table = Mock()
        mock_table.to_pydict.return_value = {"count": [1000]}
        mock_table.num_rows = 1000
        mock_arrow.Table.from_pydict.return_value = mock_table
        
        # Run benchmark
        orchestrator = BenchmarkOrchestrator(sample_config)
        report = orchestrator.run()
        
        # Verify results for both engines
        assert len(report.aggregated_results) == 2
        assert len(report.all_iterations) == 4  # 2 engines * 2 iterations
        
        engine_names = {result.engine_name for result in report.aggregated_results}
        assert engine_names == {"DuckDB", "Arrow"}
        
        # All engines should succeed
        for result in report.aggregated_results:
            assert result.success is True
            assert result.row_count == 1000
    
    def test_report_serialization_integration(self, sample_config):
        """Test complete report serialization and file saving."""
        with patch('src.engines.duckdb_engine.duckdb') as mock_duckdb:
            # Mock DuckDB
            mock_conn = Mock()
            mock_cursor = Mock()
            mock_cursor.fetchall.return_value = [{"count": 500}]
            mock_conn.cursor.return_value = mock_cursor
            mock_duckdb.connect.return_value = mock_conn
            
            # Run benchmark
            orchestrator = BenchmarkOrchestrator(sample_config)
            report = orchestrator.run()
            
            # Test JSON serialization
            json_str = report.to_json()
            parsed_report = json.loads(json_str)
            
            assert "benchmark_run" in parsed_report
            assert "aggregated_results" in parsed_report
            assert "all_iterations" in parsed_report
            assert "summary" in parsed_report
            
            # Test file saving
            with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
                temp_file = f.name
            
            try:
                report.save_to_file(temp_file)
                
                # Verify file was created and contains valid JSON
                assert os.path.exists(temp_file)
                
                with open(temp_file, 'r') as f:
                    loaded_data = json.load(f)
                
                assert loaded_data["benchmark_run"]["table_name"] == "test.sample_table"
                assert loaded_data["summary"]["success_count"] == 1
            finally:
                os.unlink(temp_file)
    
    def test_error_handling_integration(self):
        """Test error handling across the system."""
        # Create config with invalid parameters
        invalid_config = BenchmarkConfig(
            hive_metastore_uri="thrift://nonexistent:9083",
            table_name="invalid.table",
            query="INVALID SQL",
            iterations=1,
            engines=["DuckDB"],
            use_multiprocessing=False
        )
        
        # Run benchmark (should handle connection/query errors gracefully)
        orchestrator = BenchmarkOrchestrator(invalid_config)
        report = orchestrator.run()
        
        # Should complete but with failed results
        assert report is not None
        assert len(report.aggregated_results) == 1
        
        # DuckDB result should show failure
        duckdb_result = report.aggregated_results[0]
        assert duckdb_result.engine_name == "DuckDB"
        assert duckdb_result.success is False
        assert duckdb_result.error_message is not None
        
        # Summary should reflect failures
        assert report.summary.success_count == 0
        assert report.summary.total_engines == 1
        assert report.summary.fastest_engine is None
        assert report.summary.most_memory_efficient is None


class TestCLIIntegration:
    """Integration tests for CLI interfaces."""
    
    def test_benchmark_cli_integration(self):
        """Test new CLI interface integration."""
        # This would test the benchmark_cli.py script
        # For now, just verify it can be imported
        try:
            # Import should work without errors
            import benchmark_cli
            assert hasattr(benchmark_cli, 'main')
        except ImportError:
            pytest.fail("benchmark_cli.py should be importable")
    
    def test_legacy_cli_integration(self):
        """Test legacy CLI interface still works."""
        # This would test the original benchmark.py script
        # For now, just verify it exists and can be imported
        try:
            # Import should work without errors
            import benchmark
            # Legacy script should still have main functionality
            assert hasattr(benchmark, 'run_benchmark') or hasattr(benchmark, 'main')
        except ImportError:
            pytest.fail("benchmark.py should be importable for backward compatibility")


class TestPerformanceRegression:
    """Tests to ensure no performance regression after refactoring."""
    
    @pytest.mark.performance
    def test_benchmark_execution_time(self, sample_config):
        """Test that benchmark execution completes within reasonable time."""
        import time
        
        with patch('src.engines.duckdb_engine.duckdb') as mock_duckdb:
            # Mock fast DuckDB execution
            mock_conn = Mock()
            mock_cursor = Mock()
            mock_cursor.fetchall.return_value = [{"count": 1000}]
            mock_conn.cursor.return_value = mock_cursor
            mock_duckdb.connect.return_value = mock_conn
            
            start_time = time.time()
            
            orchestrator = BenchmarkOrchestrator(sample_config)
            report = orchestrator.run()
            
            execution_time = time.time() - start_time
            
            # Benchmark orchestration should complete quickly (under 5 seconds with mocking)
            assert execution_time < 5.0
            assert report is not None
    
    @pytest.mark.performance
    def test_memory_usage_monitoring(self, sample_config):
        """Test that memory monitoring works correctly."""
        with patch('src.engines.duckdb_engine.duckdb') as mock_duckdb:
            mock_conn = Mock()
            mock_cursor = Mock()
            mock_cursor.fetchall.return_value = [{"count": 1000}]
            mock_conn.cursor.return_value = mock_cursor
            mock_duckdb.connect.return_value = mock_conn
            
            orchestrator = BenchmarkOrchestrator(sample_config)
            report = orchestrator.run()
            
            # Verify memory metrics are captured
            for result in report.aggregated_results:
                if result.success:
                    assert result.memory_baseline_mb >= 0
                    assert result.memory_usage_mb >= 0
                    # Peak memory should be >= baseline
                    assert result.memory_usage_mb >= result.memory_baseline_mb
