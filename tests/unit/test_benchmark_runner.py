"""Unit tests for benchmark runner orchestrator."""

import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
import multiprocessing as mp

from src.benchmark_runner import BenchmarkOrchestrator
from src.models.benchmark import BenchmarkConfig, EngineResult, PerformanceReport
from src.engines.factory import EngineFactory


class TestBenchmarkOrchestrator:
    """Test BenchmarkOrchestrator class."""
    
    def test_orchestrator_initialization(self):
        """Test orchestrator initialization with config."""
        config = BenchmarkConfig(
            hive_metastore_uri="thrift://test:9083",
            table_name="test.table",
            query="SELECT COUNT(*) FROM test.table",
            iterations=5,
            engines=["DuckDB", "Arrow"]
        )
        
        orchestrator = BenchmarkOrchestrator(config)
        
        assert orchestrator.config == config
        assert orchestrator.config.iterations == 5
        assert orchestrator.config.engines == ["DuckDB", "Arrow"]
    
    @patch('src.benchmark_runner.EngineFactory.create_engine')
    def test_run_single_benchmark_success(self, mock_create_engine):
        """Test running a single benchmark successfully."""
        # Setup mock engine
        mock_engine = Mock()
        mock_result = EngineResult(
            engine_name="TestEngine",
            execution_time_ms=1000.0,
            memory_usage_mb=128.0,
            memory_baseline_mb=64.0,
            success=True,
            row_count=1000
        )
        mock_engine.benchmark.return_value = mock_result
        mock_create_engine.return_value = mock_engine
        
        config = BenchmarkConfig(
            hive_metastore_uri="thrift://test:9083",
            table_name="test.table",
            query="SELECT COUNT(*) FROM test.table",
            iterations=1,
            engines=["TestEngine"]
        )
        
        orchestrator = BenchmarkOrchestrator(config)
        result = orchestrator._run_single_benchmark("TestEngine")
        
        assert result == mock_result
        mock_create_engine.assert_called_once_with(
            "TestEngine",
            "thrift://test:9083",
            "test.table", 
            "SELECT COUNT(*) FROM test.table"
        )
        mock_engine.benchmark.assert_called_once()
    
    @patch('src.benchmark_runner.EngineFactory.create_engine')
    def test_run_single_benchmark_failure(self, mock_create_engine):
        """Test handling of single benchmark failure."""
        # Setup mock engine that fails
        mock_create_engine.side_effect = Exception("Engine creation failed")
        
        config = BenchmarkConfig(
            hive_metastore_uri="thrift://test:9083",
            table_name="test.table",
            query="SELECT COUNT(*) FROM test.table",
            iterations=1,
            engines=["FailingEngine"]
        )
        
        orchestrator = BenchmarkOrchestrator(config)
        result = orchestrator._run_single_benchmark("FailingEngine")
        
        assert isinstance(result, EngineResult)
        assert result.engine_name == "FailingEngine"
        assert result.success is False
        assert "Engine creation failed" in result.error_message
        assert result.execution_time_ms == 0.0
        assert result.memory_usage_mb == 0.0
    
    @patch('src.benchmark_runner.BenchmarkOrchestrator._run_single_benchmark')
    def test_run_benchmarks_sequential(self, mock_run_single):
        """Test running benchmarks sequentially."""
        # Setup mock results
        mock_results = [
            EngineResult("Engine1", 1000.0, 128.0, 64.0, True, row_count=1000),
            EngineResult("Engine2", 1500.0, 256.0, 128.0, True, row_count=1000)
        ]
        mock_run_single.side_effect = mock_results
        
        config = BenchmarkConfig(
            hive_metastore_uri="thrift://test:9083",
            table_name="test.table",
            query="SELECT COUNT(*) FROM test.table",
            iterations=1,
            engines=["Engine1", "Engine2"],
            use_multiprocessing=False
        )
        
        orchestrator = BenchmarkOrchestrator(config)
        results = orchestrator._run_benchmarks()
        
        assert len(results) == 2
        assert results[0] == mock_results[0]
        assert results[1] == mock_results[1]
        assert mock_run_single.call_count == 2
    
    @patch('multiprocessing.Pool')
    @patch('src.benchmark_runner.BenchmarkOrchestrator._run_single_benchmark')
    def test_run_benchmarks_parallel(self, mock_run_single, mock_pool_class):
        """Test running benchmarks in parallel using multiprocessing."""
        # Setup mock results
        mock_results = [
            EngineResult("Engine1", 1000.0, 128.0, 64.0, True, row_count=1000),
            EngineResult("Engine2", 1500.0, 256.0, 128.0, True, row_count=1000)
        ]
        
        # Setup mock multiprocessing pool
        mock_pool = Mock()
        mock_pool.__enter__ = Mock(return_value=mock_pool)
        mock_pool.__exit__ = Mock(return_value=None)
        mock_pool.map.return_value = mock_results
        mock_pool_class.return_value = mock_pool
        
        config = BenchmarkConfig(
            hive_metastore_uri="thrift://test:9083",
            table_name="test.table",
            query="SELECT COUNT(*) FROM test.table",
            iterations=1,
            engines=["Engine1", "Engine2"],
            use_multiprocessing=True
        )
        
        orchestrator = BenchmarkOrchestrator(config)
        results = orchestrator._run_benchmarks()
        
        assert len(results) == 2
        assert results[0] == mock_results[0]
        assert results[1] == mock_results[1]
        mock_pool_class.assert_called_once()
        mock_pool.map.assert_called_once()
    
    def test_aggregate_results(self):
        """Test aggregating multiple iteration results."""
        # Create sample results for multiple iterations
        iteration_results = [
            # Iteration 1
            [
                EngineResult("Engine1", 1000.0, 128.0, 64.0, True, row_count=1000),
                EngineResult("Engine2", 1500.0, 256.0, 128.0, True, row_count=1000)
            ],
            # Iteration 2  
            [
                EngineResult("Engine1", 1200.0, 132.0, 64.0, True, row_count=1000),
                EngineResult("Engine2", 1400.0, 252.0, 128.0, True, row_count=1000)
            ]
        ]
        
        config = BenchmarkConfig(
            hive_metastore_uri="thrift://test:9083",
            table_name="test.table",
            query="SELECT COUNT(*) FROM test.table"
        )
        
        orchestrator = BenchmarkOrchestrator(config)
        aggregated = orchestrator._aggregate_results(iteration_results)
        
        # Should have one result per engine
        assert len(aggregated) == 2
        
        # Find results by engine name
        engine1_result = next(r for r in aggregated if r.engine_name == "Engine1")
        engine2_result = next(r for r in aggregated if r.engine_name == "Engine2")
        
        # Check Engine1 averages: (1000 + 1200) / 2 = 1100, (128 + 132) / 2 = 130
        assert engine1_result.execution_time_ms == 1100.0
        assert engine1_result.memory_usage_mb == 130.0
        assert engine1_result.memory_baseline_mb == 64.0  # Should be same
        assert engine1_result.success is True
        assert engine1_result.row_count == 1000
        
        # Check Engine2 averages: (1500 + 1400) / 2 = 1450, (256 + 252) / 2 = 254
        assert engine2_result.execution_time_ms == 1450.0
        assert engine2_result.memory_usage_mb == 254.0
        assert engine2_result.memory_baseline_mb == 128.0
        assert engine2_result.success is True
        assert engine2_result.row_count == 1000
    
    def test_aggregate_results_with_failures(self):
        """Test aggregating results when some iterations fail."""
        iteration_results = [
            [
                EngineResult("Engine1", 1000.0, 128.0, 64.0, True, row_count=1000),
                EngineResult("Engine2", 0.0, 0.0, 128.0, False, error_message="Failed")
            ],
            [
                EngineResult("Engine1", 1200.0, 132.0, 64.0, True, row_count=1000),
                EngineResult("Engine2", 1400.0, 252.0, 128.0, True, row_count=1000)
            ]
        ]
        
        config = BenchmarkConfig(
            hive_metastore_uri="thrift://test:9083", 
            table_name="test.table",
            query="SELECT COUNT(*) FROM test.table"
        )
        
        orchestrator = BenchmarkOrchestrator(config)
        aggregated = orchestrator._aggregate_results(iteration_results)
        
        # Should have one result per engine
        assert len(aggregated) == 2
        
        engine1_result = next(r for r in aggregated if r.engine_name == "Engine1")
        engine2_result = next(r for r in aggregated if r.engine_name == "Engine2")
        
        # Engine1 should be successful (average of 2 successes)
        assert engine1_result.success is True
        assert engine1_result.execution_time_ms == 1100.0
        
        # Engine2 should be successful (1 success out of 2, so success rate > 50%)
        assert engine2_result.success is True
        assert engine2_result.execution_time_ms == 1400.0  # Average of only successful runs
        assert engine2_result.memory_usage_mb == 252.0
    
    def test_create_summary(self):
        """Test creating benchmark summary."""
        aggregated_results = [
            EngineResult("FastEngine", 800.0, 100.0, 50.0, True, row_count=1000),
            EngineResult("SlowEngine", 1500.0, 200.0, 100.0, True, row_count=1000),
            EngineResult("FailedEngine", 0.0, 0.0, 0.0, False, error_message="Error")
        ]
        
        config = BenchmarkConfig(
            hive_metastore_uri="thrift://test:9083",
            table_name="test.table", 
            query="SELECT COUNT(*) FROM test.table"
        )
        
        orchestrator = BenchmarkOrchestrator(config)
        summary = orchestrator._create_summary(aggregated_results)
        
        assert summary.fastest_engine == "FastEngine"
        assert summary.most_memory_efficient == "FastEngine"
        assert summary.success_count == 2
        assert summary.total_engines == 3
    
    def test_create_summary_all_failed(self):
        """Test creating summary when all engines failed."""
        aggregated_results = [
            EngineResult("Engine1", 0.0, 0.0, 0.0, False, error_message="Error 1"),
            EngineResult("Engine2", 0.0, 0.0, 0.0, False, error_message="Error 2")
        ]
        
        config = BenchmarkConfig(
            hive_metastore_uri="thrift://test:9083",
            table_name="test.table",
            query="SELECT COUNT(*) FROM test.table"
        )
        
        orchestrator = BenchmarkOrchestrator(config)
        summary = orchestrator._create_summary(aggregated_results)
        
        assert summary.fastest_engine is None
        assert summary.most_memory_efficient is None
        assert summary.success_count == 0
        assert summary.total_engines == 2
    
    @patch('src.benchmark_runner.BenchmarkOrchestrator._run_benchmarks')
    @patch('src.benchmark_runner.BenchmarkOrchestrator._aggregate_results')
    @patch('src.benchmark_runner.BenchmarkOrchestrator._create_summary')
    @patch('datetime.datetime')
    def test_run_full_benchmark(self, mock_datetime, mock_create_summary, 
                               mock_aggregate_results, mock_run_benchmarks):
        """Test running complete benchmark orchestration."""
        # Setup mocks
        mock_datetime.now.return_value.strftime.return_value = "2025-09-24 10:00:00"
        
        mock_iteration_results = [
            [EngineResult("Engine1", 1000.0, 128.0, 64.0, True, row_count=1000)]
        ]
        mock_run_benchmarks.return_value = mock_iteration_results[0]
        
        mock_aggregated = [EngineResult("Engine1", 1000.0, 128.0, 64.0, True, row_count=1000)]
        mock_aggregate_results.return_value = mock_aggregated
        
        mock_summary = Mock()
        mock_create_summary.return_value = mock_summary
        
        config = BenchmarkConfig(
            hive_metastore_uri="thrift://test:9083",
            table_name="test.table",
            query="SELECT COUNT(*) FROM test.table",
            iterations=1,
            engines=["Engine1"]
        )
        
        orchestrator = BenchmarkOrchestrator(config)
        report = orchestrator.run()
        
        # Verify the full workflow was executed
        assert mock_run_benchmarks.call_count == 1  # Called once per iteration
        mock_aggregate_results.assert_called_once()
        mock_create_summary.assert_called_once_with(mock_aggregated)
        
        # Verify report structure
        assert isinstance(report, PerformanceReport)
        assert report.aggregated_results == mock_aggregated
        assert report.all_iterations == mock_iteration_results[0]
        assert report.summary == mock_summary
        assert report.benchmark_run.table_name == "test.table"
        assert report.benchmark_run.timestamp == "2025-09-24 10:00:00"
        
    @patch('src.benchmark_runner.BenchmarkOrchestrator._run_benchmarks')
    def test_run_multiple_iterations(self, mock_run_benchmarks):
        """Test running multiple benchmark iterations."""
        # Setup mock to return different results for each iteration
        iteration_results = [
            [EngineResult("Engine1", 1000.0, 128.0, 64.0, True, row_count=1000)],
            [EngineResult("Engine1", 1200.0, 132.0, 64.0, True, row_count=1000)],
            [EngineResult("Engine1", 900.0, 120.0, 64.0, True, row_count=1000)]
        ]
        mock_run_benchmarks.side_effect = iteration_results
        
        config = BenchmarkConfig(
            hive_metastore_uri="thrift://test:9083",
            table_name="test.table",
            query="SELECT COUNT(*) FROM test.table", 
            iterations=3,
            engines=["Engine1"]
        )
        
        orchestrator = BenchmarkOrchestrator(config)
        report = orchestrator.run()
        
        # Should run benchmarks 3 times
        assert mock_run_benchmarks.call_count == 3
        
        # All iteration results should be included
        expected_all_iterations = [result for iteration in iteration_results for result in iteration]
        assert len(report.all_iterations) == 3
        
        # Aggregated results should be averaged
        assert len(report.aggregated_results) == 1
        engine_result = report.aggregated_results[0]
        assert engine_result.engine_name == "Engine1"
        # Average: (1000 + 1200 + 900) / 3 = 1033.33
        assert abs(engine_result.execution_time_ms - 1033.33) < 0.01
