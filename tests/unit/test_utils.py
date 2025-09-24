"""Unit tests for utilities modules."""

import pytest
import os
import tempfile
import yaml
import time
from unittest.mock import Mock, patch, MagicMock
from dataclasses import asdict

from src.utils.config import ConfigLoader
from src.utils.metrics import MemoryMonitor, ExecutionTimer, benchmark_execution
from src.models.benchmark import BenchmarkConfig


class TestConfigLoader:
    """Test ConfigLoader utility."""
    
    def test_load_from_environment(self):
        """Test loading configuration from environment variables."""
        env_vars = {
            "HIVE_METASTORE_URI": "thrift://env:9083",
            "TABLE_NAME": "env.table", 
            "QUERY": "SELECT COUNT(*) FROM env.table",
            "ITERATIONS": "5",
            "TIMEOUT_SECONDS": "300",
            "USE_MULTIPROCESSING": "false"
        }
        
        with patch.dict(os.environ, env_vars):
            config = ConfigLoader.from_env()
            
            assert config.hive_metastore_uri == "thrift://env:9083"
            assert config.table_name == "env.table"
            assert config.query == "SELECT COUNT(*) FROM env.table"
            assert config.iterations == 5
            assert config.timeout_seconds == 300
            assert config.use_multiprocessing is False
            
            assert config.hive_metastore_uri == "thrift://env:9083"
            assert config.table_name == "env.table"
            assert config.query == "SELECT COUNT(*) FROM env.table"
            assert config.iterations == 5
            assert config.timeout_seconds == 300
            assert config.use_multiprocessing is False
            assert config.engines == ["DuckDB", "Arrow"]
    
    def test_load_from_environment_defaults(self):
        """Test loading with missing environment variables uses defaults."""
        required_env = {
            "HIVE_METASTORE_URI": "thrift://test:9083",
            "TABLE_NAME": "test.table",
            "QUERY": "SELECT 1"
        }
        
        with patch.dict(os.environ, required_env, clear=True):
            config = ConfigLoader.load_from_environment()
            
            # Required fields should be set
            assert config.hive_metastore_uri == "thrift://test:9083"
            assert config.table_name == "test.table"
            assert config.query == "SELECT 1"
            
            # Optional fields should use defaults
            assert config.iterations == 10
            assert config.timeout_seconds == 600
            assert config.use_multiprocessing is True
            assert config.engines == ["Polar", "Arrow", "Daft", "DuckDB", "StarRocks"]
    
    def test_load_from_file_yaml(self):
        """Test loading configuration from YAML file."""
        config_data = {
            "hive_metastore_uri": "thrift://yaml:9083",
            "table_name": "yaml.table",
            "query": "SELECT * FROM yaml.table",
            "iterations": 3,
            "timeout_seconds": 120,
            "use_multiprocessing": False,
            "engines": ["Polar", "DuckDB"]
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(config_data, f)
            temp_file = f.name
        
        try:
            config = ConfigLoader.load_from_file(temp_file)
            
            assert config.hive_metastore_uri == "thrift://yaml:9083"
            assert config.table_name == "yaml.table"
            assert config.query == "SELECT * FROM yaml.table"
            assert config.iterations == 3
            assert config.timeout_seconds == 120
            assert config.use_multiprocessing is False
            assert config.engines == ["Polar", "DuckDB"]
        finally:
            os.unlink(temp_file)
    
    def test_load_from_file_json(self):
        """Test loading configuration from JSON file."""
        import json
        
        config_data = {
            "hive_metastore_uri": "thrift://json:9083",
            "table_name": "json.table",
            "query": "SELECT COUNT(*) FROM json.table",
            "iterations": 7
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(config_data, f)
            temp_file = f.name
        
        try:
            config = ConfigLoader.load_from_file(temp_file)
            
            assert config.hive_metastore_uri == "thrift://json:9083"
            assert config.table_name == "json.table"
            assert config.query == "SELECT COUNT(*) FROM json.table"
            assert config.iterations == 7
            # Should use defaults for missing fields
            assert config.use_multiprocessing is True
            assert config.timeout_seconds == 600
        finally:
            os.unlink(temp_file)
    
    def test_load_from_file_missing(self):
        """Test loading from non-existent file raises error."""
        with pytest.raises(FileNotFoundError):
            ConfigLoader.load_from_file("/nonexistent/config.yaml")
    
    def test_load_from_file_invalid_format(self):
        """Test loading from unsupported file format raises error."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write("invalid config content")
            temp_file = f.name
        
        try:
            with pytest.raises(ValueError) as excinfo:
                ConfigLoader.load_from_file(temp_file)
            assert "Unsupported config file format" in str(excinfo.value)
        finally:
            os.unlink(temp_file)
    
    def test_from_dict(self):
        """Test creating config from dictionary."""
        config_dict = {
            "hive_metastore_uri": "thrift://dict:9083",
            "table_name": "dict.table",
            "query": "SELECT 1 FROM dict.table",
            "iterations": 2
        }
        
        config = ConfigLoader.from_dict(config_dict)
        
        assert config.hive_metastore_uri == "thrift://dict:9083"
        assert config.table_name == "dict.table"
        assert config.query == "SELECT 1 FROM dict.table"
        assert config.iterations == 2
        assert config.use_multiprocessing is True  # default


class TestMemoryMonitor:
    """Test MemoryMonitor utility."""
    
    @patch('psutil.Process')
    def test_memory_monitor_basic(self, mock_process_class):
        """Test basic memory monitoring functionality."""
        mock_process = Mock()
        mock_memory_info = Mock()
        mock_memory_info.rss = 1024 * 1024 * 128  # 128 MB
        mock_process.memory_info.return_value = mock_memory_info
        mock_process_class.return_value = mock_process
        
        monitor = MemoryMonitor()
        memory_mb = monitor.get_memory_usage()
        
        assert memory_mb == 128.0
        mock_process.memory_info.assert_called_once()
    
    @patch('psutil.Process')
    def test_memory_monitor_context_manager(self, mock_process_class):
        """Test memory monitor as context manager."""
        mock_process = Mock()
        # Simulate memory usage: 64MB baseline, 128MB peak
        memory_values = [
            Mock(rss=1024 * 1024 * 64),   # baseline
            Mock(rss=1024 * 1024 * 96),   # during execution
            Mock(rss=1024 * 1024 * 128),  # peak
            Mock(rss=1024 * 1024 * 80)    # after execution
        ]
        mock_process.memory_info.side_effect = memory_values
        mock_process_class.return_value = mock_process
        
        with MemoryMonitor() as monitor:
            # Simulate some work that uses memory
            pass
        
        assert monitor.baseline_memory_mb == 64.0
        assert monitor.peak_memory_mb == 128.0
    
    @patch('psutil.Process')
    def test_memory_monitor_error_handling(self, mock_process_class):
        """Test memory monitor handles psutil errors gracefully."""
        mock_process = Mock()
        mock_process.memory_info.side_effect = Exception("Process not found")
        mock_process_class.return_value = mock_process
        
        monitor = MemoryMonitor()
        memory_mb = monitor.get_memory_usage()
        
        # Should return 0.0 when error occurs
        assert memory_mb == 0.0


class TestExecutionTimer:
    """Test ExecutionTimer utility."""
    
    def test_execution_timer_basic(self):
        """Test basic execution timing."""
        timer = ExecutionTimer()
        
        timer.start()
        time.sleep(0.01)  # Sleep for ~10ms
        elapsed = timer.stop()
        
        # Should be approximately 10ms, allow some tolerance
        assert 8 <= elapsed <= 20
    
    def test_execution_timer_context_manager(self):
        """Test execution timer as context manager."""
        with ExecutionTimer() as timer:
            time.sleep(0.01)  # Sleep for ~10ms
        
        # Should be approximately 10ms, allow some tolerance
        assert 8 <= timer.elapsed_ms <= 20
    
    def test_execution_timer_not_started(self):
        """Test timer raises error when stopped without starting."""
        timer = ExecutionTimer()
        
        with pytest.raises(RuntimeError) as excinfo:
            timer.stop()
        
        assert "Timer was not started" in str(excinfo.value)
    
    def test_execution_timer_reset(self):
        """Test timer can be reset and reused."""
        timer = ExecutionTimer()
        
        # First measurement
        timer.start()
        time.sleep(0.01)
        first_elapsed = timer.stop()
        
        # Reset and second measurement
        timer.reset()
        timer.start()
        time.sleep(0.005)  # Shorter sleep
        second_elapsed = timer.stop()
        
        # Second measurement should be shorter
        assert second_elapsed < first_elapsed


class TestBenchmarkExecution:
    """Test benchmark_execution decorator function."""
    
    def test_benchmark_execution_success(self):
        """Test successful benchmark execution."""
        def sample_function():
            return "success", 100
        
        with patch('src.utils.metrics.MemoryMonitor') as mock_monitor_class:
            mock_monitor = Mock()
            mock_monitor.baseline_memory_mb = 64.0
            mock_monitor.peak_memory_mb = 128.0
            mock_monitor.__enter__ = Mock(return_value=mock_monitor)
            mock_monitor.__exit__ = Mock(return_value=None)
            mock_monitor_class.return_value = mock_monitor
            
            with patch('src.utils.metrics.ExecutionTimer') as mock_timer_class:
                mock_timer = Mock()
                mock_timer.elapsed_ms = 1500.0
                mock_timer.__enter__ = Mock(return_value=mock_timer)
                mock_timer.__exit__ = Mock(return_value=None)
                mock_timer_class.return_value = mock_timer
                
                result, execution_time, baseline_memory, peak_memory = benchmark_execution(sample_function)
                
                assert result == ("success", 100)
                assert execution_time == 1500.0
                assert baseline_memory == 64.0
                assert peak_memory == 128.0
    
    def test_benchmark_execution_with_exception(self):
        """Test benchmark execution handles exceptions properly."""
        def failing_function():
            raise ValueError("Test error")
        
        with patch('src.utils.metrics.MemoryMonitor') as mock_monitor_class:
            mock_monitor = Mock()
            mock_monitor.baseline_memory_mb = 64.0
            mock_monitor.peak_memory_mb = 64.0  # No memory increase due to failure
            mock_monitor.__enter__ = Mock(return_value=mock_monitor)
            mock_monitor.__exit__ = Mock(return_value=None)
            mock_monitor_class.return_value = mock_monitor
            
            with patch('src.utils.metrics.ExecutionTimer') as mock_timer_class:
                mock_timer = Mock()
                mock_timer.elapsed_ms = 50.0  # Quick failure
                mock_timer.__enter__ = Mock(return_value=mock_timer)
                mock_timer.__exit__ = Mock(return_value=None)
                mock_timer_class.return_value = mock_timer
                
                with pytest.raises(ValueError):
                    benchmark_execution(failing_function)
    
    def test_benchmark_execution_preserves_arguments(self):
        """Test benchmark execution preserves function arguments."""
        def function_with_args(a, b, keyword=None):
            return f"result: {a}, {b}, {keyword}"
        
        with patch('src.utils.metrics.MemoryMonitor') as mock_monitor_class:
            mock_monitor = Mock()
            mock_monitor.baseline_memory_mb = 32.0
            mock_monitor.peak_memory_mb = 48.0
            mock_monitor.__enter__ = Mock(return_value=mock_monitor)
            mock_monitor.__exit__ = Mock(return_value=None)
            mock_monitor_class.return_value = mock_monitor
            
            with patch('src.utils.metrics.ExecutionTimer') as mock_timer_class:
                mock_timer = Mock()
                mock_timer.elapsed_ms = 750.0
                mock_timer.__enter__ = Mock(return_value=mock_timer)
                mock_timer.__exit__ = Mock(return_value=None)
                mock_timer_class.return_value = mock_timer
                
                result, _, _, _ = benchmark_execution(
                    function_with_args, "arg1", "arg2", keyword="test"
                )
                
                assert result == "result: arg1, arg2, test"
