"""Contract tests to validate engine interface compliance."""

import pytest
from abc import ABC
import inspect
from unittest.mock import Mock, patch

from src.engines.base import BaseEngine
from src.engines.factory import EngineFactory
from src.models.benchmark import EngineResult


class TestEngineContractCompliance:
    """Test that all engines properly implement the BaseEngine interface."""
    
    def test_all_engines_inherit_from_base(self):
        """Test that all registered engines inherit from BaseEngine."""
        available_engines = EngineFactory.get_available_engines()
        
        for engine_name in available_engines:
            engine_class = EngineFactory._engines[engine_name]
            
            # Check inheritance
            assert issubclass(engine_class, BaseEngine), f"{engine_name} must inherit from BaseEngine"
            
            # Check it's not the abstract base class itself
            assert engine_class is not BaseEngine, f"{engine_name} cannot be the BaseEngine class"
    
    def test_all_engines_implement_required_methods(self):
        """Test that all engines implement required abstract methods."""
        available_engines = EngineFactory.get_available_engines()
        
        for engine_name in available_engines:
            engine_class = EngineFactory._engines[engine_name]
            
            # Check required methods exist and are not abstract
            required_methods = ['engine_name', '_execute_query']
            
            for method_name in required_methods:
                assert hasattr(engine_class, method_name), f"{engine_name} missing {method_name}"
                
                method = getattr(engine_class, method_name)
                if method_name == 'engine_name':
                    # engine_name should be a property
                    assert isinstance(method, property), f"{engine_name}.engine_name should be a property"
                else:
                    # _execute_query should be a method
                    assert callable(method), f"{engine_name}.{method_name} should be callable"
    
    def test_engine_name_property_returns_string(self):
        """Test that engine_name property returns the correct engine name."""
        available_engines = EngineFactory.get_available_engines()
        
        for engine_name in available_engines:
            # Create engine instance with mock dependencies
            with patch.multiple(
                'src.engines.duckdb_engine' if engine_name == 'DuckDB' else f'src.engines.{engine_name.lower()}_engine',
                **self._get_mock_dependencies(engine_name)
            ):
                try:
                    engine = EngineFactory.create_engine(
                        engine_name,
                        "thrift://test:9083",
                        "test.table",
                        "SELECT 1"
                    )
                    
                    # Test engine_name property
                    assert isinstance(engine.engine_name, str), f"{engine_name}.engine_name should return a string"
                    assert engine.engine_name == engine_name, f"{engine_name}.engine_name should return '{engine_name}'"
                    
                except ImportError:
                    # Skip if dependencies are not available
                    pytest.skip(f"Dependencies for {engine_name} not available")
    
    def test_execute_query_signature(self):
        """Test that _execute_query method has the correct signature."""
        available_engines = EngineFactory.get_available_engines()
        
        for engine_name in available_engines:
            engine_class = EngineFactory._engines[engine_name]
            
            # Check method signature
            execute_method = engine_class._execute_query
            signature = inspect.signature(execute_method)
            
            # Should take only self parameter
            params = list(signature.parameters.keys())
            assert params == ['self'], f"{engine_name}._execute_query should only take 'self' parameter"
            
            # Should return tuple[list, int] (results, row_count)
            # Note: We can't easily test return type annotation in all Python versions
            # But we can test that it's documented to return the right type
    
    def test_benchmark_method_returns_engine_result(self):
        """Test that benchmark method returns EngineResult."""
        available_engines = EngineFactory.get_available_engines()
        
        for engine_name in available_engines:
            with patch.multiple(
                'src.engines.duckdb_engine' if engine_name == 'DuckDB' else f'src.engines.{engine_name.lower()}_engine',
                **self._get_mock_dependencies(engine_name)
            ):
                try:
                    engine = EngineFactory.create_engine(
                        engine_name,
                        "thrift://test:9083",
                        "test.table",
                        "SELECT 1"
                    )
                    
                    # Mock the _execute_query method to return test data
                    with patch.object(engine, '_execute_query') as mock_execute:
                        mock_execute.return_value = ([{"result": "test"}], 1)
                        
                        result = engine.benchmark()
                        
                        # Should return EngineResult
                        assert isinstance(result, EngineResult), f"{engine_name}.benchmark() should return EngineResult"
                        assert result.engine_name == engine_name
                        
                except ImportError:
                    pytest.skip(f"Dependencies for {engine_name} not available")
    
    def test_all_engines_handle_initialization_parameters(self):
        """Test that all engines properly handle initialization parameters."""
        available_engines = EngineFactory.get_available_engines()
        
        test_params = {
            "hive_metastore_uri": "thrift://test:9083",
            "table_name": "test.table",
            "query": "SELECT COUNT(*) FROM test.table"
        }
        
        for engine_name in available_engines:
            with patch.multiple(
                'src.engines.duckdb_engine' if engine_name == 'DuckDB' else f'src.engines.{engine_name.lower()}_engine',
                **self._get_mock_dependencies(engine_name)
            ):
                try:
                    engine = EngineFactory.create_engine(engine_name, **test_params)
                    
                    # Check that parameters are stored correctly
                    assert engine.hive_metastore_uri == test_params["hive_metastore_uri"]
                    assert engine.table_name == test_params["table_name"]
                    assert engine.query == test_params["query"]
                    
                except ImportError:
                    pytest.skip(f"Dependencies for {engine_name} not available")
    
    def test_engine_error_handling_contract(self):
        """Test that engines handle errors according to the contract."""
        available_engines = EngineFactory.get_available_engines()
        
        for engine_name in available_engines:
            with patch.multiple(
                'src.engines.duckdb_engine' if engine_name == 'DuckDB' else f'src.engines.{engine_name.lower()}_engine',
                **self._get_mock_dependencies(engine_name)
            ):
                try:
                    engine = EngineFactory.create_engine(
                        engine_name,
                        "thrift://test:9083",
                        "test.table",
                        "SELECT 1"
                    )
                    
                    # Mock _execute_query to raise an exception
                    with patch.object(engine, '_execute_query') as mock_execute:
                        mock_execute.side_effect = Exception("Test error")
                        
                        result = engine.benchmark()
                        
                        # Should return EngineResult with failure information
                        assert isinstance(result, EngineResult)
                        assert result.engine_name == engine_name
                        assert result.success is False
                        assert result.error_message is not None
                        assert "Test error" in result.error_message
                        assert result.execution_time_ms == 0.0
                        assert result.memory_usage_mb == 0.0
                        assert result.memory_baseline_mb == 0.0
                        
                except ImportError:
                    pytest.skip(f"Dependencies for {engine_name} not available")
    
    def _get_mock_dependencies(self, engine_name: str) -> dict:
        """Get appropriate mock dependencies for each engine."""
        if engine_name == "DuckDB":
            return {"duckdb": Mock()}
        elif engine_name == "Arrow":
            return {"pyarrow": Mock()}
        elif engine_name == "Polar":
            return {"polars": Mock()}
        elif engine_name == "Daft":
            return {"daft": Mock()}
        elif engine_name == "StarRocks":
            return {"pymysql": Mock()}
        else:
            return {}


class TestEngineResultContract:
    """Test that EngineResult contract is properly followed."""
    
    def test_engine_result_required_fields(self):
        """Test that EngineResult has all required fields."""
        result = EngineResult(
            engine_name="TestEngine",
            execution_time_ms=1000.0,
            memory_usage_mb=128.0,
            memory_baseline_mb=64.0,
            success=True
        )
        
        # Test required fields exist
        required_fields = [
            'engine_name', 'execution_time_ms', 'memory_usage_mb', 
            'memory_baseline_mb', 'success', 'error_message', 'row_count'
        ]
        
        for field in required_fields:
            assert hasattr(result, field), f"EngineResult missing field: {field}"
    
    def test_engine_result_type_constraints(self):
        """Test that EngineResult fields have correct types."""
        # Successful result
        success_result = EngineResult(
            engine_name="TestEngine",
            execution_time_ms=1000.5,
            memory_usage_mb=128.7,
            memory_baseline_mb=64.3,
            success=True,
            row_count=1000
        )
        
        assert isinstance(success_result.engine_name, str)
        assert isinstance(success_result.execution_time_ms, (int, float))
        assert isinstance(success_result.memory_usage_mb, (int, float))
        assert isinstance(success_result.memory_baseline_mb, (int, float))
        assert isinstance(success_result.success, bool)
        assert success_result.error_message is None
        assert isinstance(success_result.row_count, int)
        
        # Failed result
        failure_result = EngineResult(
            engine_name="FailedEngine",
            execution_time_ms=0.0,
            memory_usage_mb=0.0,
            memory_baseline_mb=0.0,
            success=False,
            error_message="Connection failed"
        )
        
        assert isinstance(failure_result.engine_name, str)
        assert failure_result.success is False
        assert isinstance(failure_result.error_message, str)
        assert failure_result.row_count is None
    
    def test_engine_result_serialization_contract(self):
        """Test that EngineResult can be serialized consistently."""
        result = EngineResult(
            engine_name="TestEngine",
            execution_time_ms=1500.0,
            memory_usage_mb=256.0,
            memory_baseline_mb=128.0,
            success=True,
            row_count=5000
        )
        
        # Test to_dict method
        result_dict = result.to_dict()
        
        # Should contain all fields
        expected_keys = {
            'engine_name', 'execution_time_ms', 'memory_usage_mb',
            'memory_baseline_mb', 'success', 'error_message', 'row_count'
        }
        assert set(result_dict.keys()) == expected_keys
        
        # Values should be JSON-serializable
        import json
        json_str = json.dumps(result_dict)
        parsed = json.loads(json_str)
        
        assert parsed['engine_name'] == "TestEngine"
        assert parsed['success'] is True
        assert parsed['row_count'] == 5000


class TestBenchmarkConfigContract:
    """Test BenchmarkConfig contract compliance."""
    
    def test_benchmark_config_required_fields(self):
        """Test that BenchmarkConfig has required fields."""
        config = BenchmarkConfig(
            hive_metastore_uri="thrift://test:9083",
            table_name="test.table",
            query="SELECT 1"
        )
        
        # Required fields
        assert hasattr(config, 'hive_metastore_uri')
        assert hasattr(config, 'table_name')
        assert hasattr(config, 'query')
        
        # Optional fields with defaults
        assert hasattr(config, 'iterations')
        assert hasattr(config, 'use_multiprocessing')
        assert hasattr(config, 'timeout_seconds')
        assert hasattr(config, 'engines')
        
        # Check default values
        assert config.iterations == 10
        assert config.use_multiprocessing is True
        assert config.timeout_seconds == 600
        assert isinstance(config.engines, list)
        assert len(config.engines) > 0
    
    def test_benchmark_config_validation(self):
        """Test that BenchmarkConfig validates input parameters."""
        # Valid config should work
        valid_config = BenchmarkConfig(
            hive_metastore_uri="thrift://test:9083",
            table_name="test.table",
            query="SELECT COUNT(*) FROM test.table",
            iterations=5,
            engines=["DuckDB"]
        )
        
        assert valid_config.iterations == 5
        assert valid_config.engines == ["DuckDB"]
        
        # Test type constraints
        assert isinstance(valid_config.hive_metastore_uri, str)
        assert isinstance(valid_config.table_name, str)
        assert isinstance(valid_config.query, str)
        assert isinstance(valid_config.iterations, int)
        assert isinstance(valid_config.use_multiprocessing, bool)
        assert isinstance(valid_config.timeout_seconds, int)
        assert isinstance(valid_config.engines, list)
