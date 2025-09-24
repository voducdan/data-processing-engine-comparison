"""Unit tests for engine factory and base engine."""

import pytest
from unittest.mock import Mock, patch
from src.engines.base import BaseEngine
from src.engines.factory import EngineFactory
from src.models.benchmark import EngineResult


class MockEngine(BaseEngine):
    """Mock engine for testing."""
    
    @property
    def engine_name(self) -> str:
        return "MockEngine"
    
    def _execute_query(self) -> tuple[list, int]:
        return [{"result": "success"}], 1


class FailingMockEngine(BaseEngine):
    """Mock engine that always fails for testing."""
    
    @property
    def engine_name(self) -> str:
        return "FailingMockEngine"
    
    def _execute_query(self) -> tuple[list, int]:
        raise Exception("Mock failure")


class TestBaseEngine:
    """Test BaseEngine abstract class."""
    
    def test_successful_benchmark(self):
        """Test successful benchmark execution."""
        engine = MockEngine("test://uri", "test.table", "SELECT 1")
        
        with patch('src.engines.base.benchmark_execution') as mock_benchmark:
            # Mock benchmark_execution to return expected values
            mock_benchmark.return_value = (
                ([{"result": "success"}], 1),  # result
                1500.0,  # execution_time_ms
                64.0,    # baseline_memory
                128.0    # peak_memory
            )
            
            result = engine.benchmark()
            
            assert isinstance(result, EngineResult)
            assert result.engine_name == "MockEngine"
            assert result.execution_time_ms == 1500.0
            assert result.memory_baseline_mb == 64.0
            assert result.memory_usage_mb == 128.0
            assert result.success is True
            assert result.row_count == 1
            assert result.error_message is None
    
    def test_failed_benchmark(self):
        """Test benchmark execution with failure."""
        engine = FailingMockEngine("test://uri", "test.table", "SELECT 1")
        
        result = engine.benchmark()
        
        assert isinstance(result, EngineResult)
        assert result.engine_name == "FailingMockEngine"
        assert result.success is False
        assert result.error_message == "Mock failure"
        assert result.execution_time_ms == 0.0
        assert result.memory_usage_mb == 0.0
        assert result.memory_baseline_mb == 0.0


class TestEngineFactory:
    """Test EngineFactory class."""
    
    def test_get_available_engines(self):
        """Test getting list of available engines."""
        engines = EngineFactory.get_available_engines()
        
        expected_engines = {"DuckDB", "Arrow", "Polar", "Daft", "StarRocks"}
        assert set(engines) == expected_engines
    
    def test_create_engine_success(self):
        """Test successful engine creation."""
        engine = EngineFactory.create_engine(
            "DuckDB",
            "test://uri", 
            "test.table",
            "SELECT 1"
        )
        
        assert engine.engine_name == "DuckDB"
        assert engine.hive_metastore_uri == "test://uri"
        assert engine.table_name == "test.table"
        assert engine.query == "SELECT 1"
    
    def test_create_engine_unknown(self):
        """Test creation of unknown engine raises error."""
        with pytest.raises(ValueError) as excinfo:
            EngineFactory.create_engine(
                "UnknownEngine",
                "test://uri",
                "test.table", 
                "SELECT 1"
            )
        
        assert "Unknown engine: UnknownEngine" in str(excinfo.value)
        assert "Available engines:" in str(excinfo.value)
    
    def test_register_custom_engine(self):
        """Test registering a custom engine."""
        # Register mock engine
        EngineFactory.register_engine("MockEngine", MockEngine)
        
        # Verify it's available
        engines = EngineFactory.get_available_engines()
        assert "MockEngine" in engines
        
        # Verify it can be created
        engine = EngineFactory.create_engine(
            "MockEngine",
            "test://uri",
            "test.table",
            "SELECT 1"
        )
        
        assert engine.engine_name == "MockEngine"
        assert isinstance(engine, MockEngine)
        
        # Cleanup - remove the registered engine for other tests
        if "MockEngine" in EngineFactory._engines:
            del EngineFactory._engines["MockEngine"]
