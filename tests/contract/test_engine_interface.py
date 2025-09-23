"""Contract tests for BaseEngine interface."""
import pytest
from abc import ABC
from dataclasses import dataclass
from typing import Dict, Any, Optional


class TestBaseEngineInterface:
    """Test BaseEngine abstract interface contract."""
    
    def test_base_engine_is_abstract(self):
        """Test BaseEngine cannot be instantiated directly."""
        # Import will fail until BaseEngine is implemented
        with pytest.raises(ImportError):
            from src.engines.base import BaseEngine
            
    def test_base_engine_abstract_methods(self):
        """Test BaseEngine has required abstract methods."""
        # This will fail until BaseEngine is implemented
        from src.engines.base import BaseEngine
        
        # Check abstract methods exist
        abstract_methods = BaseEngine.__abstractmethods__
        required_methods = {"connect", "execute_query", "disconnect", "validate_connectivity"}
        assert required_methods.issubset(abstract_methods)
        
    def test_query_result_dataclass(self):
        """Test QueryResult dataclass structure."""
        # This will fail until QueryResult is implemented
        from src.engines.base import QueryResult
        
        # Test required fields
        result = QueryResult(
            execution_time_ms=100.5,
            memory_usage_mb=256.7,
            memory_baseline_mb=128.3,
            success=True
        )
        
        assert result.execution_time_ms == 100.5
        assert result.memory_usage_mb == 256.7
        assert result.memory_baseline_mb == 128.3
        assert result.success is True
        assert result.error_message is None
        assert result.row_count is None
        
    def test_query_result_optional_fields(self):
        """Test QueryResult optional fields."""
        from src.engines.base import QueryResult
        
        result = QueryResult(
            execution_time_ms=100.5,
            memory_usage_mb=256.7,
            memory_baseline_mb=128.3,
            success=False,
            error_message="Query failed",
            row_count=1000
        )
        
        assert result.error_message == "Query failed"
        assert result.row_count == 1000
        
    def test_base_engine_cannot_be_instantiated(self):
        """Test BaseEngine raises TypeError when instantiated."""
        from src.engines.base import BaseEngine
        
        with pytest.raises(TypeError):
            BaseEngine()
            
    def test_engine_lifecycle_methods(self):
        """Test engine has correct method signatures."""
        from src.engines.base import BaseEngine
        import inspect
        
        # Check method signatures
        connect_sig = inspect.signature(BaseEngine.connect)
        assert len(connect_sig.parameters) == 2  # self, config
        assert "config" in connect_sig.parameters
        
        execute_sig = inspect.signature(BaseEngine.execute_query)
        assert len(execute_sig.parameters) == 2  # self, query
        assert "query" in execute_sig.parameters
        
        disconnect_sig = inspect.signature(BaseEngine.disconnect)
        assert len(disconnect_sig.parameters) == 1  # self only
        
        validate_sig = inspect.signature(BaseEngine.validate_connectivity)
        assert len(validate_sig.parameters) == 1  # self only
