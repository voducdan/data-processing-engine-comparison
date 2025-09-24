"""Base engine interface for benchmarking."""

from abc import ABC, abstractmethod
from typing import Any

from ..models.benchmark import EngineResult
from ..utils.metrics import benchmark_execution


class BaseEngine(ABC):
    """Abstract base class for all benchmark engines."""
    
    def __init__(self, hive_metastore_uri: str, table_name: str, query: str):
        self.hive_metastore_uri = hive_metastore_uri
        self.table_name = table_name
        self.query = query
    
    @property
    @abstractmethod
    def engine_name(self) -> str:
        """Return the name of this engine."""
        pass
    
    @abstractmethod
    def _execute_query(self) -> tuple[Any, int]:
        """
        Execute the query and return (result, row_count).
        This method should contain the actual query execution logic.
        """
        pass
    
    def benchmark(self) -> EngineResult:
        """Execute benchmark and return performance metrics."""
        try:
            result, execution_time_ms, baseline_memory, peak_memory = benchmark_execution(
                self._execute_query
            )
            
            _, row_count = result
            
            return EngineResult(
                engine_name=self.engine_name,
                execution_time_ms=execution_time_ms,
                memory_usage_mb=peak_memory,
                memory_baseline_mb=baseline_memory,
                success=True,
                row_count=row_count
            )
            
        except Exception as e:
            return EngineResult(
                engine_name=self.engine_name,
                execution_time_ms=0.0,
                memory_usage_mb=0.0,
                memory_baseline_mb=0.0,
                success=False,
                error_message=str(e)
            )
