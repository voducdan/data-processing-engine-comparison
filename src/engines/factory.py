"""Engine factory for creating benchmark engine instances."""

from typing import Dict, Type
from .base import BaseEngine
from .duckdb_engine import DuckDBEngine
from .arrow_engine import ArrowEngine
from .polar_engine import PolarEngine
from .daft_engine import DaftEngine
from .starrocks_engine import StarRocksEngine


class EngineFactory:
    """Factory for creating engine instances."""
    
    _engines: Dict[str, Type[BaseEngine]] = {
        "DuckDB": DuckDBEngine,
        "Arrow": ArrowEngine,
        "Polar": PolarEngine,
        "Daft": DaftEngine,
        "StarRocks": StarRocksEngine
    }
    
    @classmethod
    def create_engine(cls, engine_name: str, hive_metastore_uri: str, table_name: str, query: str) -> BaseEngine:
        """Create an engine instance by name."""
        if engine_name not in cls._engines:
            raise ValueError(f"Unknown engine: {engine_name}. Available engines: {list(cls._engines.keys())}")
        
        engine_class = cls._engines[engine_name]
        return engine_class(hive_metastore_uri, table_name, query)
    
    @classmethod
    def get_available_engines(cls) -> list[str]:
        """Get list of available engine names."""
        return list(cls._engines.keys())
    
    @classmethod
    def register_engine(cls, name: str, engine_class: Type[BaseEngine]) -> None:
        """Register a new engine type."""
        cls._engines[name] = engine_class
