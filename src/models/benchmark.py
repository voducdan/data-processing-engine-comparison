"""Core data models for the benchmark system."""

from dataclasses import dataclass, asdict
from typing import Optional, Dict, Any, List
from datetime import datetime
import json


@dataclass
class EngineResult:
    """Performance metrics for a single engine execution."""
    engine_name: str
    execution_time_ms: float
    memory_usage_mb: float
    memory_baseline_mb: float
    success: bool
    error_message: Optional[str] = None
    row_count: Optional[int] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return asdict(self)


@dataclass
class BenchmarkConfig:
    """Configuration for benchmark execution."""
    hive_metastore_uri: str
    table_name: str
    query: str
    iterations: int = 10
    use_multiprocessing: bool = True
    timeout_seconds: int = 600
    engines: List[str] = None
    
    def __post_init__(self):
        if self.engines is None:
            self.engines = ["Polar", "Arrow", "Daft", "DuckDB", "StarRocks"]


@dataclass 
class BenchmarkRun:
    """Metadata about a benchmark execution."""
    table_name: str
    query: str
    hive_metastore_uri: str
    timestamp: str
    iterations: int
    multiprocessing: bool
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return asdict(self)


@dataclass
class BenchmarkSummary:
    """Summary statistics from benchmark execution."""
    fastest_engine: Optional[str]
    most_memory_efficient: Optional[str]
    success_count: int
    total_engines: int
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return asdict(self)


@dataclass
class PerformanceReport:
    """Complete performance report containing all benchmark data."""
    benchmark_run: BenchmarkRun
    aggregated_results: List[EngineResult]
    all_iterations: List[EngineResult]
    summary: BenchmarkSummary
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert entire report to dictionary."""
        return {
            "benchmark_run": self.benchmark_run.to_dict(),
            "aggregated_results": [result.to_dict() for result in self.aggregated_results],
            "all_iterations": [result.to_dict() for result in self.all_iterations],
            "summary": self.summary.to_dict()
        }
    
    def to_json(self, indent: int = 2) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), indent=indent)
    
    def save_to_file(self, filepath: str) -> None:
        """Save report to JSON file."""
        with open(filepath, 'w') as f:
            f.write(self.to_json())
