"""Performance measurement utilities."""

import time
import psutil
import gc
from typing import Any


class MemoryMonitor:
    """Utility for measuring memory usage."""
    
    @staticmethod
    def get_memory_baseline() -> float:
        """Get current memory usage as baseline in MB."""
        gc.collect()  # Force garbage collection
        process = psutil.Process()
        return process.memory_info().rss / 1024 / 1024  # Convert to MB


class ExecutionTimer:
    """Context manager for timing code execution."""
    
    def __init__(self):
        self.start_time = None
        self.end_time = None
        self.execution_time_ms = None
    
    def __enter__(self):
        self.start_time = time.perf_counter()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end_time = time.perf_counter()
        self.execution_time_ms = (self.end_time - self.start_time) * 1000
    
    def get_execution_time_ms(self) -> float:
        """Get execution time in milliseconds."""
        if self.execution_time_ms is None:
            raise RuntimeError("Timer has not been used in a with statement")
        return self.execution_time_ms


def benchmark_execution(func: callable, *args, **kwargs) -> tuple[Any, float, float, float]:
    """
    Execute a function and measure its performance.
    
    Returns:
        Tuple of (result, execution_time_ms, memory_baseline_mb, memory_usage_mb)
    """
    memory_monitor = MemoryMonitor()
    baseline_memory = memory_monitor.get_memory_baseline()
    
    with ExecutionTimer() as timer:
        result = func(*args, **kwargs)
    
    peak_memory = memory_monitor.get_memory_baseline()
    
    return result, timer.get_execution_time_ms(), baseline_memory, peak_memory
