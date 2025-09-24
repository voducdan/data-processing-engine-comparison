"""Main benchmark orchestrator with multiprocessing support."""

import time
import multiprocessing as mp
from typing import Dict, Any, List
from dataclasses import asdict

from .models.benchmark import (
    BenchmarkConfig, EngineResult, BenchmarkRun, 
    BenchmarkSummary, PerformanceReport
)
from .engines.factory import EngineFactory


def run_single_engine_benchmark(
    engine_name: str, 
    hive_metastore_uri: str, 
    table_name: str, 
    query: str, 
    iterations: int = 10
) -> Dict[str, Any]:
    """Run benchmark for a single engine in a separate process."""
    try:
        engine = EngineFactory.create_engine(engine_name, hive_metastore_uri, table_name, query)
        
        print(f"Running {engine_name} benchmark ({iterations} iterations)...")
        engine_results = []
        successful_runs = []
        
        for i in range(iterations):
            print(f"  Iteration {i+1}/{iterations} for Engine {engine_name}", end=" ")
            result = engine.benchmark()
            engine_results.append(result)
            
            if result.success:
                successful_runs.append(result)
                print(f"✅ {result.execution_time_ms:.2f}ms")
            else:
                print(f"❌ Failed - {result.error_message}")
        
        # Aggregate successful runs
        if successful_runs:
            execution_times = [r.execution_time_ms for r in successful_runs]
            memory_usage = [r.memory_usage_mb for r in successful_runs]
            memory_baseline = [r.memory_baseline_mb for r in successful_runs]
            row_counts = [r.row_count for r in successful_runs if r.row_count is not None]
            
            aggregated_result = EngineResult(
                engine_name=engine_name,
                execution_time_ms=sum(execution_times) / len(execution_times),  # Average
                memory_usage_mb=sum(memory_usage) / len(memory_usage),  # Average
                memory_baseline_mb=sum(memory_baseline) / len(memory_baseline),  # Average
                success=True,
                row_count=row_counts[0] if row_counts else None  # Should be same across runs
            )
            
            print(f"📊 {engine_name} Average: {aggregated_result.execution_time_ms:.2f}ms, "
                  f"{aggregated_result.memory_usage_mb:.2f}MB memory, "
                  f"Success rate: {len(successful_runs)}/{iterations}")
        else:
            aggregated_result = EngineResult(
                engine_name=engine_name,
                execution_time_ms=0.0,
                memory_usage_mb=0.0,
                memory_baseline_mb=0.0,
                success=False,
                error_message="All runs failed"
            )
            print(f"❌ {engine_name}: All runs failed")
        
        return {
            "engine_name": engine_name,
            "success": True,
            "error": None,
            "results": [result.to_dict() for result in engine_results],
            "aggregated_result": aggregated_result.to_dict()
        }
        
    except Exception as e:
        return {
            "engine_name": engine_name,
            "success": False,
            "error": str(e),
            "results": [],
            "aggregated_result": None
        }


class BenchmarkOrchestrator:
    """Main benchmark orchestrator."""
    
    def __init__(self, config: BenchmarkConfig):
        self.config = config
    
    def run_benchmark(self) -> PerformanceReport:
        """Run benchmark on all engines with configured parameters."""
        print(f"Starting benchmark for table: {self.config.table_name}")
        print(f"Query: {self.config.query}")
        mode = "separate processes" if self.config.use_multiprocessing else "sequential mode"
        print(f"Running {self.config.iterations} iterations per engine in {mode}...")
        print("-" * 50)
        
        all_results = []
        aggregated_results = []
        
        if self.config.use_multiprocessing:
            all_results, aggregated_results = self._run_multiprocessing()
        else:
            all_results, aggregated_results = self._run_sequential()
        
        print("-" * 50)
        
        # Generate summary
        summary = self._generate_summary(aggregated_results)
        
        # Create benchmark run metadata
        benchmark_run = BenchmarkRun(
            table_name=self.config.table_name,
            query=self.config.query,
            hive_metastore_uri=self.config.hive_metastore_uri,
            timestamp=time.strftime("%Y-%m-%d %H:%M:%S"),
            iterations=self.config.iterations,
            multiprocessing=self.config.use_multiprocessing
        )
        
        return PerformanceReport(
            benchmark_run=benchmark_run,
            aggregated_results=aggregated_results,
            all_iterations=all_results,
            summary=summary
        )
    
    def _run_multiprocessing(self) -> tuple[List[EngineResult], List[EngineResult]]:
        """Run engines in parallel using multiprocessing."""
        all_results = []
        aggregated_results = []
        
        with mp.Pool(processes=min(len(self.config.engines), mp.cpu_count())) as pool:
            # Create tasks for each engine
            tasks = [
                pool.apply_async(
                    run_single_engine_benchmark, 
                    args=(engine_name, self.config.hive_metastore_uri, 
                          self.config.table_name, self.config.query, self.config.iterations)
                )
                for engine_name in self.config.engines
            ]
            
            # Collect results from each process
            for task in tasks:
                try:
                    engine_result = task.get(timeout=self.config.timeout_seconds)
                    
                    if engine_result["success"]:
                        # Convert results back to EngineResult objects
                        engine_results = [
                            EngineResult(**result_dict) 
                            for result_dict in engine_result["results"]
                        ]
                        all_results.extend(engine_results)
                        
                        # Add aggregated result
                        if engine_result["aggregated_result"]:
                            aggregated_result = EngineResult(**engine_result["aggregated_result"])
                            aggregated_results.append(aggregated_result)
                    else:
                        # Create failed result
                        failed_result = EngineResult(
                            engine_name=engine_result["engine_name"],
                            execution_time_ms=0.0,
                            memory_usage_mb=0.0,
                            memory_baseline_mb=0.0,
                            success=False,
                            error_message=engine_result["error"]
                        )
                        aggregated_results.append(failed_result)
                        print(f"❌ {engine_result['engine_name']}: Process failed - {engine_result['error']}")
                        
                except mp.TimeoutError:
                    print(f"❌ Engine process timed out after {self.config.timeout_seconds} seconds")
                except Exception as e:
                    print(f"❌ Error collecting results from engine process: {e}")
        
        return all_results, aggregated_results
    
    def _run_sequential(self) -> tuple[List[EngineResult], List[EngineResult]]:
        """Run engines sequentially."""
        all_results = []
        aggregated_results = []
        
        for engine_name in self.config.engines:
            try:
                engine_result = run_single_engine_benchmark(
                    engine_name, self.config.hive_metastore_uri, 
                    self.config.table_name, self.config.query, self.config.iterations
                )
                
                if engine_result["success"]:
                    # Convert results back to EngineResult objects
                    engine_results = [
                        EngineResult(**result_dict) 
                        for result_dict in engine_result["results"]
                    ]
                    all_results.extend(engine_results)
                    
                    # Add aggregated result
                    if engine_result["aggregated_result"]:
                        aggregated_result = EngineResult(**engine_result["aggregated_result"])
                        aggregated_results.append(aggregated_result)
                else:
                    # Create failed result
                    failed_result = EngineResult(
                        engine_name=engine_result["engine_name"],
                        execution_time_ms=0.0,
                        memory_usage_mb=0.0,
                        memory_baseline_mb=0.0,
                        success=False,
                        error_message=engine_result["error"]
                    )
                    aggregated_results.append(failed_result)
                    print(f"❌ {engine_result['engine_name']}: Failed - {engine_result['error']}")
                    
            except Exception as e:
                print(f"❌ Error running {engine_name}: {e}")
        
        return all_results, aggregated_results
    
    def _generate_summary(self, aggregated_results: List[EngineResult]) -> BenchmarkSummary:
        """Generate summary statistics from aggregated results."""
        successful_results = [r for r in aggregated_results if r.success]
        
        if successful_results:
            fastest = min(successful_results, key=lambda x: x.execution_time_ms)
            most_memory_efficient = min(successful_results, key=lambda x: x.memory_usage_mb)
            
            print(f"🏆 Fastest (avg): {fastest.engine_name} ({fastest.execution_time_ms:.2f}ms)")
            print(f"💾 Most Memory Efficient (avg): {most_memory_efficient.engine_name} ({most_memory_efficient.memory_usage_mb:.2f}MB)")
            
            return BenchmarkSummary(
                fastest_engine=fastest.engine_name,
                most_memory_efficient=most_memory_efficient.engine_name,
                success_count=len(successful_results),
                total_engines=len(aggregated_results)
            )
        else:
            print("❌ No engines completed successfully")
            return BenchmarkSummary(
                fastest_engine=None,
                most_memory_efficient=None,
                success_count=0,
                total_engines=len(aggregated_results)
            )
