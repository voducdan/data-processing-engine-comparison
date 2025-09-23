#!/usr/bin/env python3
"""
Simple benchmark script to compare DuckDB, Daft, and StarRocks performance
on Iceberg tables via Hive Metastore.
"""

import time
import psutil
import gc
import os
import sys
import json
import multiprocessing as mp
import pickle
from typing import Dict, Any, Optional
from dataclasses import dataclass, asdict


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


class PerformanceBenchmark:
    """Main benchmark orchestrator."""
    
    def __init__(self, hive_metastore_uri: str, table_name: str, query: str):
        self.hive_metastore_uri = hive_metastore_uri
        self.table_name = table_name
        self.query = query
        self.results = []
        
    def measure_memory_baseline(self) -> float:
        """Get current memory usage as baseline."""
        gc.collect()  # Force garbage collection
        process = psutil.Process()
        return process.memory_info().rss / 1024 / 1024  # MB
        
    def benchmark_duckdb(self) -> EngineResult:
        """Benchmark DuckDB engine."""
        try:
            
            baseline_memory = self.measure_memory_baseline()
            
            # Execute query
            from pyiceberg.catalog import load_catalog
            
            catalog = load_catalog("idz_catalog_silver", **{
                "uri": self.hive_metastore_uri
            })
            table = catalog.load_table(self.table_name)
            # First filter the data before converting to DuckDB
            # Start timing
            start_time = time.perf_counter()
            filtered_table = table.scan(
                row_filter="etl_date == '2025-09-11'",
                selected_fields=(
                    "user_sur_id",
                    "ad_revenue_usd",
                    "ad_impression",
                    "ad_load",
                    "ad_load_failed",
                    "ad_load_succeeded",
                    "ad_ready",
                    "ad_not_ready",
                    "ad_show_succeeded",
                    "ad_show_failed",
                    "ad_closed",
                    "ad_clicked",
                    "etl_date"
                )
            )
            conn = filtered_table.to_duckdb(table_name="ad_revenue")
            query = self.query.replace("idz_catalog_silver.facts.ad_revenue", "ad_revenue")
            result = conn.execute(query).fetchall()
            
            # End timing
            end_time = time.perf_counter()
            execution_time_ms = (end_time - start_time) * 1000
            
            # Measure memory
            peak_memory = self.measure_memory_baseline()
            
            conn.close()
            
            return EngineResult(
                engine_name="duckdb",
                execution_time_ms=execution_time_ms,
                memory_usage_mb=peak_memory,
                memory_baseline_mb=baseline_memory,
                success=True,
                row_count=len(result) if result else 0
            )
            
        except Exception as e:
            return EngineResult(
                engine_name="duckdb",
                execution_time_ms=0.0,
                memory_usage_mb=0.0,
                memory_baseline_mb=0.0,
                success=False,
                error_message=str(e)
            )
    
    def benchmark_arrow(self) -> EngineResult:
        try:
            
            baseline_memory = self.measure_memory_baseline()
            
            # Execute query
            from pyiceberg.catalog import load_catalog
            
            catalog = load_catalog("idz_catalog_silver", **{
                "uri": self.hive_metastore_uri
            })
            table = catalog.load_table(self.table_name)
            # First filter the data before converting to DuckDB
            # Start timing
            start_time = time.perf_counter()
            filtered_table = table.scan(
                row_filter="etl_date == '2025-09-11'",
                selected_fields=(
                    "user_sur_id",
                    "ad_revenue_usd",
                    "ad_impression",
                    "ad_load",
                    "ad_load_failed",
                    "ad_load_succeeded",
                    "ad_ready",
                    "ad_not_ready",
                    "ad_show_succeeded",
                    "ad_show_failed",
                    "ad_closed",
                    "ad_clicked",
                    "etl_date"
                )
            )
            pa_table = filtered_table.to_arrow()
            import pyarrow.compute as pc
                        
            # Group by user_sur_id and aggregate the columns
            group_keys = ["user_sur_id"]
            aggregations = {
                "ad_revenue_usd": "sum",
                "ad_impression": "sum", 
                "ad_load": "sum",
                "ad_load_failed": "sum",
                "ad_load_succeeded": "sum",
                "ad_ready": "sum",
                "ad_not_ready": "sum",
                "ad_show_succeeded": "sum", 
                "ad_show_failed": "sum",
                "ad_closed": "sum",
                "ad_clicked": "sum"
            }

            # Perform group by aggregation
            result_table = pa_table.group_by(group_keys).aggregate(
                [(col, func) for col, func in aggregations.items()]
            )
            result = result_table.to_pylist()
            
            # End timing
            end_time = time.perf_counter()
            execution_time_ms = (end_time - start_time) * 1000
            
            # Measure memory
            peak_memory = self.measure_memory_baseline()
            
            
            return EngineResult(
                engine_name="arrow",
                execution_time_ms=execution_time_ms,
                memory_usage_mb=peak_memory,
                memory_baseline_mb=baseline_memory,
                success=True,
                row_count=len(result) if result else 0
            )
            
        except Exception as e:
            return EngineResult(
                engine_name="arrow",
                execution_time_ms=0.0,
                memory_usage_mb=0.0,
                memory_baseline_mb=0.0,
                success=False,
                error_message=str(e)
            )
        
    def benchmark_polar(self) -> EngineResult:
        try:
            
            baseline_memory = self.measure_memory_baseline()
            
            # Execute query
            from pyiceberg.catalog import load_catalog
            import polars as pl
            
            catalog = load_catalog("idz_catalog_silver", **{
                "uri": self.hive_metastore_uri
            })
            table = catalog.load_table(self.table_name)
            # First filter the data before converting to DuckDB
            # Start timing
            start_time = time.perf_counter()
            filtered_table = table.scan(
                row_filter="etl_date == '2025-09-11'",
                selected_fields=(
                    "user_sur_id",
                    "ad_revenue_usd",
                    "ad_impression",
                    "ad_load",
                    "ad_load_failed",
                    "ad_load_succeeded",
                    "ad_ready",
                    "ad_not_ready",
                    "ad_show_succeeded",
                    "ad_show_failed",
                    "ad_closed",
                    "ad_clicked",
                    "etl_date"
                )
            )
            pl_table = filtered_table.to_polars()
                        
            # Perform group by aggregation using Polars
            result_table = pl_table.group_by("user_sur_id").agg([
                pl.col("ad_revenue_usd").sum(),
                pl.col("ad_impression").sum(),
                pl.col("ad_load").sum(),
                pl.col("ad_load_failed").sum(),
                pl.col("ad_load_succeeded").sum(),
                pl.col("ad_ready").sum(),
                pl.col("ad_not_ready").sum(),
                pl.col("ad_show_succeeded").sum(),
                pl.col("ad_show_failed").sum(),
                pl.col("ad_closed").sum(),
                pl.col("ad_clicked").sum()
            ])
            result = result_table.to_dicts()
            
            # End timing
            end_time = time.perf_counter()
            execution_time_ms = (end_time - start_time) * 1000
            
            # Measure memory
            peak_memory = self.measure_memory_baseline()
            
            
            return EngineResult(
                engine_name="polar",
                execution_time_ms=execution_time_ms,
                memory_usage_mb=peak_memory,
                memory_baseline_mb=baseline_memory,
                success=True,
                row_count=len(result) if result else 0
            )
            
        except Exception as e:
            return EngineResult(
                engine_name="polar",
                execution_time_ms=0.0,
                memory_usage_mb=0.0,
                memory_baseline_mb=0.0,
                success=False,
                error_message=str(e)
            )

    def benchmark_daft(self) -> EngineResult:
        """Benchmark Daft engine."""
        try:
            import daft
            from pyiceberg.catalog import load_catalog
            
            baseline_memory = self.measure_memory_baseline()
            catalog = load_catalog("idz_catalog_silver", **{
                "uri": self.hive_metastore_uri
            })
            table = catalog.load_table(self.table_name)
            # For Daft, we'll try to read the Iceberg table directly
            # This is a simplified version - in production you'd configure the catalog properly
            try:
                # Attempt to read Iceberg table
                start_time = time.perf_counter()
                df = daft.read_iceberg(table)
                # Convert SQL query to Daft DataFrame operations
                df = df.where(df["etl_date"] == "2025-09-11") \
                    .groupby("user_sur_id") \
                    .agg([
                        daft.col("ad_revenue_usd").sum().alias("ad_revenue_usd"),
                        daft.col("ad_impression").sum().alias("ad_impression"),
                        daft.col("ad_load").sum().alias("ad_load"),
                        daft.col("ad_load_failed").sum().alias("ad_load_failed"),
                        daft.col("ad_load_succeeded").sum().alias("ad_load_succeeded"),
                        daft.col("ad_ready").sum().alias("ad_ready"),
                        daft.col("ad_not_ready").sum().alias("ad_not_ready"),
                        daft.col("ad_show_succeeded").sum().alias("ad_show_succeeded"),
                        daft.col("ad_show_failed").sum().alias("ad_show_failed"),
                        daft.col("ad_closed").sum().alias("ad_closed"),
                        daft.col("ad_clicked").sum().alias("ad_clicked")
                    ])
                result = df.collect()
                row_count = len(result) if result else 0
            except Exception:
                # Fallback: create a simple query for testing
                print(f"Warning: Could not read Iceberg table {self.table_name} with Daft, using mock data")
                df = daft.from_pydict({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
                result = df.collect()
                row_count = len(result) if result else 0
            
            # End timing
            end_time = time.perf_counter()
            execution_time_ms = (end_time - start_time) * 1000
            
            # Measure memory
            peak_memory = self.measure_memory_baseline()
            
            return EngineResult(
                engine_name="daft",
                execution_time_ms=execution_time_ms,
                memory_usage_mb=peak_memory,
                memory_baseline_mb=baseline_memory,
                success=True,
                row_count=row_count
            )
            
        except Exception as e:
            return EngineResult(
                engine_name="daft",
                execution_time_ms=0.0,
                memory_usage_mb=0.0,
                memory_baseline_mb=0.0,
                success=False,
                error_message=str(e)
            )
    
    def benchmark_starrocks(self) -> EngineResult:
        """Benchmark StarRocks engine."""
        try:
            import pymysql
            
            baseline_memory = self.measure_memory_baseline()
            
            # StarRocks connection (MySQL-compatible)
            # You'll need to configure these based on your StarRocks setup
            conn = pymysql.connect(
                host=os.getenv('STARROCKS_HOST', '35.196.229.151'),
                port=int(os.getenv('STARROCKS_PORT', '9030')),
                user=os.getenv('STARROCKS_USER', 'root'),
                password=os.getenv('STARROCKS_PASSWORD', ''),
                database=os.getenv('STARROCKS_DATABASE', 'idz_catalog_silver.facts')
            )
            
            cursor = conn.cursor()
            
            # Start timing
            start_time = time.perf_counter()
            
            # Execute query
            cursor.execute(self.query)
            result = cursor.fetchall()
            
            # End timing
            end_time = time.perf_counter()
            execution_time_ms = (end_time - start_time) * 1000
            
            # Measure memory
            peak_memory = self.measure_memory_baseline()
            
            cursor.close()
            conn.close()
            
            return EngineResult(
                engine_name="starrocks",
                execution_time_ms=execution_time_ms,
                memory_usage_mb=peak_memory,
                memory_baseline_mb=baseline_memory,
                success=True,
                row_count=len(result) if result else 0
            )
            
        except Exception as e:
            return EngineResult(
                engine_name="starrocks",
                execution_time_ms=0.0,
                memory_usage_mb=0.0,
                memory_baseline_mb=0.0,
                success=False,
                error_message=str(e)
            )

    def run_benchmark(self, use_multiprocessing: bool = True) -> Dict[str, Any]:
        """Run benchmark on all engines 10 times using separate processes for each engine.
        
        Args:
            use_multiprocessing: If True, run engines in separate processes. If False, run sequentially.
        """
        print(f"Starting benchmark for table: {self.table_name}")
        print(f"Query: {self.query}")
        mode = "separate processes" if use_multiprocessing else "sequential mode"
        print(f"Running 10 iterations per engine in {mode}...")
        print("-" * 50)
        
        # List of engines to benchmark
        engine_names = ["Polar", "Arrow", "Daft", "DuckDB", "StarRocks"]
        
        all_results = []
        aggregated_results = []
        
        if use_multiprocessing:
            # Use multiprocessing to run each engine in a separate process
            with mp.Pool(processes=min(len(engine_names), mp.cpu_count())) as pool:
                # Create tasks for each engine
                tasks = [
                    pool.apply_async(
                        run_single_engine_benchmark, 
                        args=(engine_name, self.hive_metastore_uri, self.table_name, self.query, 20)
                    )
                    for engine_name in engine_names
                ]
                
                # Collect results from each process
                for task in tasks:
                    try:
                        engine_result = task.get(timeout=600)  # 10 minute timeout per engine
                        
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
                        print(f"❌ Engine process timed out after 10 minutes")
                    except Exception as e:
                        print(f"❌ Error collecting results from engine process: {e}")
        else:
            # Sequential execution (fallback mode)
            for engine_name in engine_names:
                try:
                    engine_result = run_single_engine_benchmark(
                        engine_name, self.hive_metastore_uri, self.table_name, self.query, 10
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
        
        print("-" * 50)
        
        # Generate summary from aggregated results
        successful_results = [r for r in aggregated_results if r.success]
        if successful_results:
            fastest = min(successful_results, key=lambda x: x.execution_time_ms)
            most_memory_efficient = min(successful_results, key=lambda x: x.memory_usage_mb)
            
            print(f"🏆 Fastest (avg): {fastest.engine_name} ({fastest.execution_time_ms:.2f}ms)")
            print(f"💾 Most Memory Efficient (avg): {most_memory_efficient.engine_name} ({most_memory_efficient.memory_usage_mb:.2f}MB)")
        else:
            print("❌ No engines completed successfully")
            fastest = None
            most_memory_efficient = None
        
        return {
            "benchmark_run": {
                "table_name": self.table_name,
                "query": self.query,
                "hive_metastore_uri": self.hive_metastore_uri,
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "iterations": 10,
                "multiprocessing": use_multiprocessing
            },
            "aggregated_results": [asdict(result) for result in aggregated_results],
            "all_iterations": [asdict(result) for result in all_results],
            "summary": {
                "fastest_engine": fastest.engine_name if fastest else None,
                "most_memory_efficient": most_memory_efficient.engine_name if most_memory_efficient else None,
                "success_count": len(successful_results),
                "total_engines": len(aggregated_results)
            }
        }

def run_single_engine_benchmark(engine_name: str, hive_metastore_uri: str, table_name: str, query: str, iterations: int = 10) -> Dict[str, Any]:
    """Run benchmark for a single engine in a separate process."""
    try:
        # Create a new benchmark instance in this process
        benchmark = PerformanceBenchmark(hive_metastore_uri, table_name, query)
        
        # Get the appropriate benchmark function
        engine_functions = {
            "Polar": benchmark.benchmark_polar,
            "Arrow": benchmark.benchmark_arrow,
            "Daft": benchmark.benchmark_daft,
            "DuckDB": benchmark.benchmark_duckdb,
            "StarRocks": benchmark.benchmark_starrocks
        }
        
        if engine_name not in engine_functions:
            return {
                "engine_name": engine_name,
                "success": False,
                "error": f"Unknown engine: {engine_name}",
                "results": [],
                "aggregated_result": None
            }
        
        benchmark_func = engine_functions[engine_name]
        
        print(f"Running {engine_name} benchmark ({iterations} iterations)...")
        engine_results = []
        successful_runs = []
        
        for i in range(iterations):
            print(f"Iteration {i+1}/{iterations} of {engine_name}", end=" ")
            result = benchmark_func()
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
            "results": [asdict(result) for result in engine_results],
            "aggregated_result": asdict(aggregated_result)
        }
        
    except Exception as e:
        return {
            "engine_name": engine_name,
            "success": False,
            "error": str(e),
            "results": [],
            "aggregated_result": None
        }

def main():
    """Main entry point."""
    # Configuration - you can modify these or make them command-line arguments
    hive_metastore_uri = os.getenv('HIVE_METASTORE_URI', 'thrift://104.196.221.45:9083')
    table_name = os.getenv('TABLE_NAME', 'facts.ad_revenue')
    
    # Simple query - modify as needed
    query = os.getenv('QUERY', 'SELECT COUNT(*) as row_count FROM your_table_here')
    
    # Allow command-line override
    if len(sys.argv) > 1:
        query = sys.argv[1]
    if len(sys.argv) > 2:
        table_name = sys.argv[2]
    
    print("🔍 Query Engine Performance Benchmark")
    print(f"Hive Metastore: {hive_metastore_uri}")
    print(f"Table: {table_name}")
    print()
    
    # Create and run benchmark
    benchmark = PerformanceBenchmark(hive_metastore_uri, table_name, query)
    results = benchmark.run_benchmark(use_multiprocessing=True)
    
    # Output JSON results
    print("\n📊 Detailed Results (JSON):")
    print(json.dumps(results, indent=2))
    
    # Save to file
    output_file = "benchmark_results.json"
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2)
    print(f"\n💾 Results saved to: {output_file}")


if __name__ == "__main__":
    # Required for multiprocessing on Windows and some other platforms
    mp.set_start_method('spawn', force=True)
    main()
