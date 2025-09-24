#!/usr/bin/env python3
"""
Modern CLI for the query engine benchmark system.
"""

import sys
import multiprocessing as mp
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

from src.utils.config import ConfigLoader
from src.benchmark_runner import BenchmarkOrchestrator


def main():
    """Main entry point for the benchmark CLI."""
    # Parse command line arguments
    args = {}
    if len(sys.argv) > 1:
        args['query'] = sys.argv[1]
    if len(sys.argv) > 2:
        args['table_name'] = sys.argv[2]
    
    # Load configuration
    try:
        # Try to load from YAML first, fall back to env
        config_file = Path(__file__).parent / "benchmark.yaml"
        if config_file.exists():
            config = ConfigLoader.from_yaml(str(config_file))
        else:
            config = ConfigLoader.from_env()
        
        # Override with command line args
        if args:
            config = ConfigLoader.from_args(args)
            
    except Exception as e:
        print(f"❌ Error loading configuration: {e}")
        return 1
    
    print("🔍 Query Engine Performance Benchmark")
    print(f"Hive Metastore: {config.hive_metastore_uri}")
    print(f"Table: {config.table_name}")
    print(f"Engines: {', '.join(config.engines)}")
    print()
    
    # Create and run benchmark
    try:
        orchestrator = BenchmarkOrchestrator(config)
        report = orchestrator.run_benchmark()
        
        # Output results
        print("\n📊 Detailed Results (JSON):")
        print(report.to_json())
        
        # Save to file
        output_file = "benchmark_results.json"
        report.save_to_file(output_file)
        print(f"\n💾 Results saved to: {output_file}")
        
        return 0
        
    except Exception as e:
        print(f"❌ Benchmark execution failed: {e}")
        return 1


if __name__ == "__main__":
    # Required for multiprocessing on Windows and some other platforms
    mp.set_start_method('spawn', force=True)
    sys.exit(main())
