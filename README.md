# Query Engine Performance Comparison

A Python benchmark system for comparing the performance of multiple query engines (Polar, Arrow, Daft, DuckDB, and StarRocks) when executing queries against Iceberg tables via Hive Metastore.

**🏗️ Refactored Architecture v2.0:**
- **Modular Design**: Clean separation of concerns with dedicated modules
- **Plugin Architecture**: Easy to add new engines via `BaseEngine` interface
- **Flexible Configuration**: Environment variables, YAML files, and CLI options
- **Type Safety**: Full type hints and structured data models
- **Enhanced Testing**: Unit testable components with dependency injection

**✨ Performance Features:**
- **Multiprocessing Support**: Each engine runs in a separate process for complete isolation
- **Parallel Execution**: Multiple engines can run simultaneously  
- **Fault Isolation**: Engine crashes don't affect other engines
- **Comprehensive Metrics**: Execution time, memory usage, and success rates
- **Timeout Protection**: 10-minute timeout per engine

## Quick Start

1. **Install Dependencies**:
   ```bash
   pip install pyiceberg[hive] duckdb getdaft pymysql pyarrow psutil pyyaml
   ```

2. **Set Environment Variables** (copy from benchmark.env):
   ```bash
   export HIVE_METASTORE_URI="thrift://localhost:9083"
   export TABLE_NAME="warehouse.default.sample_table"
   export QUERY="SELECT COUNT(*) FROM warehouse.default.sample_table"
   ```

3. **Run Benchmark**:
   ```bash
   # New CLI (recommended)
   python benchmark_cli.py

   # With custom query
   python benchmark_cli.py "SELECT COUNT(*), AVG(price) FROM my_table"

   # With custom query and table
   python benchmark_cli.py "SELECT * FROM {table} LIMIT 10" "warehouse.sales.transactions"
   
   # Legacy CLI (still supported)
   python benchmark.py "SELECT COUNT(*) FROM my_table"
   ```

## Architecture

### 🏗️ Modular Design (v2.0)

```
src/
├── models/              # Data structures & models
│   └── benchmark.py     # EngineResult, BenchmarkConfig, PerformanceReport  
├── engines/             # Engine implementations
│   ├── base.py          # BaseEngine abstract interface
│   ├── factory.py       # EngineFactory for creating engines
│   ├── duckdb_engine.py # DuckDB implementation
│   ├── arrow_engine.py  # Arrow implementation
│   ├── polar_engine.py  # Polars implementation
│   ├── daft_engine.py   # Daft implementation
│   └── starrocks_engine.py # StarRocks implementation
├── utils/               # Utilities & helpers  
│   ├── config.py        # Configuration management
│   └── metrics.py       # Performance measurement
└── benchmark_runner.py  # Main orchestrator

benchmark_cli.py         # New CLI interface
benchmark.py            # Legacy CLI (maintained for compatibility)
```

### 🔄 Multiprocessing Design
The benchmark uses Python's multiprocessing module to run each engine in a separate process:

- **Process Isolation**: Each engine runs independently via `BaseEngine` interface
- **Parallel Execution**: Engines run simultaneously using `multiprocessing.Pool`
- **Fault Tolerance**: If one engine fails, others continue running
- **Memory Accuracy**: True memory usage measurement without interference
- **Timeout Protection**: Configurable timeout per engine to prevent hangs
- **Cross-Platform**: Uses 'spawn' method for Windows/macOS compatibility

### 🚀 Execution Flow
1. `BenchmarkOrchestrator` loads configuration from multiple sources
2. `EngineFactory` creates engine instances from configuration
3. Worker processes execute each engine's benchmark (configurable iterations)
4. Results are collected and aggregated into `PerformanceReport`
5. Summary statistics and detailed results are generated

## Configuration

### Environment Variables
- `HIVE_METASTORE_URI`: Connection to Hive Metastore (required)
- `TABLE_NAME`: Default table name
- `QUERY`: Default query to execute
- `STARROCKS_HOST`, `STARROCKS_PORT`, etc.: StarRocks connection settings

### Example Configuration
See `benchmark.env` for all available environment variables.

## Output

The script produces:
- **Real-time progress**: Shows iteration progress for each engine
- **Process isolation status**: Indicates which engines are running in parallel
- **Performance metrics**: Execution time, memory usage, success rates per engine
- **Aggregated statistics**: Average performance across all iterations
- **Detailed JSON results**: Complete data for further analysis
- **Saved results**: Results automatically saved to `benchmark_results.json`

## Advanced Usage

### 🔧 Programmatic API (New v2.0)

```python
from src.benchmark_runner import BenchmarkOrchestrator
from src.utils.config import ConfigLoader
from src.models.benchmark import BenchmarkConfig

# Method 1: Load from environment
config = ConfigLoader.from_env()
orchestrator = BenchmarkOrchestrator(config)
report = orchestrator.run_benchmark()

# Method 2: Load from YAML
config = ConfigLoader.from_yaml("benchmark.yaml")
orchestrator = BenchmarkOrchestrator(config)  
report = orchestrator.run_benchmark()

# Method 3: Create config programmatically
config = BenchmarkConfig(
    hive_metastore_uri="thrift://localhost:9083",
    table_name="warehouse.sales.data",
    query="SELECT COUNT(*) FROM warehouse.sales.data",
    engines=["DuckDB", "Arrow"],
    iterations=5,
    use_multiprocessing=True
)
orchestrator = BenchmarkOrchestrator(config)
report = orchestrator.run_benchmark()

# Access results
print(f"Fastest engine: {report.summary.fastest_engine}")
print(f"Success rate: {report.summary.success_count}/{report.summary.total_engines}")

# Save results
report.save_to_file("my_results.json")
```

### 🔌 Adding New Engines

```python
from src.engines.base import BaseEngine
from src.engines.factory import EngineFactory

class MyCustomEngine(BaseEngine):
    @property
    def engine_name(self) -> str:
        return "MyEngine"
    
    def _execute_query(self) -> tuple[Any, int]:
        # Your engine implementation
        result = self.execute_my_query(self.query)
        return result, len(result)

# Register with factory
EngineFactory.register_engine("MyEngine", MyCustomEngine)

# Now available in config
config = BenchmarkConfig(
    engines=["DuckDB", "MyEngine"],  # Your engine included
    # ... other config
)
```

### 🐛 Debug Mode (Sequential Execution)
For debugging purposes, you can run engines sequentially:

```python
config = BenchmarkConfig(
    # ... your config
    use_multiprocessing=False  # Sequential execution
)
```

### 📊 Memory and Performance Benefits
- **Isolated Memory Measurement**: Each process measures its own memory usage accurately
- **No Memory Leaks Between Engines**: Process isolation prevents cross-contamination
- **Parallel Execution**: Reduces total benchmark runtime
- **Fault Isolation**: One engine failure doesn't crash the entire benchmark
- **Type Safety**: Full type hints for better IDE support and fewer runtime errors

## Example Output

```
🔍 Query Engine Performance Benchmark
Hive Metastore: thrift://localhost:9083
Table: warehouse.default.sample_table

Running 10 iterations per engine in separate processes...
--------------------------------------------------
Running Polar benchmark (10 iterations)...
  Iteration 1/10... ✅ 890.20ms
  Iteration 2/10... ✅ 875.10ms
  ...
📊 Polar Average: 882.50ms, 145.30MB memory, Success rate: 10/10

Running Arrow benchmark (10 iterations)...
  Iteration 1/10... ✅ 1250.50ms
  ...
📊 Arrow Average: 1240.80ms, 128.70MB memory, Success rate: 10/10

Running DuckDB benchmark (10 iterations)...
  Iteration 1/10... ✅ 950.30ms
  ...
📊 DuckDB Average: 945.20ms, 112.40MB memory, Success rate: 10/10

Running StarRocks benchmark (10 iterations)...
  Iteration 1/10... ❌ Failed - Connection refused
  ...
❌ StarRocks: All runs failed

🏆 Fastest (avg): DuckDB (945.20ms)
💾 Most Memory Efficient (avg): DuckDB (112.40MB)
```

## Requirements

- Python 3.11+
- Access to Hive Metastore
- Iceberg tables configured in Hive catalog
- Optional: StarRocks cluster with external Iceberg catalog
