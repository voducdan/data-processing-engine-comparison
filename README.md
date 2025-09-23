# Query Engine Performance Comparison

A Python benchmark script for comparing the performance of multiple query engines (Polar, Arrow, Daft, DuckDB, and StarRocks) when executing queries against Iceberg tables via Hive Metastore. 

**✨ Features:**
- **Multiprocessing Support**: Each engine runs in a separate process for complete isolation
- **Parallel Execution**: Multiple engines can run simultaneously  
- **Fault Isolation**: Engine crashes don't affect other engines
- **Comprehensive Metrics**: Execution time, memory usage, and success rates
- **Timeout Protection**: 10-minute timeout per engine
- **Flexible Configuration**: Environment variables and command-line options

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
   # Basic usage (multiprocessing mode - default)
   python benchmark.py

   # Custom query
   python benchmark.py "SELECT COUNT(*), AVG(price) FROM my_table"

   # Custom query and table
   python benchmark.py "SELECT * FROM {table} LIMIT 10" "warehouse.sales.transactions"
   ```

## Architecture

### Multiprocessing Design
The benchmark uses Python's multiprocessing module to run each engine in a separate process:

- **Process Isolation**: Each engine (Polar, Arrow, Daft, DuckDB, StarRocks) runs independently
- **Parallel Execution**: Engines run simultaneously using `multiprocessing.Pool`
- **Fault Tolerance**: If one engine fails, others continue running
- **Memory Accuracy**: True memory usage measurement without interference
- **Timeout Protection**: 10-minute timeout per engine to prevent hangs
- **Cross-Platform**: Uses 'spawn' method for Windows/macOS compatibility

### Execution Flow
1. Main process creates a pool of worker processes
2. Each worker process runs one engine's benchmark (10 iterations)
3. Results are collected and aggregated from all processes
4. Summary statistics and detailed results are generated

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

### Debug Mode (Sequential Execution)
For debugging purposes, you can run engines sequentially instead of in parallel:

```python
from benchmark import PerformanceBenchmark

benchmark = PerformanceBenchmark(metastore_uri, table_name, query)
results = benchmark.run_benchmark(use_multiprocessing=False)
```

### Memory and Performance Benefits
- **Isolated Memory Measurement**: Each process measures its own memory usage accurately
- **No Memory Leaks Between Engines**: Process isolation prevents cross-contamination
- **Parallel Execution**: Reduces total benchmark runtime
- **Fault Isolation**: One engine failure doesn't crash the entire benchmark

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
