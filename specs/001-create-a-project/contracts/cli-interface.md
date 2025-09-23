# Benchmark CLI Contract

## Command Interface

### Primary Command
```bash
python benchmark.py [OPTIONS]
```

### Options
- `--config PATH`: Path to YAML configuration file (default: `benchmark.yaml`)
- `--table STRING`: Iceberg table name (overrides config)
- `--query STRING`: SQL query template (overrides config)
- `--engines LIST`: Comma-separated engine list (default: "duckdb,daft,starrocks")
- `--output-format {json,csv}`: Output format (default: "json")
- `--iterations INT`: Number of iterations per engine (default: 1)
- `--output PATH`: Output file path (default: stdout)
- `--verbose`: Enable verbose logging
- `--validate-only`: Validate configuration and connectivity without running benchmark

### Environment Variables
- `HIVE_METASTORE_URI`: Hive Metastore connection string
- `BENCHMARK_CONFIG`: Path to configuration file

## Input Contracts

### Configuration File Schema (YAML)
```yaml
hive_metastore_uri: "thrift://localhost:9083"
table_name: "warehouse.default.sample_table"
query_template: "SELECT COUNT(*), AVG(price) FROM {table} WHERE date_partition = '{date}'"
query_parameters:
  date: "2024-01-01"
engines:
  - "duckdb"
  - "daft" 
  - "starrocks"
output_format: "json"
iterations: 1
```

### Query Template Requirements
- Must be valid SQL
- Parameters enclosed in `{parameter_name}` format
- Must work across all specified engines
- Should avoid engine-specific syntax

## Output Contracts

### JSON Output Schema
```json
{
  "benchmark_run": {
    "run_id": "string (UUID)",
    "timestamp": "string (ISO 8601)",
    "config": {
      "table_name": "string",
      "query": "string",
      "engines": ["string"],
      "iterations": "integer"
    },
    "duration_seconds": "number",
    "status": "string (completed|partial|failed)"
  },
  "results": [
    {
      "engine_name": "string",
      "query_id": "string", 
      "execution_time_ms": "number",
      "memory_usage_mb": "number",
      "memory_baseline_mb": "number",
      "success": "boolean",
      "error_message": "string|null",
      "row_count": "integer|null",
      "iteration": "integer"
    }
  ],
  "summary": {
    "fastest_engine": "string",
    "most_memory_efficient": "string",
    "success_rate_by_engine": {
      "engine_name": "number (0.0-1.0)"
    }
  }
}
```

### CSV Output Schema
```csv
run_id,timestamp,engine_name,iteration,execution_time_ms,memory_usage_mb,success,error_message,row_count
```

## Exit Codes
- `0`: Success - All engines completed successfully
- `1`: Partial success - Some engines failed but at least one succeeded
- `2`: Configuration error - Invalid configuration or missing parameters
- `3`: Infrastructure error - Cannot connect to Hive Metastore or table not found
- `4`: Complete failure - All engines failed to execute

## Error Messages

### Configuration Errors
```json
{
  "error": "configuration_error",
  "message": "Missing required configuration: hive_metastore_uri",
  "details": {
    "missing_fields": ["hive_metastore_uri"],
    "provided_config": {...}
  }
}
```

### Infrastructure Errors
```json
{
  "error": "infrastructure_error", 
  "message": "Cannot connect to Hive Metastore",
  "details": {
    "uri": "thrift://localhost:9083",
    "underlying_error": "Connection refused"
  }
}
```

### Engine Execution Errors
```json
{
  "error": "engine_error",
  "message": "DuckDB execution failed",
  "details": {
    "engine": "duckdb",
    "query": "SELECT ...",
    "underlying_error": "Table not found: warehouse.default.sample_table"
  }
}
```

## Validation Requirements

### Pre-execution Validation
1. **Configuration Validation**: All required fields present and valid
2. **Connectivity Validation**: Can connect to Hive Metastore
3. **Table Validation**: Target table exists and is accessible
4. **Query Validation**: Query template is syntactically valid
5. **Engine Validation**: All specified engines are supported

### Runtime Validation
1. **Memory Baseline**: Establish valid memory baseline before execution
2. **Execution Timeout**: Queries must complete within reasonable time (configurable)
3. **Result Validation**: Verify query produced expected result structure

## Performance Requirements

### Execution Time Measurement
- **Precision**: Millisecond precision using `time.perf_counter()`
- **Scope**: Measure only query execution, exclude connection setup
- **Accuracy**: Account for cold start vs warm execution

### Memory Measurement
- **Method**: Process-level RSS memory measurement
- **Baseline**: Measure before query execution
- **Peak**: Capture maximum memory during execution
- **Units**: Megabytes with 2 decimal precision

### Reporting Requirements
- **Timing**: Results available immediately after execution
- **Format**: Structured output suitable for automated analysis
- **Completeness**: Include all measurement metadata for reproducibility
