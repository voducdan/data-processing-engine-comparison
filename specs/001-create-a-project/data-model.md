# Data Model: Query Engine Benchmark System

## Core Entities

### BenchmarkConfig
**Purpose**: Configuration for benchmark execution  
**Fields**:
- `hive_metastore_uri: str` - Connection string for Hive Metastore
- `table_name: str` - Target Iceberg table name
- `query_template: str` - SQL query template with parameters
- `query_parameters: dict` - Parameters to substitute in query template
- `engines: List[str]` - List of engines to benchmark (default: ["duckdb", "daft", "starrocks"])
- `output_format: str` - Output format ("json" or "csv")
- `iterations: int` - Number of benchmark iterations per engine (default: 1)

**Validation Rules**:
- `hive_metastore_uri` must be valid URI format
- `table_name` must be non-empty string
- `query_template` must contain valid SQL
- `engines` must contain at least one supported engine

### BenchmarkRun
**Purpose**: Represents a complete benchmark execution session  
**Fields**:
- `run_id: str` - Unique identifier (UUID)
- `timestamp: datetime` - Execution start time
- `config: BenchmarkConfig` - Configuration used for this run
- `results: List[EngineResult]` - Results from all engines
- `duration_seconds: float` - Total benchmark duration
- `status: str` - Overall status ("completed", "partial", "failed")

**State Transitions**:
- `initializing` → `running` → `completed`/`partial`/`failed`

### EngineResult
**Purpose**: Performance metrics for a single engine execution  
**Fields**:
- `engine_name: str` - Engine identifier ("duckdb", "daft", "starrocks")
- `query_id: str` - Query identifier for this execution
- `execution_time_ms: float` - Query execution time in milliseconds
- `memory_usage_mb: float` - Peak memory usage in megabytes
- `memory_baseline_mb: float` - Memory usage before query execution
- `success: bool` - Whether query executed successfully
- `error_message: Optional[str]` - Error details if execution failed
- `row_count: Optional[int]` - Number of rows returned (if applicable)
- `iteration: int` - Iteration number (for multiple runs)

**Validation Rules**:
- `engine_name` must be in supported engines list
- `execution_time_ms` must be non-negative
- `memory_usage_mb` must be non-negative
- If `success` is False, `error_message` must be provided

### QueryMetadata
**Purpose**: Information about the query being executed  
**Fields**:
- `query_id: str` - Unique query identifier
- `sql_statement: str` - Actual SQL executed (after parameter substitution)
- `complexity_score: Optional[int]` - Query complexity rating (1-10)
- `expected_row_count: Optional[int]` - Expected result size for validation

### PerformanceReport
**Purpose**: Aggregated comparison results across engines  
**Fields**:
- `benchmark_run_id: str` - Reference to source benchmark run
- `generated_at: datetime` - Report generation timestamp
- `summary: Dict[str, Any]` - High-level comparison metrics
- `engine_rankings: List[Dict]` - Engines ranked by performance
- `detailed_metrics: Dict[str, EngineResult]` - Per-engine detailed results

**Computed Fields**:
- `fastest_engine: str` - Engine with lowest execution time
- `most_memory_efficient: str` - Engine with lowest memory usage
- `success_rate_by_engine: Dict[str, float]` - Success percentage per engine

## Relationships

```
BenchmarkRun (1) ──→ (1) BenchmarkConfig
BenchmarkRun (1) ──→ (n) EngineResult
BenchmarkRun (1) ──→ (1) QueryMetadata
BenchmarkRun (1) ──→ (0..1) PerformanceReport
```

## Data Flow

1. **Configuration Loading**: `BenchmarkConfig` loaded from environment/file
2. **Run Initialization**: `BenchmarkRun` created with unique ID and timestamp
3. **Query Preparation**: `QueryMetadata` generated from config template
4. **Engine Execution**: For each engine, create `EngineResult` with measurements
5. **Report Generation**: `PerformanceReport` aggregates results for comparison

## Validation Schema

### BenchmarkConfig Schema
```json
{
  "type": "object",
  "required": ["hive_metastore_uri", "table_name", "query_template"],
  "properties": {
    "hive_metastore_uri": {"type": "string", "format": "uri"},
    "table_name": {"type": "string", "minLength": 1},
    "query_template": {"type": "string", "minLength": 1},
    "query_parameters": {"type": "object"},
    "engines": {"type": "array", "items": {"type": "string"}, "minItems": 1},
    "output_format": {"type": "string", "enum": ["json", "csv"]},
    "iterations": {"type": "integer", "minimum": 1}
  }
}
```

### EngineResult Schema
```json
{
  "type": "object",
  "required": ["engine_name", "query_id", "execution_time_ms", "memory_usage_mb", "success"],
  "properties": {
    "engine_name": {"type": "string"},
    "query_id": {"type": "string"},
    "execution_time_ms": {"type": "number", "minimum": 0},
    "memory_usage_mb": {"type": "number", "minimum": 0},
    "memory_baseline_mb": {"type": "number", "minimum": 0},
    "success": {"type": "boolean"},
    "error_message": {"type": "string"},
    "row_count": {"type": "integer", "minimum": 0},
    "iteration": {"type": "integer", "minimum": 1}
  }
}
```

## Error Handling

### Engine Connection Failures
- **Scenario**: Engine cannot connect to Iceberg/Hive Metastore
- **Response**: Mark engine result as failed, continue with other engines
- **Data**: Store connection error in `error_message`

### Query Execution Failures
- **Scenario**: Query fails on specific engine (syntax, permissions, etc.)
- **Response**: Record failure details, continue benchmark
- **Data**: `success: false`, detailed error message

### Infrastructure Failures
- **Scenario**: Hive Metastore unavailable, table not found
- **Response**: Fail entire benchmark run early
- **Data**: Update `BenchmarkRun.status` to "failed"

## Performance Considerations

### Memory Measurement Accuracy
- Force garbage collection before baseline measurement
- Use process-level memory monitoring (RSS)
- Account for Python interpreter overhead

### Timing Precision
- Use `time.perf_counter()` for high-resolution timing
- Measure only query execution, exclude setup/teardown
- Consider query plan caching effects

### Data Isolation
- Each engine operates independently
- No shared connections or sessions
- Clean state between measurements
