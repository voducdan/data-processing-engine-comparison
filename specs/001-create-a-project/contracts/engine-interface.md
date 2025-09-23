# Engine Interface Contract

## Abstract Base Engine Contract

### Engine Interface
All engines must implement the following interface:

```python
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from dataclasses import dataclass

@dataclass
class QueryResult:
    execution_time_ms: float
    memory_usage_mb: float
    memory_baseline_mb: float
    success: bool
    error_message: Optional[str] = None
    row_count: Optional[int] = None

class BaseEngine(ABC):
    @abstractmethod
    def connect(self, config: Dict[str, Any]) -> bool:
        """Establish connection to engine and Iceberg catalog"""
        pass
    
    @abstractmethod
    def execute_query(self, query: str) -> QueryResult:
        """Execute query and return performance measurements"""
        pass
    
    @abstractmethod
    def disconnect(self) -> None:
        """Clean up connections and resources"""
        pass
    
    @abstractmethod
    def validate_connectivity(self) -> bool:
        """Test engine connectivity without executing queries"""
        pass
```

## Engine-Specific Contracts

### DuckDB Engine Contract

#### Connection Requirements
- **Method**: In-memory DuckDB instance with Iceberg extension
- **Configuration**: Hive Metastore URI, table name
- **Dependencies**: `duckdb`, `pyiceberg`

#### Connection Parameters
```python
duckdb_config = {
    "hive_metastore_uri": "thrift://localhost:9083",
    "table_name": "warehouse.default.sample_table"
}
```

#### Query Execution
- **SQL Dialect**: DuckDB SQL (mostly ANSI compliant)
- **Iceberg Integration**: Use DuckDB's Iceberg scan functions
- **Memory Measurement**: Process-level monitoring during query execution

#### Error Handling
- **Connection Errors**: Hive Metastore unreachable, authentication failures
- **Query Errors**: Invalid SQL, table not found, permission denied
- **Resource Errors**: Insufficient memory, disk space

### Daft Engine Contract

#### Connection Requirements
- **Method**: Daft session with Iceberg catalog configuration
- **Configuration**: Catalog configuration, table path
- **Dependencies**: `getdaft`, `pyiceberg`

#### Connection Parameters
```python
daft_config = {
    "catalog_type": "hive",
    "hive_metastore_uri": "thrift://localhost:9083",
    "table_name": "warehouse.default.sample_table"
}
```

#### Query Execution
- **Method**: Use Daft DataFrame API with `read_iceberg()`
- **SQL Support**: Convert SQL to Daft DataFrame operations where possible
- **Memory Measurement**: Monitor Daft session memory usage

#### Limitations
- **SQL Compatibility**: May require query translation for complex SQL
- **Aggregations**: Support for common aggregation functions
- **Filtering**: Support for WHERE clause conditions

### StarRocks Engine Contract

#### Connection Requirements
- **Method**: MySQL-compatible connection to StarRocks
- **Configuration**: StarRocks host, port, database, external catalog
- **Dependencies**: `pymysql` or `sqlalchemy`

#### Connection Parameters
```python
starrocks_config = {
    "host": "localhost",
    "port": 9030,
    "user": "root", 
    "password": "",
    "database": "iceberg_catalog",
    "table_name": "warehouse.default.sample_table"
}
```

#### External Catalog Setup
- **Requirement**: StarRocks must have external catalog configured for Iceberg
- **Catalog Type**: Iceberg catalog pointing to Hive Metastore
- **Table Access**: Tables accessible via catalog.database.table syntax

#### Query Execution
- **SQL Dialect**: MySQL-compatible SQL
- **Iceberg Integration**: Via external catalog reference
- **Memory Measurement**: Monitor client connection memory usage

## Performance Measurement Contract

### Timing Requirements
```python
# Timing measurement contract
def measure_execution_time(func):
    start_time = time.perf_counter()
    result = func()
    end_time = time.perf_counter()
    execution_time_ms = (end_time - start_time) * 1000
    return result, execution_time_ms
```

### Memory Measurement Requirements
```python
# Memory measurement contract
def measure_memory_usage():
    import psutil
    process = psutil.Process()
    
    # Force garbage collection for accurate baseline
    import gc
    gc.collect()
    
    # Get memory baseline
    baseline_mb = process.memory_info().rss / 1024 / 1024
    
    # Execute query with monitoring
    peak_memory_mb = baseline_mb
    # ... query execution with periodic memory checks
    
    return peak_memory_mb, baseline_mb
```

### Result Validation Contract
```python
def validate_query_result(result: Any, expected_schema: Optional[Dict] = None) -> bool:
    """Validate query result structure and content"""
    if result is None:
        return False
    
    # Check result is iterable (has rows)
    try:
        row_count = len(list(result))
        return row_count >= 0
    except (TypeError, AttributeError):
        return False
```

## Error Handling Contract

### Error Categories
1. **ConnectionError**: Cannot connect to engine or catalog
2. **QueryError**: Query execution failed
3. **ResourceError**: Insufficient resources (memory, disk)
4. **TimeoutError**: Query execution exceeded timeout
5. **ValidationError**: Invalid configuration or parameters

### Error Response Format
```python
@dataclass
class EngineError:
    error_type: str  # One of the categories above
    message: str     # Human-readable error message
    details: Dict[str, Any]  # Additional error context
    recoverable: bool  # Whether operation can be retried
```

### Error Recovery Strategies
- **ConnectionError**: Retry with exponential backoff
- **QueryError**: Log and continue with other engines
- **ResourceError**: Reduce query complexity or skip engine
- **TimeoutError**: Increase timeout or skip engine
- **ValidationError**: Fail fast, do not attempt execution

## Integration Testing Contract

### Required Test Cases
1. **Successful Query Execution**: All engines execute query successfully
2. **Engine Failure Handling**: One engine fails, others continue
3. **Connection Failure**: Engine cannot connect to infrastructure
4. **Query Timeout**: Long-running query handling
5. **Memory Limit**: Large result set memory management
6. **Invalid Query**: Malformed SQL handling

### Test Data Requirements
- **Sample Table**: Known Iceberg table with predictable data
- **Sample Queries**: Queries with known execution characteristics
- **Error Scenarios**: Configurations that trigger specific error conditions

### Performance Benchmarks
- **Baseline Measurements**: Known performance characteristics for comparison
- **Regression Detection**: Identify performance degradation
- **Resource Monitoring**: Validate memory measurement accuracy
