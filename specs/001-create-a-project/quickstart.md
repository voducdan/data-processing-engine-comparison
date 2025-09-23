# Quickstart Guide: Query Engine Benchmark

## Prerequisites

### Infrastructure Requirements
- ✅ **Iceberg Infrastructure**: Existing Iceberg tables with Hive Metastore catalog
- ✅ **Python Environment**: Python 3.11+ installed
- ✅ **Network Access**: Connectivity to Hive Metastore service

### System Requirements
- **OS**: Linux, macOS, or Windows with WSL
- **Memory**: Minimum 4GB RAM (8GB+ recommended for larger datasets)
- **Storage**: 100MB free space for dependencies

## Quick Setup

### 1. Install Dependencies
```bash
# Install uv if not already installed
pip install uv

# Clone/navigate to project directory
cd query-engine-benchmark

# Install project dependencies
uv pip install -e .
```

### 2. Configure Environment
```bash
# Set Hive Metastore connection
export HIVE_METASTORE_URI="thrift://your-metastore-host:9083"

# Optional: Set default configuration file
export BENCHMARK_CONFIG="./benchmark.yaml"
```

### 3. Create Configuration File
Create `benchmark.yaml`:
```yaml
hive_metastore_uri: "thrift://localhost:9083"
table_name: "warehouse.default.sample_table"
query_template: "SELECT COUNT(*) as total_rows, AVG(price) as avg_price FROM {table}"
query_parameters: {}
engines:
  - "duckdb"
  - "daft"
  - "starrocks"
output_format: "json"
iterations: 1
```

## Running Your First Benchmark

### Basic Execution
```bash
# Run benchmark with default configuration
python benchmark.py

# Run with custom table
python benchmark.py --table "warehouse.sales.transactions"

# Run specific engines only
python benchmark.py --engines "duckdb,daft"
```

### Expected Output
```json
{
  "benchmark_run": {
    "run_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
    "timestamp": "2025-09-23T10:30:00Z",
    "status": "completed",
    "duration_seconds": 12.34
  },
  "results": [
    {
      "engine_name": "duckdb",
      "execution_time_ms": 1250.5,
      "memory_usage_mb": 128.7,
      "success": true,
      "row_count": 1000000
    },
    {
      "engine_name": "daft", 
      "execution_time_ms": 2100.8,
      "memory_usage_mb": 256.3,
      "success": true,
      "row_count": 1000000
    }
  ],
  "summary": {
    "fastest_engine": "duckdb",
    "most_memory_efficient": "duckdb"
  }
}
```

## Validation Steps

### 1. Infrastructure Connectivity
```bash
# Test connectivity without running benchmark
python benchmark.py --validate-only

# Expected output:
# ✅ Hive Metastore connectivity: OK
# ✅ Table access: warehouse.default.sample_table found
# ✅ Engine availability: duckdb, daft, starrocks
# ✅ Configuration valid
```

### 2. Simple Query Test
```bash
# Run with simple count query
python benchmark.py --query "SELECT COUNT(*) FROM {table}"
```

### 3. Single Engine Test
```bash
# Test one engine at a time
python benchmark.py --engines "duckdb"
python benchmark.py --engines "daft"
python benchmark.py --engines "starrocks"
```

## Common Queries for Testing

### Basic Aggregation
```sql
SELECT COUNT(*) as row_count, 
       AVG(numeric_column) as avg_value,
       MAX(timestamp_column) as latest_date
FROM {table}
```

### Filtered Aggregation
```sql
SELECT region, 
       COUNT(*) as count,
       SUM(amount) as total_amount
FROM {table} 
WHERE date_partition >= '2024-01-01'
GROUP BY region
```

### Join Query (if multiple tables available)
```sql
SELECT t1.category,
       COUNT(*) as transaction_count,
       AVG(t1.amount) as avg_amount
FROM {table} t1
WHERE t1.status = 'completed'
GROUP BY t1.category
```

## Troubleshooting

### Connection Issues
```bash
# Check Hive Metastore connectivity
telnet your-metastore-host 9083

# Verify table exists
python benchmark.py --validate-only --verbose
```

### Engine-Specific Issues

#### DuckDB
- **Issue**: "Extension not found"
- **Solution**: Ensure DuckDB Iceberg extension is available
- **Command**: `python -c "import duckdb; duckdb.install_extension('iceberg')"`

#### Daft
- **Issue**: "Cannot read Iceberg table"
- **Solution**: Verify PyIceberg configuration
- **Check**: Catalog permissions and table accessibility

#### StarRocks
- **Issue**: "External catalog not found"
- **Solution**: Configure StarRocks external catalog for Iceberg
- **Setup**: Create external catalog pointing to your Hive Metastore

### Performance Issues
```bash
# Run with verbose logging
python benchmark.py --verbose

# Reduce dataset size for testing
python benchmark.py --query "SELECT COUNT(*) FROM {table} LIMIT 1000"

# Monitor system resources
htop  # or equivalent system monitor
```

## Advanced Usage

### Multiple Iterations
```bash
# Run 5 iterations for statistical significance
python benchmark.py --iterations 5
```

### Custom Output Location
```bash
# Save results to file
python benchmark.py --output results.json

# CSV format for spreadsheet analysis
python benchmark.py --output-format csv --output results.csv
```

### Configuration File Customization
```yaml
# Advanced configuration example
hive_metastore_uri: "thrift://prod-metastore:9083"
table_name: "warehouse.analytics.large_dataset"
query_template: |
  SELECT 
    date_trunc('month', event_date) as month,
    event_type,
    COUNT(*) as event_count,
    COUNT(DISTINCT user_id) as unique_users
  FROM {table}
  WHERE event_date >= '{start_date}'
    AND event_date < '{end_date}'
  GROUP BY 1, 2
  ORDER BY 1, 2
query_parameters:
  start_date: "2024-01-01"
  end_date: "2024-12-31"
engines:
  - "duckdb"
  - "starrocks"  # Skip Daft for complex queries
output_format: "json"
iterations: 3
```

## Next Steps

### Production Usage
1. **Scale Testing**: Test with larger datasets and complex queries
2. **Automation**: Integrate into CI/CD pipelines
3. **Monitoring**: Set up regular performance monitoring
4. **Optimization**: Use results to optimize query performance

### Extending the Benchmark
1. **Additional Engines**: Add support for Spark, Trino, etc.
2. **Custom Metrics**: Add latency percentiles, throughput measurements
3. **Reporting**: Generate HTML reports with visualizations
4. **Historical Tracking**: Store results for trend analysis

### Getting Help
- **Logs**: Check verbose output for detailed error information
- **Documentation**: Review engine-specific documentation
- **Issues**: Report bugs with full configuration and error messages
