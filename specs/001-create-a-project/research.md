# Research: Query Engine Performance Comparison

## Overview
Research findings for implementing a Python-based benchmark system comparing DuckDB, Daft, and StarRocks performance against Iceberg tables.

## Technology Stack Decisions

### Dependency Management
**Decision**: UV for Python dependency management  
**Rationale**: Fast, modern Python package installer and resolver. Provides deterministic builds and faster installation than pip.  
**Alternatives considered**: pip + requirements.txt, Poetry, Pipenv

### Engine Integration Approaches

#### DuckDB
**Decision**: Use DuckDB Python API with Iceberg extension  
**Rationale**: Native Python integration, supports Iceberg tables directly, excellent performance measurement capabilities  
**Integration**: `duckdb.connect()` with Iceberg scan functions  
**Dependencies**: `duckdb`, `duckdb-iceberg` (if available) or manual Iceberg configuration

#### Daft
**Decision**: Use Daft DataFrame API with Iceberg reader  
**Rationale**: Python-native distributed dataframe library with native Iceberg support  
**Integration**: `daft.read_iceberg()` for direct table access  
**Dependencies**: `getdaft`, `pyiceberg`

#### StarRocks
**Decision**: Use MySQL/JDBC-compatible Python connector  
**Rationale**: StarRocks exposes MySQL-compatible interface, can connect via external catalogs  
**Integration**: PyMySQL or SQLAlchemy with StarRocks-specific connection parameters  
**Dependencies**: `pymysql` or `sqlalchemy`, StarRocks external catalog configuration

### Iceberg Integration
**Decision**: Use PyIceberg for catalog interactions and metadata  
**Rationale**: Official Python implementation, supports Hive Metastore catalog, provides table metadata for validation  
**Configuration**: Hive Metastore URI configuration via environment variables or config files  
**Dependencies**: `pyiceberg[hive]`

### Performance Measurement
**Decision**: Combine time measurement with psutil for memory tracking  
**Rationale**: `time.perf_counter()` for high-precision timing, `psutil` for process memory monitoring  
**Approach**: 
- Pre-execution memory baseline
- Query execution timing
- Post-execution memory measurement
- Process-level resource monitoring

### Output Format
**Decision**: JSON output with optional CSV export  
**Rationale**: Structured data for programmatic analysis, human-readable format option  
**Schema**: Engine name, query ID, execution time (ms), memory usage (MB), success status

## Implementation Patterns

### Engine Abstraction
**Pattern**: Abstract base class with engine-specific implementations  
**Benefits**: Consistent interface, easy to add new engines, isolated error handling  
**Structure**:
```
BaseEngine (ABC)
├── DuckDBEngine
├── DaftEngine  
└── StarRocksEngine
```

### Configuration Management
**Pattern**: Environment variables with YAML config file fallback  
**Benefits**: Flexible deployment, version control friendly, environment-specific settings  
**Required configs**: Hive Metastore URI, table names, connection parameters

### Error Handling Strategy
**Pattern**: Per-engine error isolation with graceful degradation  
**Benefits**: One engine failure doesn't stop entire benchmark, detailed error reporting  
**Implementation**: Try-catch per engine with detailed error logging

## Query Strategy
**Decision**: Single standardized query with configurable parameters  
**Rationale**: Fair comparison requires identical operations, configurable for different datasets  
**Example**: `SELECT COUNT(*), AVG(column), MAX(column) FROM table WHERE date_partition = ?`

## Memory Measurement Approach
**Decision**: Process-level memory monitoring with garbage collection control  
**Rationale**: Accurate measurement requires controlling Python GC and measuring actual process memory  
**Implementation**:
1. Force garbage collection before measurement
2. Record baseline memory
3. Execute query
4. Record peak memory during execution
5. Calculate delta

## Validation Requirements
**Decision**: Pre-flight checks for infrastructure connectivity  
**Rationale**: Fail fast if infrastructure is unavailable, validate setup before benchmarking  
**Checks**:
- Hive Metastore connectivity
- Table existence and accessibility
- Engine-specific connectivity tests

## Next Steps
All technical uncertainties resolved. Ready for Phase 1 design and contract generation.
