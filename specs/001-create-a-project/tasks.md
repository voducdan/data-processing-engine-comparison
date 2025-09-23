# Tasks: Query Engine Performance Comparison Project

**Input**: Design documents from `/specs/001-create-a-project/`
**Prerequisites**: plan.md (required), research.md, data-model.md, contracts/

## Execution Flow (main)
```
1. Load plan.md from feature directory
   → Extracted: Python 3.11+, PyIceberg, DuckDB, Daft, StarRocks, single project structure
2. Load optional design documents:
   → data-model.md: Entities (BenchmarkConfig, BenchmarkRun, EngineResult, PerformanceReport)
   → contracts/: CLI interface and engine interface contracts
   → research.md: Technology decisions and implementation patterns
3. Generate tasks by category:
   → Setup: UV project init, dependencies, directory structure
   → Tests: contract tests, integration tests (TDD approach)
   → Core: data models, engine implementations, measurement utilities
   → Integration: CLI interface, benchmark orchestration
   → Polish: validation, documentation, performance optimization
4. Apply task rules:
   → Different files = mark [P] for parallel
   → Same file = sequential (no [P])
   → Tests before implementation (TDD)
5. Number tasks sequentially (T001, T002...)
6. Generate dependency graph
7. SUCCESS: 24 tasks generated for query engine benchmark system
```

## Format: `[ID] [P?] Description`
- **[P]**: Can run in parallel (different files, no dependencies)
- Include exact file paths in descriptions

## Path Conventions
- **Single project**: `src/`, `tests/` at repository root per plan.md
- All paths relative to repository root `/Users/dan.vo/Documents/compare-query-engines/`

## Phase 3.1: Setup
- [ ] T001 Create project structure with src/, tests/ directories and pyproject.toml for UV dependency management
- [ ] T002 Initialize Python project dependencies: PyIceberg, DuckDB, Daft, StarRocks connectors, pyarrow, psutil, pytest
- [ ] T003 [P] Configure linting (ruff) and formatting tools per coding guidelines

## Phase 3.2: Tests First (TDD) ⚠️ MUST COMPLETE BEFORE 3.3
**CRITICAL: These tests MUST be written and MUST FAIL before ANY implementation**
- [ ] T004 [P] Contract test for CLI interface in tests/contract/test_cli_interface.py
- [ ] T005 [P] Contract test for BaseEngine interface in tests/contract/test_engine_interface.py
- [ ] T006 [P] Contract test for DuckDB engine implementation in tests/contract/test_duckdb_engine.py
- [ ] T007 [P] Contract test for Daft engine implementation in tests/contract/test_daft_engine.py
- [ ] T008 [P] Contract test for StarRocks engine implementation in tests/contract/test_starrocks_engine.py
- [ ] T009 [P] Integration test for benchmark execution flow in tests/integration/test_benchmark_flow.py
- [ ] T010 [P] Integration test for quickstart scenario validation in tests/integration/test_quickstart.py

## Phase 3.3: Core Implementation (ONLY after tests are failing)
- [ ] T011 [P] BenchmarkConfig model in src/models/config.py
- [ ] T012 [P] BenchmarkRun model in src/models/benchmark.py
- [ ] T013 [P] EngineResult model in src/models/result.py
- [ ] T014 [P] PerformanceReport model in src/models/report.py
- [ ] T015 [P] Performance measurement utilities in src/utils/metrics.py
- [ ] T016 [P] Configuration management utilities in src/utils/config.py
- [ ] T017 BaseEngine abstract interface in src/engines/base.py
- [ ] T018 [P] DuckDB engine implementation in src/engines/duckdb_engine.py
- [ ] T019 [P] Daft engine implementation in src/engines/daft_engine.py
- [ ] T020 [P] StarRocks engine implementation in src/engines/starrocks_engine.py

## Phase 3.4: Integration
- [ ] T021 CLI argument parsing and command interface in src/cli.py
- [ ] T022 Benchmark orchestration service in src/benchmark.py
- [ ] T023 Main benchmark script entry point in benchmark.py (repository root)

## Phase 3.5: Polish
- [ ] T024 [P] Update quickstart guide validation and documentation

## Dependencies
- **Setup Phase**: T001 → T002 → T003
- **Tests Phase**: T004-T010 must complete before any T011+ tasks
- **Models Phase**: T011-T014 are parallel (different files)
- **Utilities Phase**: T015-T016 are parallel (different files)
- **Engines Phase**: T017 blocks T018-T020, but T018-T020 are parallel
- **Integration Phase**: T021-T023 depend on all previous core tasks
- **Polish Phase**: T024 depends on all implementation tasks

## Parallel Execution Examples

### Phase 3.2 - All Contract Tests (Run Together)
```bash
# Launch T004-T010 together:
Task: "Contract test for CLI interface in tests/contract/test_cli_interface.py"
Task: "Contract test for BaseEngine interface in tests/contract/test_engine_interface.py"
Task: "Contract test for DuckDB engine in tests/contract/test_duckdb_engine.py"
Task: "Contract test for Daft engine in tests/contract/test_daft_engine.py"
Task: "Contract test for StarRocks engine in tests/contract/test_starrocks_engine.py"
Task: "Integration test for benchmark flow in tests/integration/test_benchmark_flow.py"
Task: "Integration test for quickstart validation in tests/integration/test_quickstart.py"
```

### Phase 3.3 - Data Models (Run Together)
```bash
# Launch T011-T014 together:
Task: "BenchmarkConfig model in src/models/config.py"
Task: "BenchmarkRun model in src/models/benchmark.py"
Task: "EngineResult model in src/models/result.py"
Task: "PerformanceReport model in src/models/report.py"
```

### Phase 3.3 - Engine Implementations (Run Together, after T017)
```bash
# Launch T018-T020 together (after T017 complete):
Task: "DuckDB engine implementation in src/engines/duckdb_engine.py"
Task: "Daft engine implementation in src/engines/daft_engine.py"
Task: "StarRocks engine implementation in src/engines/starrocks_engine.py"
```

## Detailed Task Specifications

### T001: Project Structure Setup
- Create `src/` directory with subdirectories: `models/`, `engines/`, `utils/`
- Create `tests/` directory with subdirectories: `contract/`, `integration/`, `unit/`
- Create `pyproject.toml` with UV configuration and project metadata
- Create basic `__init__.py` files in all Python packages

### T002: Dependency Installation
- Add core dependencies to pyproject.toml: `pyiceberg[hive]`, `duckdb`, `getdaft`, `pymysql`, `pyarrow`, `psutil`
- Add development dependencies: `pytest`, `ruff`, `mypy`
- Configure UV virtual environment and install dependencies
- Verify all engines can be imported successfully

### T004: CLI Interface Contract Test
- Test command-line argument parsing for all options
- Test YAML configuration file loading and validation
- Test environment variable precedence
- Test output format validation (JSON/CSV)
- Verify exit codes for different scenarios

### T005: BaseEngine Interface Contract Test
- Test abstract interface enforcement (cannot instantiate BaseEngine)
- Test all required methods are abstract
- Test QueryResult dataclass validation
- Test engine lifecycle (connect → execute → disconnect)

### T011: BenchmarkConfig Model
- Implement dataclass with all required fields
- Add validation for URI format, non-empty strings
- Add default values for engines list and iterations
- Implement parameter substitution for query templates
- Add JSON/YAML serialization support

### T017: BaseEngine Abstract Interface
- Define abstract base class with all required methods
- Implement QueryResult dataclass with validation
- Add type hints for all method signatures
- Define exception hierarchy for engine errors
- Add context manager support for connection lifecycle

### T021: CLI Argument Parsing
- Use argparse to implement all CLI options from contract
- Add mutually exclusive groups for conflicting options
- Implement configuration file merging with CLI overrides
- Add input validation and user-friendly error messages
- Support both short and long argument forms

### T022: Benchmark Orchestration
- Implement main benchmark execution logic
- Handle engine failures gracefully (partial results)
- Coordinate performance measurement across engines
- Generate performance reports and summaries
- Implement result aggregation and comparison logic

## Validation Checklist
*GATE: Checked by main() before returning*

- [x] All contracts have corresponding tests (T004-T008)
- [x] All entities have model tasks (T011-T014)
- [x] All tests come before implementation (T004-T010 before T011+)
- [x] Parallel tasks truly independent (different files)
- [x] Each task specifies exact file path
- [x] No task modifies same file as another [P] task
- [x] Engine implementations follow TDD approach
- [x] Integration tests cover quickstart scenarios
- [x] CLI contract fully tested before implementation

## Notes
- **TDD Approach**: All contract and integration tests must fail before implementing corresponding functionality
- **Engine Independence**: DuckDB, Daft, and StarRocks implementations are completely parallel
- **Error Handling**: Each engine handles failures independently without affecting others
- **Performance Focus**: Accurate measurement is critical - tests should verify measurement precision
- **Configuration Flexibility**: Support both file-based and CLI-based configuration with proper precedence
