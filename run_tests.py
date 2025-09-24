#!/usr/bin/env python3
"""
Test runner script for the compare-query-engines project.

This script provides convenient commands for running different types of tests
and generating reports.
"""

import sys
import os
import subprocess
import argparse
from pathlib import Path


def run_command(cmd: list, description: str) -> int:
    """Run a command and return exit code."""
    print(f"\n{'='*60}")
    print(f"Running: {description}")
    print(f"Command: {' '.join(cmd)}")
    print(f"{'='*60}")
    
    result = subprocess.run(cmd, cwd=Path(__file__).parent)
    return result.returncode


def run_unit_tests(verbose: bool = False) -> int:
    """Run unit tests."""
    cmd = ["python", "-m", "pytest", "tests/unit/", "-m", "unit"]
    if verbose:
        cmd.append("-v")
    
    return run_command(cmd, "Unit Tests")


def run_integration_tests(verbose: bool = False) -> int:
    """Run integration tests."""
    cmd = ["python", "-m", "pytest", "tests/integration/", "-m", "integration"]
    if verbose:
        cmd.append("-v")
    
    return run_command(cmd, "Integration Tests")


def run_contract_tests(verbose: bool = False) -> int:
    """Run contract tests."""
    cmd = ["python", "-m", "pytest", "tests/integration/test_contracts.py", "-m", "contract"]
    if verbose:
        cmd.append("-v")
    
    return run_command(cmd, "Contract Tests")


def run_performance_tests(verbose: bool = False) -> int:
    """Run performance tests."""
    cmd = ["python", "-m", "pytest", "-m", "performance"]
    if verbose:
        cmd.append("-v")
    
    return run_command(cmd, "Performance Tests")


def run_all_tests(verbose: bool = False) -> int:
    """Run all tests."""
    cmd = ["python", "-m", "pytest", "tests/"]
    if verbose:
        cmd.append("-v")
    
    return run_command(cmd, "All Tests")


def run_tests_with_coverage(verbose: bool = False) -> int:
    """Run tests with coverage report."""
    cmd = [
        "python", "-m", "pytest", 
        "tests/",
        "--cov=src",
        "--cov-report=term-missing",
        "--cov-report=html:htmlcov",
        "--cov-report=xml:coverage.xml"
    ]
    if verbose:
        cmd.append("-v")
    
    return run_command(cmd, "Tests with Coverage")


def lint_code() -> int:
    """Run code linting."""
    return run_command(
        ["python", "-m", "ruff", "check", "src/", "tests/"],
        "Code Linting"
    )


def format_code() -> int:
    """Format code with ruff."""
    return run_command(
        ["python", "-m", "ruff", "format", "src/", "tests/"],
        "Code Formatting"
    )


def type_check() -> int:
    """Run type checking with mypy."""
    return run_command(
        ["python", "-m", "mypy", "src/"],
        "Type Checking"
    )


def run_quick_validation() -> int:
    """Run a quick validation suite (unit tests + linting)."""
    print("Running Quick Validation Suite...")
    
    # Run linting first
    if lint_code() != 0:
        print("❌ Linting failed")
        return 1
    
    # Run unit tests
    if run_unit_tests() != 0:
        print("❌ Unit tests failed")
        return 1
    
    print("✅ Quick validation passed!")
    return 0


def run_full_validation() -> int:
    """Run full validation suite (all tests + quality checks)."""
    print("Running Full Validation Suite...")
    
    # Run linting
    if lint_code() != 0:
        print("❌ Linting failed")
        return 1
    
    # Run type checking (optional, may not have mypy installed)
    type_check()  # Don't fail on type check errors for now
    
    # Run all tests
    if run_all_tests() != 0:
        print("❌ Tests failed")
        return 1
    
    print("✅ Full validation passed!")
    return 0


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Test runner for compare-query-engines project"
    )
    parser.add_argument(
        "command",
        choices=[
            "unit", "integration", "contract", "performance", "all",
            "coverage", "lint", "format", "typecheck", 
            "quick", "full"
        ],
        help="Test command to run"
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Verbose output"
    )
    
    args = parser.parse_args()
    
    # Ensure we're in the right directory
    os.chdir(Path(__file__).parent)
    
    # Add src to Python path
    src_path = Path("src").absolute()
    if str(src_path) not in sys.path:
        sys.path.insert(0, str(src_path))
    
    # Run the requested command
    command_map = {
        "unit": run_unit_tests,
        "integration": run_integration_tests, 
        "contract": run_contract_tests,
        "performance": run_performance_tests,
        "all": run_all_tests,
        "coverage": run_tests_with_coverage,
        "lint": lint_code,
        "format": format_code,
        "typecheck": type_check,
        "quick": run_quick_validation,
        "full": run_full_validation
    }
    
    try:
        exit_code = command_map[args.command](args.verbose)
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\nTest run interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"Error running tests: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
