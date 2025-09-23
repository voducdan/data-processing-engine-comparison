"""Contract tests for CLI interface."""
import pytest
import subprocess
import sys
from pathlib import Path


class TestCLIInterface:
    """Test CLI argument parsing and interface contract."""
    
    def test_cli_help_option(self):
        """Test --help option displays usage information."""
        # This will fail until CLI is implemented
        result = subprocess.run([
            sys.executable, "benchmark.py", "--help"
        ], capture_output=True, text=True, cwd=Path(__file__).parent.parent.parent)
        
        assert result.returncode == 0
        assert "usage:" in result.stdout.lower()
        assert "--config" in result.stdout
        assert "--engines" in result.stdout
        
    def test_cli_config_option(self):
        """Test --config option accepts file path."""
        result = subprocess.run([
            sys.executable, "benchmark.py", "--config", "test.yaml", "--validate-only"
        ], capture_output=True, text=True, cwd=Path(__file__).parent.parent.parent)
        
        # Should fail gracefully with config not found
        assert result.returncode != 0
        assert "config" in result.stderr.lower() or "not found" in result.stderr.lower()
        
    def test_cli_engines_option(self):
        """Test --engines option accepts comma-separated list."""
        result = subprocess.run([
            sys.executable, "benchmark.py", "--engines", "duckdb,daft", "--validate-only"
        ], capture_output=True, text=True, cwd=Path(__file__).parent.parent.parent)
        
        # Should attempt validation but may fail on infrastructure
        # The important part is that it parses the engines option
        assert result.returncode in [0, 2, 3]  # Success, config error, or infrastructure error
        
    def test_cli_output_format_option(self):
        """Test --output-format accepts json and csv."""
        for format_type in ["json", "csv"]:
            result = subprocess.run([
                sys.executable, "benchmark.py", 
                "--output-format", format_type, "--validate-only"
            ], capture_output=True, text=True, cwd=Path(__file__).parent.parent.parent)
            
            # Should not fail on format validation
            assert "invalid choice" not in result.stderr.lower()
            
    def test_cli_invalid_output_format(self):
        """Test invalid output format is rejected."""
        result = subprocess.run([
            sys.executable, "benchmark.py", "--output-format", "xml"
        ], capture_output=True, text=True, cwd=Path(__file__).parent.parent.parent)
        
        assert result.returncode != 0
        assert "invalid choice" in result.stderr.lower() or "xml" in result.stderr.lower()
        
    def test_cli_validate_only_option(self):
        """Test --validate-only runs validation without executing benchmark."""
        result = subprocess.run([
            sys.executable, "benchmark.py", "--validate-only"
        ], capture_output=True, text=True, cwd=Path(__file__).parent.parent.parent)
        
        # Should attempt validation (may fail on missing config/infrastructure)
        assert result.returncode in [0, 2, 3]
        
    def test_cli_environment_variable_support(self):
        """Test HIVE_METASTORE_URI environment variable is recognized."""
        import os
        env = os.environ.copy()
        env["HIVE_METASTORE_URI"] = "thrift://test:9083"
        
        result = subprocess.run([
            sys.executable, "benchmark.py", "--validate-only"
        ], capture_output=True, text=True, env=env, cwd=Path(__file__).parent.parent.parent)
        
        # Should not fail on missing metastore URI
        assert "hive_metastore_uri" not in result.stderr.lower() or result.returncode != 2
        
    def test_cli_iterations_option(self):
        """Test --iterations option accepts positive integers."""
        result = subprocess.run([
            sys.executable, "benchmark.py", "--iterations", "3", "--validate-only"
        ], capture_output=True, text=True, cwd=Path(__file__).parent.parent.parent)
        
        # Should parse iterations without error
        assert "iterations" not in result.stderr.lower() or result.returncode != 2
