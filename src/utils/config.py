"""Configuration management utilities."""

import os
from typing import Dict, Any, Optional
from pathlib import Path

try:
    import yaml
    HAS_YAML = True
except ImportError:
    HAS_YAML = False

from ..models.benchmark import BenchmarkConfig


class ConfigLoader:
    """Load configuration from various sources."""
    
    @staticmethod
    def from_env() -> BenchmarkConfig:
        """Load configuration from environment variables."""
        return BenchmarkConfig(
            hive_metastore_uri=os.getenv('HIVE_METASTORE_URI', 'thrift://localhost:9083'),
            table_name=os.getenv('TABLE_NAME', 'facts.ad_revenue'),
            query=os.getenv('QUERY', """
                select
                user_sur_id, 
                sum(ad_revenue_usd) ad_revenue_usd, 
                sum(ad_impression) ad_impression,
                sum(ad_load) ad_load,
                sum(ad_load_failed) ad_load_failed,
                sum(ad_load_succeeded) ad_load_succeeded,
                sum(ad_ready) ad_ready,
                sum(ad_not_ready) ad_not_ready,
                sum(ad_show_succeeded) ad_show_succeeded,
                sum(ad_show_failed) ad_show_failed,
                sum(ad_closed) ad_closed,
                sum(ad_clicked) ad_clicked
            from idz_catalog_silver.facts.ad_revenue
            where true
                and etl_date = '2025-08-10'
            group by user_sur_id                
            """),
            iterations=int(os.getenv('ITERATIONS', '10')),
            use_multiprocessing=os.getenv('USE_MULTIPROCESSING', 'true').lower() == 'true',
            timeout_seconds=int(os.getenv('TIMEOUT_SECONDS', '600'))
        )
    
    @staticmethod
    def from_yaml(filepath: str) -> BenchmarkConfig:
        """Load configuration from YAML file."""
        if not HAS_YAML:
            raise ImportError("PyYAML is required for YAML configuration loading")
            
        with open(filepath, 'r') as f:
            data = yaml.safe_load(f)
        
        return BenchmarkConfig(
            hive_metastore_uri=data.get('hive_metastore_uri', 'thrift://localhost:9083'),
            table_name=data.get('table_name', 'warehouse.default.sample_table'),
            query=data.get('query_template', 'SELECT COUNT(*) FROM {table}').format(
                table=data.get('table_name', 'warehouse.default.sample_table'),
                **data.get('query_parameters', {})
            ),
            iterations=data.get('iterations', 10),
            use_multiprocessing=data.get('use_multiprocessing', True),
            engines=data.get('engines', ["Polar", "Arrow", "Daft", "DuckDB", "StarRocks"])
        )
    
    @staticmethod
    def from_args(args: Dict[str, Any]) -> BenchmarkConfig:
        """Load configuration from command line arguments dict."""
        config = ConfigLoader.from_env()  # Start with env defaults
        
        # Override with provided arguments
        if 'query' in args:
            config.query = args['query']
        if 'table_name' in args:
            config.table_name = args['table_name']
        if 'hive_metastore_uri' in args:
            config.hive_metastore_uri = args['hive_metastore_uri']
        if 'iterations' in args:
            config.iterations = args['iterations']
        if 'use_multiprocessing' in args:
            config.use_multiprocessing = args['use_multiprocessing']
        if 'engines' in args:
            config.engines = args['engines']
            
        return config


class StarRocksConfig:
    """StarRocks connection configuration."""
    
    def __init__(self):
        self.host = os.getenv('STARROCKS_HOST', 'localhost')
        self.port = int(os.getenv('STARROCKS_PORT', '9030'))
        self.user = os.getenv('STARROCKS_USER', 'root')
        self.password = os.getenv('STARROCKS_PASSWORD', '')
        self.database = os.getenv('STARROCKS_DATABASE', 'idz_catalog_silver.facts')
    
    def get_connection_params(self) -> Dict[str, Any]:
        """Get connection parameters for StarRocks."""
        return {
            'host': self.host,
            'port': self.port,
            'user': self.user,
            'password': self.password,
            'database': self.database
        }
