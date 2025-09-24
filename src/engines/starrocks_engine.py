"""StarRocks engine implementation."""

from typing import Any, Tuple
from .base import BaseEngine
from ..utils.config import StarRocksConfig


class StarRocksEngine(BaseEngine):
    """StarRocks engine implementation."""
    
    @property
    def engine_name(self) -> str:
        return "StarRocks"
    
    def _execute_query(self) -> Tuple[Any, int]:
        """Execute query using StarRocks engine."""
        import pymysql
        
        config = StarRocksConfig()
        conn_params = config.get_connection_params()
        
        # StarRocks connection (MySQL-compatible)
        conn = pymysql.connect(**conn_params)
        cursor = conn.cursor()
        
        try:
            # Execute query
            cursor.execute("SET GLOBAL enable_scan_datacache=false;")
            cursor.execute(self.query)
            result = cursor.fetchall()
            
            return result, len(result) if result else 0
            
        finally:
            cursor.close()
            conn.close()
