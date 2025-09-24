"""DuckDB engine implementation."""

from typing import Any, Tuple
from .base import BaseEngine


class DuckDBEngine(BaseEngine):
    """DuckDB engine implementation."""
    
    @property
    def engine_name(self) -> str:
        return "DuckDB"
    
    def _execute_query(self) -> Tuple[Any, int]:
        """Execute query using DuckDB engine."""
        from pyiceberg.catalog import load_catalog
        
        catalog = load_catalog("idz_catalog_silver", **{
            "uri": self.hive_metastore_uri
        })
        table = catalog.load_table(self.table_name)
        
        # Filter the data before converting to DuckDB
        filtered_table = table.scan(
            row_filter="etl_date == '2025-08-10'",
            selected_fields=(
                "user_sur_id",
                "ad_revenue_usd",
                "ad_impression",
                "ad_load",
                "ad_load_failed",
                "ad_load_succeeded",
                "ad_ready",
                "ad_not_ready",
                "ad_show_succeeded",
                "ad_show_failed",
                "ad_closed",
                "ad_clicked",
                "etl_date"
            )
        )
        
        conn = filtered_table.to_duckdb(table_name="ad_revenue")
        query = self.query.replace("idz_catalog_silver.facts.ad_revenue", "ad_revenue")
        result = conn.execute(query).fetchall()
        
        conn.close()
        
        return result, len(result) if result else 0
