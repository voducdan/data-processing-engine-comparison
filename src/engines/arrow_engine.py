"""Arrow engine implementation."""

from typing import Any, Tuple
from .base import BaseEngine


class ArrowEngine(BaseEngine):
    """Arrow engine implementation."""
    
    @property
    def engine_name(self) -> str:
        return "Arrow"
    
    def _execute_query(self) -> Tuple[Any, int]:
        """Execute query using Arrow engine."""
        from pyiceberg.catalog import load_catalog
        import pyarrow.compute as pc
        
        catalog = load_catalog("idz_catalog_silver", **{
            "uri": self.hive_metastore_uri
        })
        table = catalog.load_table(self.table_name)
        
        # Filter the data before converting to Arrow
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
        
        pa_table = filtered_table.to_arrow()
        
        # Group by user_sur_id and aggregate the columns
        group_keys = ["user_sur_id"]
        aggregations = {
            "ad_revenue_usd": "sum",
            "ad_impression": "sum", 
            "ad_load": "sum",
            "ad_load_failed": "sum",
            "ad_load_succeeded": "sum",
            "ad_ready": "sum",
            "ad_not_ready": "sum",
            "ad_show_succeeded": "sum", 
            "ad_show_failed": "sum",
            "ad_closed": "sum",
            "ad_clicked": "sum"
        }

        # Perform group by aggregation
        result_table = pa_table.group_by(group_keys).aggregate(
            [(col, func) for col, func in aggregations.items()]
        )
        result = result_table.to_pylist()
        
        return result, len(result) if result else 0
