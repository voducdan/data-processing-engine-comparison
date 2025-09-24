"""Polars engine implementation."""

from typing import Any, Tuple
from .base import BaseEngine


class PolarEngine(BaseEngine):
    """Polars engine implementation."""
    
    @property
    def engine_name(self) -> str:
        return "Polar"
    
    def _execute_query(self) -> Tuple[Any, int]:
        """Execute query using Polars engine."""
        from pyiceberg.catalog import load_catalog
        import polars as pl
        
        catalog = load_catalog("idz_catalog_silver", **{
            "uri": self.hive_metastore_uri
        })
        table = catalog.load_table(self.table_name)
        
        # Filter the data before converting to Polars
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
        
        pl_table = filtered_table.to_polars()
        
        # Perform group by aggregation using Polars
        result_table = pl_table.group_by("user_sur_id").agg([
            pl.col("ad_revenue_usd").sum(),
            pl.col("ad_impression").sum(),
            pl.col("ad_load").sum(),
            pl.col("ad_load_failed").sum(),
            pl.col("ad_load_succeeded").sum(),
            pl.col("ad_ready").sum(),
            pl.col("ad_not_ready").sum(),
            pl.col("ad_show_succeeded").sum(),
            pl.col("ad_show_failed").sum(),
            pl.col("ad_closed").sum(),
            pl.col("ad_clicked").sum()
        ])
        result = result_table.to_dicts()
        
        return result, len(result) if result else 0
