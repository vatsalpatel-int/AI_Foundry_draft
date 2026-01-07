"""
Delta Lake Writer Module

Handles writing cost data to Delta Lake with:
- Proper schema definition
- Date-based partitioning
- Idempotent MERGE operations (no duplicates)
- Metadata enrichment
"""

import os
import logging
from typing import List
from datetime import datetime

from data_extractor import CostReportData

logger = logging.getLogger(__name__)


# Define the schema for Azure Cost Management data
# This ensures consistent data types across all loads
COST_DATA_SCHEMA = """
    BillingAccountId STRING,
    BillingAccountName STRING,
    BillingPeriodStartDate DATE,
    BillingPeriodEndDate DATE,
    BillingProfileId STRING,
    BillingProfileName STRING,
    InvoiceSectionId STRING,
    InvoiceSectionName STRING,
    PartNumber STRING,
    ProductName STRING,
    MeterCategory STRING,
    MeterSubCategory STRING,
    MeterName STRING,
    MeterId STRING,
    MeterRegion STRING,
    ResourceLocation STRING,
    ResourceGroup STRING,
    ResourceId STRING,
    ResourceName STRING,
    ServiceName STRING,
    ServiceTier STRING,
    SubscriptionId STRING,
    SubscriptionName STRING,
    CostCenter STRING,
    UnitOfMeasure STRING,
    Quantity DOUBLE,
    EffectivePrice DOUBLE,
    CostInBillingCurrency DOUBLE,
    BillingCurrencyCode STRING,
    PricingModel STRING,
    ChargeType STRING,
    Frequency STRING,
    PublisherType STRING,
    PublisherName STRING,
    ReservationId STRING,
    ReservationName STRING,
    Tags STRING,
    Date DATE,
    CostAllocationRuleName STRING,
    benefitId STRING,
    benefitName STRING
"""


class DeltaLakeWriter:
    """
    Writes cost data to Delta Lake with idempotent MERGE operations.
    
    Features:
    - Schema enforcement
    - Date-based partitioning for efficient queries
    - MERGE (upsert) to prevent duplicate records
    - Metadata columns for data lineage
    """
    
    def __init__(self, delta_table_path: str):
        """
        Initialize the Delta writer.
        
        Args:
            delta_table_path: Path to Delta table (S3, ADLS, or DBFS)
        """
        self._table_path = delta_table_path
        self._temp_dir = "/tmp/cost_data"
    
    def write_cost_data(
        self,
        cost_reports: List[CostReportData],
        use_merge: bool = True
    ) -> int:
        """
        Write cost report data to Delta Lake.
        
        Args:
            cost_reports: List of cost report data objects
            use_merge: If True, use MERGE for idempotency. If False, use APPEND.
            
        Returns:
            int: Total number of rows written
        """
        # Import spark here (available in Databricks Runtime)
        from pyspark.sql import functions as F
        from delta.tables import DeltaTable
        
        total_rows = 0
        
        for report in cost_reports:
            logger.info(f"Processing data for scope: {report.scope_name}")
            
            # Step 1: Load CSV into DataFrame
            df = self._load_csv_to_dataframe(report)
            
            if df is None or df.count() == 0:
                logger.warning(f"No data found for scope {report.scope_name}")
                continue
            
            # Step 2: Add metadata columns
            df = self._add_metadata_columns(df, report)
            
            # Step 3: Write to Delta
            row_count = df.count()
            
            if use_merge:
                self._merge_to_delta(df)
            else:
                self._append_to_delta(df)
            
            total_rows += row_count
            logger.info(f"Wrote {row_count} rows for {report.scope_name}")
        
        logger.info(f"Total rows written: {total_rows}")
        return total_rows
    
    def _load_csv_to_dataframe(self, report: CostReportData):
        """
        Load CSV content into a Spark DataFrame.
        
        Args:
            report: Cost report data with CSV content
            
        Returns:
            DataFrame: Spark DataFrame with cost data
        """
        # Ensure temp directory exists
        os.makedirs(self._temp_dir, exist_ok=True)
        
        # Write CSV to temp file
        temp_file = f"{self._temp_dir}/temp_{report.scope_name}_{report.target_date}.csv"
        
        with open(temp_file, "wb") as f:
            f.write(report.csv_content)
        
        logger.info(f"Loading CSV from {temp_file}")
        
        # Read with Spark
        # Note: 'spark' is available in Databricks Runtime
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("multiLine", "true") \
            .option("escape", '"') \
            .csv(f"file:{temp_file}")
        
        # Cleanup temp file
        if os.path.exists(temp_file):
            os.remove(temp_file)
        
        return df
    
    def _add_metadata_columns(self, df, report: CostReportData):
        """
        Add metadata columns for data lineage and partitioning.
        
        Args:
            df: Spark DataFrame
            report: Cost report data
            
        Returns:
            DataFrame: DataFrame with additional metadata columns
        """
        from pyspark.sql import functions as F
        
        df = df \
            .withColumn("_source_scope", F.lit(report.scope_id)) \
            .withColumn("_source_scope_name", F.lit(report.scope_name)) \
            .withColumn("_ingestion_timestamp", F.current_timestamp()) \
            .withColumn("_ingestion_date", F.to_date(F.lit(report.target_date))) \
            .withColumn("_cost_date", F.to_date(F.lit(report.target_date)))
        
        return df
    
    def _merge_to_delta(self, df) -> None:
        """
        Merge data into Delta table (upsert - no duplicates).
        
        Uses composite key: Date + ResourceId + MeterId + SubscriptionId
        
        Args:
            df: Spark DataFrame to merge
        """
        from delta.tables import DeltaTable
        from pyspark.sql import functions as F
        
        # Check if table exists
        if DeltaTable.isDeltaTable(spark, self._table_path):
            logger.info("Performing MERGE into existing Delta table...")
            
            delta_table = DeltaTable.forPath(spark, self._table_path)
            
            # Define merge condition (composite key for uniqueness)
            merge_condition = """
                target._cost_date = source._cost_date
                AND target.ResourceId = source.ResourceId
                AND target.MeterId = source.MeterId
                AND target.SubscriptionId = source.SubscriptionId
                AND target._source_scope = source._source_scope
            """
            
            delta_table.alias("target").merge(
                df.alias("source"),
                merge_condition
            ).whenMatchedUpdateAll() \
             .whenNotMatchedInsertAll() \
             .execute()
            
            logger.info("MERGE completed successfully")
        else:
            # First write - create the table with partitioning
            logger.info("Creating new Delta table with partitioning...")
            self._create_partitioned_table(df)
    
    def _append_to_delta(self, df) -> None:
        """
        Append data to Delta table (may create duplicates if run twice).
        
        Args:
            df: Spark DataFrame to append
        """
        # Check if table exists for partitioning decision
        from delta.tables import DeltaTable
        
        if DeltaTable.isDeltaTable(spark, self._table_path):
            logger.info("Appending to existing Delta table...")
            df.write \
                .format("delta") \
                .mode("append") \
                .save(self._table_path)
        else:
            logger.info("Creating new Delta table...")
            self._create_partitioned_table(df)
    
    def _create_partitioned_table(self, df) -> None:
        """
        Create a new partitioned Delta table.
        
        Partitions by _cost_date for efficient date-range queries.
        
        Args:
            df: Spark DataFrame to write
        """
        logger.info(f"Creating partitioned Delta table at {self._table_path}")
        
        df.write \
            .format("delta") \
            .partitionBy("_cost_date") \
            .mode("overwrite") \
            .save(self._table_path)
        
        logger.info("Delta table created with date partitioning")
    
    def optimize_table(self) -> None:
        """
        Optimize the Delta table for better query performance.
        
        Runs OPTIMIZE and VACUUM commands.
        """
        from delta.tables import DeltaTable
        
        if not DeltaTable.isDeltaTable(spark, self._table_path):
            logger.warning("Table does not exist, skipping optimization")
            return
        
        logger.info("Optimizing Delta table...")
        
        # Optimize (compacts small files)
        spark.sql(f"OPTIMIZE delta.`{self._table_path}`")
        
        # Z-Order by commonly filtered columns
        spark.sql(f"""
            OPTIMIZE delta.`{self._table_path}` 
            ZORDER BY (SubscriptionId, ResourceGroup, ServiceName)
        """)
        
        # Vacuum old files (retain 7 days by default)
        spark.sql(f"VACUUM delta.`{self._table_path}` RETAIN 168 HOURS")
        
        logger.info("Table optimization completed")
    
    def get_table_stats(self) -> dict:
        """
        Get statistics about the Delta table.
        
        Returns:
            dict: Table statistics
        """
        from delta.tables import DeltaTable
        
        if not DeltaTable.isDeltaTable(spark, self._table_path):
            return {"exists": False}
        
        df = spark.read.format("delta").load(self._table_path)
        
        stats = {
            "exists": True,
            "total_rows": df.count(),
            "partitions": df.select("_cost_date").distinct().count(),
            "scopes": df.select("_source_scope_name").distinct().collect(),
            "date_range": {
                "min": df.agg({"_cost_date": "min"}).collect()[0][0],
                "max": df.agg({"_cost_date": "max"}).collect()[0][0]
            }
        }
        
        return stats
