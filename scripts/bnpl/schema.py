"""
BigQuery Schema Definition for BNPL Raw Data

Schema designed for optimal performance with 1.8M+ records:
- Partitioned by ingestion timestamp for time-series queries
- Clustered by customer_id and risk_level for analytics performance
- Explicit data types for financial precision and query optimization

Design Decision: Explicit schema definition chosen over auto-detection for:
1. Day-1 performance optimization with partitioning/clustering
2. Financial data precision (NUMERIC vs FLOAT64)  
3. Proper timestamp handling for time-series analysis
4. Documentation and field descriptions for ML teams
"""

from google.cloud import bigquery

# Hybrid schema: Core fields + flexible JSON for dynamic API evolution
BNPL_RAW_SCHEMA = [
    # Core identifiers - Always present and needed for joins/performance
    bigquery.SchemaField("transaction_id", "STRING", mode="REQUIRED", description="Unique transaction identifier"),
    bigquery.SchemaField("customer_id", "STRING", mode="REQUIRED", description="Customer identifier"),
    bigquery.SchemaField("product_id", "STRING", mode="REQUIRED", description="Product identifier"),
    
    # Timestamps - Critical for partitioning and time-series analysis
    bigquery.SchemaField("_timestamp", "TIMESTAMP", mode="REQUIRED", description="Transaction timestamp from simtom API"),
    
    # Key financial data - Core for analytics
    bigquery.SchemaField("amount", "NUMERIC", mode="REQUIRED", description="Transaction amount"),
    bigquery.SchemaField("currency", "STRING", mode="REQUIRED", description="Transaction currency"),
    bigquery.SchemaField("status", "STRING", mode="REQUIRED", description="Transaction status"),
    
    # Core risk indicators - Essential for ML
    bigquery.SchemaField("risk_score", "FLOAT64", mode="NULLABLE", description="Calculated risk score (0.0-1.0)"),
    bigquery.SchemaField("risk_level", "STRING", mode="NULLABLE", description="Risk level classification"),
    bigquery.SchemaField("will_default", "BOOLEAN", mode="NULLABLE", description="Predicted default flag"),
    
    # Complete raw record - Preserves ALL fields from API
    bigquery.SchemaField("raw_data", "JSON", mode="REQUIRED", description="Complete raw transaction record from simtom API - preserves all fields for schema evolution"),
    
    # Metadata
    bigquery.SchemaField("_record_id", "INTEGER", mode="NULLABLE", description="Record sequence ID from simtom"),
    bigquery.SchemaField("_generator", "STRING", mode="NULLABLE", description="Data generator identifier"),
    bigquery.SchemaField("_ingestion_timestamp", "TIMESTAMP", mode="REQUIRED", description="When this record was ingested into BigQuery"),
]

# Table configuration for optimal performance
TABLE_CONFIG = {
    "time_partitioning": bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="_timestamp",  # Partition by ingestion timestamp
        require_partition_filter=True  # Force partition filtering for cost optimization
    ),
    "clustering_fields": ["customer_id", "risk_level", "status"],  # Cluster for common query patterns
    "description": (
        "Raw BNPL transaction data from simtom API. "
        "Partitioned by ingestion timestamp for time-series performance. "
        "Clustered by customer_id, risk_level, and status for analytics queries."
    )
}

def get_table_reference(project_id: str = "flit-data-platform", dataset_id: str = "flit_bnpl_raw", table_name: str = "raw_bnpl_transactions") -> str:
    """Get fully qualified table name for BNPL raw transactions."""
    return f"{project_id}.{dataset_id}.{table_name}"