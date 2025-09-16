#!/usr/bin/env python3
"""
Test script for BNPL ingestion with a small sample.
"""

import logging
from datetime import date
from google.cloud import bigquery
from scripts.bnpl.historical_ingestion import BNPLHistoricalIngester, IngestionConfig

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Test configuration - just 2 days with small batches
test_config = IngestionConfig(
    start_date=date(2024, 1, 1),
    end_date=date(2024, 1, 2),  # Only 2 days for testing
    records_per_day=50,  # Small batch for testing
    progress_file="test_ingestion_progress.json"
)

print("Testing BNPL ingestion with 2 days of sample data...")

# Drop existing table first (schema changed)
bq_client = bigquery.Client(project=test_config.project_id)
try:
    dataset_ref = bq_client.dataset(test_config.dataset_id)
    table_ref = dataset_ref.table(test_config.table_name)
    bq_client.delete_table(table_ref)
    print("Dropped existing table with old schema")
except:
    print("No existing table to drop")

# Run ingestion
ingester = BNPLHistoricalIngester(test_config)
result = ingester.run_historical_ingestion()

print(f"\nTest completed!")
print(f"Status: {result['status']}")
print(f"Successful dates: {result['successful_dates']}")
print(f"Failed dates: {result['failed_dates']}")
print(f"Total records: {result['total_records_ingested']}")
print(f"Time taken: {result.get('total_time_seconds', 0):.2f} seconds")