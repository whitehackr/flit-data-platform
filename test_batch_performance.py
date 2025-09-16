#!/usr/bin/env python3
"""
Test batch performance optimization with 7-day batches.
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

# Test configuration - 14 days with 7-day batches (2 BigQuery jobs instead of 14)
test_config = IngestionConfig(
    start_date=date(2024, 1, 1),
    end_date=date(2024, 1, 14),  # 14 days for testing
    records_per_day=100,  # Smaller for testing
    batch_days=7,  # Weekly batches
    progress_file="test_batch_progress.json"
)

print("Testing BNPL batch performance optimization...")
print(f"Date range: {test_config.start_date} to {test_config.end_date}")
print(f"Batch size: {test_config.batch_days} days")
print(f"Expected: 2 BigQuery jobs instead of 14")
print()

# Drop existing table first 
bq_client = bigquery.Client(project=test_config.project_id)
try:
    dataset_ref = bq_client.dataset(test_config.dataset_id)
    table_ref = dataset_ref.table(test_config.table_name)
    bq_client.delete_table(table_ref)
    print("Dropped existing table")
except:
    print("No existing table to drop")

# Run batch ingestion
ingester = BNPLHistoricalIngester(test_config)
result = ingester.run_historical_ingestion()

print(f"\n" + "="*50)
print(f"BATCH TEST COMPLETED")
print(f"="*50)
print(f"Status: {result['status']}")
print(f"Successful batches: {result.get('successful_batches', 'N/A')}")
print(f"Failed batches: {result.get('failed_batches', 'N/A')}")
print(f"Total records: {result.get('total_records_ingested', 0):,}")
print(f"Time taken: {result.get('total_time_seconds', 0):.1f} seconds")
print(f"Throughput: {result.get('records_per_second', 0):.1f} records/second")

# Performance comparison
expected_daily_time = 14 * 13  # 14 days * 13s per day = 182s
actual_time = result.get('total_time_seconds', 0)
speedup = expected_daily_time / actual_time if actual_time > 0 else 0
print(f"\nPerformance Analysis:")
print(f"Expected daily approach: ~{expected_daily_time}s")
print(f"Actual batch approach: {actual_time:.1f}s")  
print(f"Speedup: {speedup:.1f}x faster")