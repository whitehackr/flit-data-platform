# BNPL Data Ingestion Engine

## Overview

The BNPL data ingestion engine retrieves historical transaction data from the simtom API and loads it into BigQuery for analytics and ML model development. Designed to handle 1.8M+ historical transactions with production-grade reliability and flexibility.

## Key Features

- **Dynamic Schema Handling**: Hybrid approach with core fields + JSON blob for API evolution
- **Resumable Ingestion**: Progress tracking allows restarting from failures
- **Data Validation**: Required field validation for data quality
- **Batch Processing**: Daily batches simulating production workflows
- **Error Handling**: Comprehensive retry logic and failure recovery

## Architecture

```
simtom API → API Client → Data Transformation → BigQuery (Batch Loading)
                     ↓
              Progress Tracking (JSON file)
```

## Schema Design

### Hybrid Schema Approach

**Core Structured Fields** (for performance/validation):
- `transaction_id` (REQUIRED) - Primary key
- `timestamp` (REQUIRED) - Transaction date/time  
- `customer_id` (REQUIRED) - Customer identifier
- `amount` (REQUIRED) - Transaction amount
- `currency` - Transaction currency
- `product_id`, `status`, `risk_score`, etc. - Key analytics fields

**Complete Raw Data** (for flexibility):
- `raw_data` (JSON) - Complete simtom API response preserving ALL fields

**Benefits**:
- **Performance**: Core fields optimized for queries and joins
- **Data Completeness**: Zero data loss with JSON preservation
- **Schema Evolution**: New API fields automatically captured
- **Data Quality**: Validation on business-critical fields

### Design Decision: Why Hybrid Schema?

**Alternative Considered**: Auto-detect all fields as separate columns
**Chosen Approach**: Core fields + JSON blob

**Reasoning**:
1. **Dynamic API**: simtom returns different fields based on transaction scenarios (e.g., `purchase_context` only appears for certain transactions)
2. **Schema Evolution**: API may add fields in future (day 217 of 365-day ingestion)
3. **Raw Data Principle**: Preserve complete source data for downstream analysis
4. **Performance**: Core fields still available for efficient querying/partitioning

## Usage

### Basic Usage

```python
from scripts.bnpl.historical_ingestion import BNPLHistoricalIngester, IngestionConfig
from datetime import date

# Configuration with realistic volume patterns
config = IngestionConfig(
    start_date=date(2024, 1, 1),
    end_date=date(2024, 12, 31),
    base_daily_volume=5000,  # Average volume (varies realistically)
    batch_days=30,  # Monthly batches for speed
    seed=42,  # Reproducible realistic patterns
    progress_file="bnpl_ingestion_progress.json"
)

# Run ingestion
ingester = BNPLHistoricalIngester(config)
result = ingester.run_historical_ingestion()

print(f"Status: {result['status']}")
print(f"Records ingested: {result['total_records_ingested']}")
print(f"Volume variations: Christmas {int(5000*0.12):,}, Black Friday {int(5000*2.04):,}")
```

### Production Usage

```bash
# Full historical ingestion with realistic patterns
python -m scripts.bnpl.historical_ingestion

# Custom date range with realistic volumes
python -c "
from scripts.bnpl.historical_ingestion import BNPLHistoricalIngester, IngestionConfig
from datetime import date

config = IngestionConfig(
    start_date=date(2024, 6, 1),
    end_date=date(2024, 6, 30),
    base_daily_volume=5000,  # Average - actual varies by business patterns
    batch_days=7,  # Weekly batches
    seed=42  # Reproducible results
)

ingester = BNPLHistoricalIngester(config)
result = ingester.run_historical_ingestion()
print(result)
"
```

## Configuration

### IngestionConfig Parameters

- `start_date` / `end_date`: Date range for historical data
- `base_daily_volume`: Average daily volume (actual varies with realistic business patterns)
- `seed`: Random seed for reproducible realistic volume patterns
- `batch_days`: Group multiple days per BigQuery job for performance (default: 30)
- `project_id`: BigQuery project ID
- `dataset_id`: BigQuery dataset name
- `table_name`: BigQuery table name
- `progress_file`: Progress tracking file location
- `max_retries`: Retry attempts for failed API calls
- `retry_delay`: Delay between retry attempts

### Realistic Volume Patterns

The ingestion engine uses simtom's business intelligence to generate realistic daily variations:

- **Weekend reduction**: 70-85% of weekday volumes
- **Holiday effects**: Christmas ~10%, Black Friday ~160% of baseline
- **Seasonal patterns**: January post-holiday low, November pre-holiday high
- **Paycheck cycles**: Higher volumes in weeks 1 & 3 of month
- **Daily noise**: Natural ±10% variation for realism

### Environment Setup

```bash
# Set GCP credentials
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"

# Activate Python environment
conda activate flit
```

## Data Validation

### Required Fields
The ingestion engine validates these core fields are present:
- `transaction_id` - Must be non-empty string
- `timestamp` - Must be valid ISO datetime
- `customer_id` - Must be non-empty string  
- `amount` - Must be numeric value

**Validation Failures**: Records missing required fields will cause ingestion to fail for that date, ensuring data quality.

## Progress Tracking

### Resumable Ingestion
Progress is tracked in JSON file with:
- `completed_dates`: Successfully processed dates
- `failed_dates`: Dates that failed processing
- `total_records_ingested`: Running count
- `ingestion_start_time`: When ingestion began

### Recovery from Failures
- **Restart**: Re-run ingestion to continue from where it left off
- **Retry Failed Dates**: Failed dates are retried on subsequent runs
- **Manual Recovery**: Edit progress file to reset specific dates if needed

## Performance

### Benchmarks
- **Test Environment**: 100 records across 2 days
- **Throughput**: ~7.4 records/second with batch loading
- **API Calls**: 1 call per day (50 records/call in test)

### Production Estimates
- **1.8M records over 365 days**: ~4.9 hours at 100 records/second
- **Daily processing**: ~30 seconds per day (5K records)
- **BigQuery costs**: Batch loading (free tier compatible)

## Monitoring and Logging

### Log Levels
- **INFO**: Progress updates, successful operations
- **WARNING**: Recoverable issues (timestamp parsing, etc.)
- **ERROR**: Failed dates, API errors, validation failures

### Key Metrics
- Records processed per second
- API call success rate
- Data validation pass rate
- Overall ingestion progress

## Troubleshooting

### Common Issues

**API Connection Failures**:
```
SimtomAPIError: API request failed: Connection timeout
```
- Check network connectivity to simtom API
- Verify API is operational
- Review retry configuration

**BigQuery Permission Errors**:
```
403 Access Denied: BigQuery
```
- Verify service account has BigQuery Data Editor role
- Check dataset exists and is accessible
- Confirm project ID is correct

**Schema Validation Errors**:
```
Missing required field 'transaction_id'
```
- Indicates data quality issue from API
- Check API response format
- Review field mapping in transform_records()

**Progress File Issues**:
```
Could not save progress: Permission denied
```
- Check file write permissions
- Ensure directory exists
- Consider using /tmp/ for containers

### Data Quality Checks

Query BigQuery to validate ingestion:

```sql
-- Check record counts by date
SELECT 
  DATE(_timestamp) as ingestion_date,
  COUNT(*) as record_count
FROM `flit-data-platform.flit_bnpl_raw.raw_bnpl_transactions`
GROUP BY 1
ORDER BY 1;

-- Validate required fields
SELECT 
  COUNT(*) as total_records,
  COUNT(transaction_id) as has_txn_id,
  COUNT(customer_id) as has_customer_id,
  COUNT(amount) as has_amount
FROM `flit-data-platform.flit_bnpl_raw.raw_bnpl_transactions`;

-- Check dynamic fields in JSON
SELECT 
  JSON_EXTRACT_SCALAR(raw_data, '$.purchase_context') as purchase_context,
  COUNT(*) as count
FROM `flit-data-platform.flit_bnpl_raw.raw_bnpl_transactions`
WHERE JSON_EXTRACT_SCALAR(raw_data, '$.purchase_context') IS NOT NULL
GROUP BY 1;
```

## Next Steps

1. **dbt Transformations**: Extract and normalize fields from `raw_data` JSON
2. **Data Quality Framework**: Implement Great Expectations validation
3. **Airflow Orchestration**: Schedule daily ingestion workflows  
4. **Monitoring**: Add alerting for ingestion failures
5. **Performance Optimization**: Tune batch sizes and parallelization