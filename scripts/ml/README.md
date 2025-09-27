# ML Prediction Caching Infrastructure

Redis-based caching system for BNPL ML predictions with BigQuery batch upload pipeline.

## Architecture & Design Decisions

This implementation uses a hybrid caching approach for ML prediction persistence, balancing real-time response requirements with cost-effective batch processing to BigQuery.

**For detailed architectural decisions, implementation insights, and design trade-offs, see [PR #14: Redis ML Caching Infrastructure](https://github.com/whitehackr/flit-data-platform/pull/14).**

Key architectural choices:
- **Hybrid caching pattern**: Redis cache → BigQuery batch uploads vs direct persistence
- **Database separation**: Redis DB partitioning for operational isolation (transactions vs predictions)
- **Network topology**: Private Railway networking for production, public endpoints for development
- **Error handling**: Graceful degradation ensuring ML API availability during cache failures

## Architecture Overview

```
eCommerce App → ML API (prediction) → eCommerce App (decision)
     ↓                    ↓
Redis Cache          Redis Cache
(transactions)       (predictions)
     ↓                    ↓
BigQuery             BigQuery
(flit_bnpl_raw)      (flit_ml_raw)
     ↓                    ↓
dbt models           dbt models
(intermediate)       (ML monitoring)
```

## Quick Start

### 1. Deploy Redis Infrastructure

```bash
cd infrastructure/redis
./deploy.sh
```

This creates a new Railway project called "flit" with Redis configured for ML caching.

### 2. ML Team Integration

```python
from scripts.ml.redis_client import MLRedisClient

# Initialize client
ml_cache = MLRedisClient(redis_url="your_redis_url")

# Cache transaction data (from eCommerce app)
transaction_data = {
    "transaction_id": "tx_123",
    "customer_id": "cust_456",
    "amount": 299.99,
    "transaction_timestamp": "2025-09-27T14:30:00Z",
    # ... other transaction fields
}
ml_cache.cache_transaction("tx_123", transaction_data)

# Cache prediction data (from ML API)
prediction_data = {
    "prediction_id": "pred_789",
    "transaction_id": "tx_123",
    "model_predictions": {
        "ridge": 0.234,
        "logistic": 0.251,
        "ensemble": 0.242
    },
    "selected_model": "ridge",
    "business_decision": "approve",
    # ... other prediction fields
}
ml_cache.cache_prediction("pred_789", prediction_data)
```

### 3. Daily Batch Upload

```bash
# Run daily upload job (scheduled at 2 AM UTC)
python -m scripts.ml.batch_upload
```

### 4. Monitoring

```bash
# Check infrastructure health
python -m scripts.ml.monitoring
```

## Redis Schema

### Transaction Data (Database 0)
- **Key Pattern**: `tx:{transaction_id}`
- **TTL**: 7 days
- **Upload Target**: `flit-data-platform.flit_bnpl_raw.raw_bnpl_txs_json`

### Prediction Data (Database 1)
- **Key Pattern**: `pred:{prediction_id}`
- **TTL**: 7 days
- **Upload Target**: `flit-data-platform.flit_ml_raw.raw_bnpl_prediction_logs`

### Upload Queue
- **Key**: `upload_queue`
- **Type**: List
- **Purpose**: Tracks keys for daily batch upload

## BigQuery Tables

### Prediction Logs
- **Table**: `flit-data-platform.flit_ml_raw.raw_bnpl_prediction_logs`
- **Schema**: Auto-detected from prediction data
- **Partitioning**: By `prediction_timestamp` (daily)

### Transaction Data
- **Table**: `flit-data-platform.flit_bnpl_raw.raw_bnpl_txs_json`
- **Schema**: Existing 3-field JSON schema
- **Partitioning**: By `_timestamp` (daily)

## Performance Specifications

### Response Time
- **Target**: <100ms API response time
- **Redis Lookup**: <5ms typical
- **Cache Hit Rate**: >95% expected

### Scale Planning
- **Current**: 5K transactions/day
- **Target**: 50K transactions/day
- **Memory Usage**: 50MB-500MB Redis memory

### Batch Upload
- **Frequency**: Daily at 2 AM UTC
- **Batch Size**: All cached records from previous 24 hours
- **Upload Time**: <5 minutes expected

## Monitoring & Alerts

### Health Checks
- Redis connectivity and memory usage
- Upload queue size monitoring
- BigQuery upload success rates
- Data freshness validation

### Key Metrics
- **Cache Stats**: Transaction/prediction key counts
- **Upload Success**: Daily batch completion rates
- **Data Latency**: Time from cache to BigQuery
- **Error Rates**: Failed caches and uploads

### Alert Thresholds
- Redis memory usage >400MB
- Upload queue size >10K items
- No uploads in 2+ hours
- Upload failure rate >5%

## Environment Configuration

### Required Environment Variables
```bash
REDIS_URL="redis://username:password@host:port"
BIGQUERY_PROJECT="flit-data-platform"
GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"
```

### Redis Database Assignment
- Database 0: Transaction data
- Database 1: Prediction data

## Error Handling

### Redis Failures
- Graceful degradation (ML API continues without caching)
- Automatic reconnection with exponential backoff
- Fallback logging for manual recovery

### BigQuery Upload Failures
- Retry with exponential backoff (max 3 attempts)
- Failed records logged for manual investigation
- Partial success handling (some records uploaded)

### Monitoring Failures
- Health check timeouts handled gracefully
- Service status degraded but not failed
- Manual intervention alerts for critical failures

## Development & Testing

### Local Development
```bash
# Start local Redis
docker run -d -p 6379:6379 redis:latest

# Set environment
export REDIS_URL="redis://localhost:6379"
export BIGQUERY_PROJECT="flit-data-platform"

# Test Redis client
python scripts/ml/redis_client.py

# Test batch upload (dry run)
python scripts/ml/batch_upload.py
```

### Testing with Sample Data
```python
from scripts.ml.redis_client import MLRedisClient

# Generate test data
ml_cache = MLRedisClient()
ml_cache.cache_transaction("test_tx", {"amount": 100.0})
ml_cache.cache_prediction("test_pred", {"score": 0.5})

# Check cache
stats = ml_cache.get_cache_stats()
print(f"Test cache stats: {stats}")
```

## Production Deployment

### Railway Setup
1. **Create flit project**: New Railway project for data platform
2. **Add Redis service**: Railway Redis add-on with persistence
3. **Configure environment**: Set Redis URL and BigQuery credentials
4. **Test connectivity**: Verify Redis access from application

### Scheduling
- **Cron Job**: Daily upload at 2 AM UTC
- **Railway Cron**: `0 2 * * *` schedule
- **Monitoring**: Upload success/failure notifications

### Security
- **Redis Auth**: Password-protected Redis instance
- **Network**: Private Redis network within Railway
- **BigQuery**: Service account with minimal required permissions

## Support & Troubleshooting

### Common Issues
1. **Redis Connection**: Check REDIS_URL format and network access
2. **BigQuery Permissions**: Verify service account has dataset write access
3. **Memory Usage**: Monitor Redis memory and adjust TTL if needed
4. **Upload Delays**: Check queue size and batch job status

### Debugging Commands
```bash
# Check Redis connectivity
python -c "from scripts.ml.redis_client import MLRedisClient; print(MLRedisClient().health_check())"

# Monitor cache usage
python -m scripts.ml.monitoring

# Manual upload trigger
python -m scripts.ml.batch_upload
```

### Contact
- **Data Engineering**: For infrastructure issues and scaling
- **ML Engineering**: For prediction schema and integration
- **DevOps**: For Railway deployment and monitoring setup