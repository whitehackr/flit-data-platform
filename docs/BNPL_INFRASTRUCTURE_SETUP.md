# BNPL Pipeline Infrastructure Setup

## Overview

This document describes the infrastructure foundation for the BNPL (Buy Now, Pay Later) data engineering pipeline. This is Phase 1 of the broader BNPL analytics initiative, establishing production-grade data infrastructure for ingesting 1.8M historical transactions and enabling ML model development.

## Architecture

```
simtom API → BigQuery (raw) → dbt (transform) → Analytics/ML tables
```

**Key Components:**
- **Data Source**: simtom API (https://simtom-production.up.railway.app/stream/bnpl)
- **Storage**: BigQuery datasets for raw, intermediate, and mart layers
- **Orchestration**: Apache Airflow with LocalExecutor
- **Quality**: Great Expectations for data validation
- **Pattern**: ELT leveraging BigQuery compute

## Environment Setup

### Prerequisites
- Python 3.11+ with conda
- Docker and Docker Compose
- Google Cloud credentials configured
- Access to flit-data-platform BigQuery project

### Setup Commands
```bash
# Navigate to project
cd /Users/kevin/Documents/repos/flit-data-platform

# Activate environment
conda activate flit

# Set GCP credentials
export GOOGLE_APPLICATION_CREDENTIALS="/Users/kevin/Documents/repos/.gcp/flit-data-platform-dev-sa.json"

# Install dependencies
pip install -r requirements.txt
```

## BigQuery Infrastructure

### Datasets Created
- `flit_bnpl_raw` - Raw BNPL transaction data from simtom API
- `flit_bnpl_intermediate` - Intermediate BNPL data transformations  
- `flit_bnpl_marts` - Analytics-ready BNPL data marts

## Directory Structure

```
flit-data-platform/
├── scripts/bnpl/
│   ├── __init__.py
│   └── api_client.py           # Production-grade API client
├── airflow/
│   ├── docker-compose.yml      # LocalExecutor + PostgreSQL
│   ├── .env                    # Environment configuration
│   ├── dags/                   # Airflow DAGs
│   ├── plugins/                # Custom Airflow plugins
│   └── utils/                  # Utility functions
├── great_expectations/
│   ├── expectations/           # Data quality expectations
│   └── checkpoints/            # Validation checkpoints
└── docs/                       # Project documentation
```

## API Client

The BNPL API client (`scripts/bnpl/api_client.py`) provides production-grade features:

- Exponential backoff retry logic
- Rate limiting and request throttling
- Comprehensive logging and monitoring
- Input validation and response verification

### Key Methods
- `get_bnpl_data()` - Core API interaction method
- `get_daily_batch()` - Simplified daily data retrieval  
- `test_connection()` - API connectivity verification

### Testing Connectivity
```python
from scripts.bnpl.api_client import BNPLAPIClient
client = BNPLAPIClient()
print(client.test_connection())
```

## Airflow Setup

### Starting Airflow
```bash
cd airflow
docker-compose up -d
```

### Access
- Web UI: http://localhost:8080
- Credentials: admin/admin

### Configuration
- **Executor**: LocalExecutor (production-appropriate for single-machine)
- **Database**: PostgreSQL for metadata storage
- **Volumes**: DAGs, plugins, scripts, and GCP credentials mounted

## Data Volume Strategy

- **Target**: 1.8M transactions for ML training
- **Distribution**: Leverages simtom's realistic business patterns (weekends, holidays, seasonal variations)
- **Ingestion**: Daily batches simulating production operations

## Next Steps

1. **PR 2**: Data Ingestion Engine - Historical data acquisition
2. **PR 3**: dbt Transformation Pipeline - Data modeling and transformations
3. **PR 4**: Data Quality Framework - Great Expectations implementation
4. **PR 5**: Airflow Orchestration - Production DAGs and scheduling
5. **PR 6**: ML Feature Engineering - Analytics-ready feature tables

## Important Design Decisions

1. **LocalExecutor over CeleryExecutor** - Right tool for single-machine deployment
2. **ELT over ETL** - Leverage BigQuery compute, preserve raw data
3. **Trust simtom patterns** - Don't duplicate business logic already provided by simtom
4. **Production patterns** - All architecture choices reflect real-world data engineering practices