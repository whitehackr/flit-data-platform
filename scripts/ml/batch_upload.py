"""
Redis to BigQuery Batch Upload Job

Daily job that processes ML prediction and transaction data from Redis cache
and uploads to BigQuery for analytics and model monitoring.

Architecture:
- Predictions: Redis → flit_ml_raw.raw_bnpl_prediction_logs (autodetect schema)
- Transactions: Redis → flit_bnpl_raw.raw_bnpl_txs_json (existing table)
"""

import logging
import json
import time
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
import os
from dataclasses import dataclass

import redis
from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError
from dotenv import load_dotenv

# Load environment variables
load_dotenv('.env.redis')


@dataclass
class UploadConfig:
    """Configuration for Redis to BigQuery batch upload."""
    # Redis settings
    redis_url: str
    redis_db_transactions: int = 0
    redis_db_predictions: int = 1

    # BigQuery settings
    project_id: str = "flit-data-platform"
    predictions_dataset: str = "flit_ml_raw"
    predictions_table: str = "raw_bnpl_prediction_logs"
    transactions_dataset: str = "flit_bnpl_raw"
    transactions_table: str = "raw_bnpl_txs_json"

    # Processing settings
    batch_size: int = 1000
    max_retries: int = 3
    retry_delay: float = 2.0


class RedisBigQueryUploader:
    """Handles Redis to BigQuery batch uploads for ML prediction data."""

    def __init__(self, config: UploadConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)

        # Initialize clients
        self.redis_client = redis.from_url(config.redis_url)
        self.bq_client = bigquery.Client(project=config.project_id)

        # Table references
        self.predictions_table_ref = f"{config.project_id}.{config.predictions_dataset}.{config.predictions_table}"
        self.transactions_table_ref = f"{config.project_id}.{config.transactions_dataset}.{config.transactions_table}"

    def get_upload_queue_keys(self, queue_name: str) -> List[str]:
        """Get all keys from upload queue."""
        try:
            # Get all items from the upload queue
            queue_items = self.redis_client.lrange(queue_name, 0, -1)
            return [item.decode('utf-8') for item in queue_items]
        except Exception as e:
            self.logger.error(f"Failed to get upload queue: {e}")
            return []

    def get_records_from_redis(self, keys: List[str], db: int) -> List[Dict[str, Any]]:
        """Retrieve records from Redis by keys."""
        records = []

        # Switch to appropriate database
        self.redis_client.select(db)

        for key in keys:
            try:
                data = self.redis_client.get(key)
                if data:
                    record = json.loads(data.decode('utf-8'))
                    # Add ingestion timestamp
                    record['_ingestion_timestamp'] = datetime.utcnow().isoformat()
                    records.append(record)
                else:
                    self.logger.warning(f"Key {key} not found in Redis")
            except Exception as e:
                self.logger.error(f"Failed to retrieve {key}: {e}")

        return records

    def upload_to_bigquery(self, records: List[Dict[str, Any]], table_ref: str) -> bool:
        """Upload records to BigQuery with autodetect schema."""
        if not records:
            self.logger.info(f"No records to upload to {table_ref}")
            return True

        try:
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                autodetect=True  # Let BigQuery detect schema
            )

            self.logger.info(f"Uploading {len(records)} records to {table_ref}")

            job = self.bq_client.load_table_from_json(
                records,
                table_ref,
                job_config=job_config
            )

            # Wait for job completion
            job.result()

            if job.errors:
                self.logger.error(f"BigQuery job errors: {job.errors}")
                return False

            self.logger.info(f"✅ Successfully uploaded {job.output_rows} rows to {table_ref}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to upload to {table_ref}: {e}")
            return False

    def clean_uploaded_keys(self, keys: List[str], queue_name: str):
        """Remove successfully uploaded keys from Redis."""
        try:
            # Remove keys from cache (they've been uploaded)
            if keys:
                # Switch to appropriate database and delete keys
                for db in [self.config.redis_db_transactions, self.config.redis_db_predictions]:
                    self.redis_client.select(db)
                    deleted = self.redis_client.delete(*keys)
                    self.logger.info(f"Deleted {deleted} keys from Redis DB {db}")

            # Clear the upload queue
            self.redis_client.delete(queue_name)
            self.logger.info(f"Cleared upload queue: {queue_name}")

        except Exception as e:
            self.logger.error(f"Failed to clean up Redis keys: {e}")

    def process_predictions(self) -> bool:
        """Process prediction data upload."""
        self.logger.info("Processing prediction data upload...")

        # Get prediction keys from upload queue
        pred_keys = [key for key in self.get_upload_queue_keys("upload_queue")
                    if key.startswith("pred:")]

        if not pred_keys:
            self.logger.info("No prediction data to upload")
            return True

        # Get prediction records
        prediction_records = self.get_records_from_redis(pred_keys, self.config.redis_db_predictions)

        # Upload to BigQuery
        success = self.upload_to_bigquery(prediction_records, self.predictions_table_ref)

        if success:
            self.clean_uploaded_keys(pred_keys, "pred_upload_queue")

        return success

    def process_transactions(self) -> bool:
        """Process transaction data upload."""
        self.logger.info("Processing transaction data upload...")

        # Get transaction keys from upload queue
        tx_keys = [key for key in self.get_upload_queue_keys("upload_queue")
                  if key.startswith("tx:")]

        if not tx_keys:
            self.logger.info("No transaction data to upload")
            return True

        # Get transaction records
        transaction_records = self.get_records_from_redis(tx_keys, self.config.redis_db_transactions)

        # Upload to BigQuery
        success = self.upload_to_bigquery(transaction_records, self.transactions_table_ref)

        if success:
            self.clean_uploaded_keys(tx_keys, "tx_upload_queue")

        return success

    def run_daily_upload(self) -> Dict[str, Any]:
        """Run complete daily upload process."""
        start_time = time.time()
        self.logger.info("Starting daily Redis to BigQuery upload")

        results = {
            "start_time": datetime.utcnow().isoformat(),
            "predictions_success": False,
            "transactions_success": False,
            "total_time_seconds": 0,
            "errors": []
        }

        try:
            # Process predictions
            results["predictions_success"] = self.process_predictions()

            # Process transactions
            results["transactions_success"] = self.process_transactions()

            # Calculate total time
            results["total_time_seconds"] = time.time() - start_time

            if results["predictions_success"] and results["transactions_success"]:
                self.logger.info(f"✅ Daily upload completed successfully in {results['total_time_seconds']:.1f}s")
            else:
                self.logger.error("❌ Daily upload completed with errors")

        except Exception as e:
            self.logger.error(f"Daily upload failed: {e}")
            results["errors"].append(str(e))

        return results


def main():
    """Main entry point for daily batch upload job."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Load configuration from environment
    config = UploadConfig(
        redis_url=os.getenv("REDIS_URL", "redis://localhost:6379"),
        project_id=os.getenv("BIGQUERY_PROJECT", "flit-data-platform")
    )

    print("Redis to BigQuery Daily Upload Job")
    print("=" * 40)
    print(f"Redis URL: {config.redis_url}")
    print(f"BigQuery Project: {config.project_id}")
    print(f"Predictions Table: {config.predictions_dataset}.{config.predictions_table}")
    print(f"Transactions Table: {config.transactions_dataset}.{config.transactions_table}")
    print()

    # Run upload
    uploader = RedisBigQueryUploader(config)
    results = uploader.run_daily_upload()

    # Print results
    print("\n" + "=" * 40)
    print("UPLOAD COMPLETED")
    print("=" * 40)
    print(f"Predictions: {'✅ SUCCESS' if results['predictions_success'] else '❌ FAILED'}")
    print(f"Transactions: {'✅ SUCCESS' if results['transactions_success'] else '❌ FAILED'}")
    print(f"Total time: {results['total_time_seconds']:.1f} seconds")

    if results["errors"]:
        print(f"Errors: {len(results['errors'])}")
        for error in results["errors"]:
            print(f"  - {error}")


if __name__ == "__main__":
    main()