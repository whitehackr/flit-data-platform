"""
BNPL Historical Data Ingestion Script

Ingests 1.8M historical BNPL transactions from simtom API into BigQuery.
Designed for production-grade data pipeline with:
- Resumable ingestion with progress tracking
- Daily batch processing for realistic simulation
- Comprehensive error handling and retry logic
- Data validation and quality checks
"""

import logging
import time
from datetime import datetime, date, timedelta, timezone
from decimal import Decimal
from typing import Dict, Any, List, Optional
import json
import os
from dataclasses import dataclass

from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError

from .api_client import BNPLAPIClient, SimtomAPIError
from .schema import BNPL_RAW_SCHEMA, TABLE_CONFIG, get_table_reference, BNPL_JSON_SCHEMA, JSON_TABLE_CONFIG, get_json_table_reference


@dataclass
class IngestionConfig:
    """Configuration for historical data ingestion with realistic volume patterns."""
    start_date: date
    end_date: date
    base_daily_volume: int = 5000  # Average daily volume (actual varies realistically)
    seed: int = 42  # For reproducible realistic patterns
    project_id: str = "flit-data-platform"
    dataset_id: str = "flit_bnpl_raw"
    table_name: str = "raw_bnpl_transactions"
    max_retries: int = 3
    retry_delay: float = 1.0
    progress_file: str = "bnpl_ingestion_progress.json"
    # Performance options
    parallel_workers: int = 1  # Set to >1 for parallel processing
    batch_days: int = 30  # Group multiple days per BigQuery load job for speed


class IngestionProgressTracker:
    """Track and persist ingestion progress for resumability."""
    
    def __init__(self, progress_file: str):
        self.progress_file = progress_file
        self.progress = self._load_progress()
        
    def _load_progress(self) -> Dict[str, Any]:
        """Load progress from file if it exists."""
        if os.path.exists(self.progress_file):
            try:
                with open(self.progress_file, 'r') as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError) as e:
                logging.warning(f"Could not load progress file: {e}")
        
        return {
            "completed_dates": [],
            "failed_dates": [],
            "total_records_ingested": 0,
            "last_updated": None,
            "ingestion_start_time": None
        }
    
    def save_progress(self):
        """Persist current progress to file."""
        self.progress["last_updated"] = datetime.now().isoformat()
        try:
            with open(self.progress_file, 'w') as f:
                json.dump(self.progress, f, indent=2)
        except IOError as e:
            logging.error(f"Could not save progress: {e}")
    
    def mark_date_completed(self, target_date: date, records_count: int):
        """Mark a date as successfully completed."""
        date_str = target_date.isoformat()
        if date_str not in self.progress["completed_dates"]:
            self.progress["completed_dates"].append(date_str)
        
        # Remove from failed if it was there
        if date_str in self.progress["failed_dates"]:
            self.progress["failed_dates"].remove(date_str)
            
        self.progress["total_records_ingested"] += records_count
        self.save_progress()
    
    def mark_date_failed(self, target_date: date):
        """Mark a date as failed."""
        date_str = target_date.isoformat()
        if date_str not in self.progress["failed_dates"]:
            self.progress["failed_dates"].append(date_str)
        self.save_progress()
    
    def is_date_completed(self, target_date: date) -> bool:
        """Check if a date has been successfully completed."""
        return target_date.isoformat() in self.progress["completed_dates"]
    
    def get_remaining_dates(self, start_date: date, end_date: date) -> List[date]:
        """Get list of dates that still need to be processed."""
        all_dates = []
        current_date = start_date
        while current_date <= end_date:
            if not self.is_date_completed(current_date):
                all_dates.append(current_date)
            current_date += timedelta(days=1)
        return all_dates
    
    def get_remaining_batches(self, start_date: date, end_date: date, batch_days: int) -> List[tuple]:
        """Get list of date ranges (batches) that still need to be processed."""
        remaining_dates = self.get_remaining_dates(start_date, end_date)
        if not remaining_dates:
            return []
        
        batches = []
        current_batch = []
        
        for date_item in remaining_dates:
            current_batch.append(date_item)
            
            # If batch is full or we're at the end, create a batch
            if len(current_batch) == batch_days or date_item == remaining_dates[-1]:
                batch_start = current_batch[0]
                batch_end = current_batch[-1]
                batches.append((batch_start, batch_end, current_batch))
                current_batch = []
        
        return batches

    def start_ingestion(self):
        """Mark the start of ingestion process."""
        if not self.progress.get("ingestion_start_time"):
            self.progress["ingestion_start_time"] = datetime.now().isoformat()
            self.save_progress()


class BNPLHistoricalIngester:
    """Main class for historical BNPL data ingestion."""
    
    def __init__(self, config: IngestionConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # Initialize clients
        self.api_client = BNPLAPIClient()
        self.bq_client = bigquery.Client(project=config.project_id)
        
        # Initialize progress tracker
        self.progress_tracker = IngestionProgressTracker(config.progress_file)
        
        # Use JSON table for reliable ingestion
        self.table_ref = get_json_table_reference(config.project_id, config.dataset_id)
        
    def setup_bigquery_table(self) -> bigquery.Table:
        """Create or verify BigQuery table exists with proper schema."""
        dataset_ref = self.bq_client.dataset(self.config.dataset_id)
        table_ref = dataset_ref.table(self.config.table_name)

        try:
            # Try to get existing table
            table = self.bq_client.get_table(table_ref)
            self.logger.info(f"Using existing table: {self.table_ref}")
            return table

        except GoogleCloudError:
            # Table doesn't exist - will be created by autodetect during load
            self.logger.info(f"Table doesn't exist: {self.table_ref}")
            self.logger.info(f"Will be created automatically by load job with autodetect")
            return None
    
    def transform_records(self, raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform API response records for simplified JSON BigQuery ingestion."""
        ingestion_time = datetime.utcnow().replace(tzinfo=timezone.utc)
        transformed_records = []

        for raw_record in raw_records:
            # Minimal validation - only check _timestamp
            timestamp_field = raw_record.get('_timestamp')
            if not timestamp_field:
                raise ValueError(f"Missing required field '_timestamp' in record: {raw_record.get('transaction_id', 'unknown')}")

            # Handle timestamp with proper parsing and validation
            try:
                # Add UTC timezone if missing (simtom sends naive timestamps)
                if not timestamp_field.endswith('Z') and '+' not in timestamp_field:
                    timestamp_field += '+00:00'  # Assume UTC
                else:
                    timestamp_field = timestamp_field.replace('Z', '+00:00')

                parsed_time = datetime.fromisoformat(timestamp_field)

                # Create minimal 3-field record for JSON table
                import json
                transformed = {
                    "_timestamp": parsed_time.isoformat(),
                    "json_body": json.dumps(raw_record),  # ✅ Serialize to JSON string
                    "_ingestion_timestamp": ingestion_time.isoformat(),
                }

            except (ValueError, TypeError, KeyError) as e:
                raise ValueError(f"Invalid or missing _timestamp in record {raw_record.get('transaction_id')}: {e}")

            transformed_records.append(transformed)

        return transformed_records

    # OLD COMPLEX TRANSFORMATION - COMMENTED OUT FOR COMPARISON
    # def transform_records_old(self, raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    #     """Transform API response records for BigQuery ingestion using hybrid schema approach."""
    #     ingestion_time = datetime.utcnow().replace(tzinfo=timezone.utc)
    #     transformed_records = []
    #
    #     for raw_record in raw_records:
    #         # Validate core required fields
    #         required_fields = ['transaction_id', '_timestamp', 'customer_id', 'amount']
    #         for field in required_fields:
    #             if not raw_record.get(field):
    #                 raise ValueError(f"Missing required field '{field}' in record: {raw_record.get('transaction_id', 'unknown')}")
    #
    #         # Create hybrid record: Core fields + complete raw_data JSON
    #         transformed = {
    #             # REQUIRED core fields - business-critical
    #             "transaction_id": raw_record["transaction_id"],
    #             "customer_id": raw_record["customer_id"],
    #             "amount": str(Decimal(str(raw_record["amount"]))),  # Convert to string for JSON serialization, BigQuery will interpret as NUMERIC
    #             "currency": raw_record.get("currency", "USD"),
    #
    #             # Optional core fields for performance/ML
    #             "product_id": raw_record.get("product_id"),
    #             "status": raw_record.get("status"),
    #             "risk_score": raw_record.get("risk_score"),
    #             "risk_level": raw_record.get("risk_level"),
    #             "will_default": raw_record.get("will_default"),
    #
    #             # Metadata
    #             "_record_id": raw_record.get("_record_id"),
    #             "_generator": raw_record.get("_generator"),
    #             "_ingestion_timestamp": ingestion_time.isoformat(),
    #
    #             # Complete raw record - preserves ALL fields including dynamic ones
    #             "raw_data": raw_record
    #         }
    #
    #         # Handle timestamp with proper parsing and validation
    #         try:
    #             # Transaction timestamp from simtom API (_timestamp field)
    #             timestamp_field = raw_record.get('_timestamp')
    #             if not timestamp_field:
    #                 raise ValueError("Missing required field '_timestamp'")
    #
    #             # Add UTC timezone if missing (simtom sends naive timestamps)
    #             if not timestamp_field.endswith('Z') and '+' not in timestamp_field:
    #                 timestamp_field += '+00:00'  # Assume UTC
    #             else:
    #                 timestamp_field = timestamp_field.replace('Z', '+00:00')
    #
    #             parsed_time = datetime.fromisoformat(timestamp_field)
    #             transformed['_timestamp'] = parsed_time.isoformat()
    #
    #         except (ValueError, TypeError, KeyError) as e:
    #             raise ValueError(f"Invalid or missing _timestamp in record {raw_record.get('transaction_id')}: {e}")
    #
    #         transformed_records.append(transformed)
    #
    #     return transformed_records
    
    def ingest_daily_batch(self, target_date: date) -> int:
        """Ingest data for a single day."""
        return self.ingest_multi_day_batch([target_date])
    
    def ingest_multi_day_batch(self, target_dates: List[date]) -> int:
        """Ingest data for multiple days in a single BigQuery load job."""
        date_range_str = f"{target_dates[0]} to {target_dates[-1]}" if len(target_dates) > 1 else str(target_dates[0])
        self.logger.info(f"Starting batch ingestion for {len(target_dates)} days: {date_range_str}")
        
        all_records = []
        successful_dates = []
        
        try:
            # Make single API call for entire date range (performance optimization)
            start_date_str = target_dates[0].strftime('%Y-%m-%d')
            end_date_str = target_dates[-1].strftime('%Y-%m-%d')

            self.logger.info(f"Fetching data for date range: {start_date_str} to {end_date_str}")

            # Get data from simtom API for entire date range
            raw_records = self.api_client.get_bnpl_data(
                start_date=start_date_str,
                end_date=end_date_str,
                base_daily_volume=self.config.base_daily_volume,
                seed=self.config.seed
            )

            if not raw_records:
                raise Exception(f"No records returned for date range {start_date_str} to {end_date_str}")

            # Early validation - check first few records before processing all
            if len(raw_records) > 0:
                sample_record = raw_records[0]
                required_fields = ['transaction_id', 'customer_id', '_timestamp', 'amount']
                missing_fields = [field for field in required_fields if field not in sample_record]
                if missing_fields:
                    raise ValueError(f"EARLY WARNING: Missing required fields in API response: {missing_fields}")
                self.logger.info(f"✓ Early validation passed - required fields present")

            # Transform records
            self.logger.info(f"Transforming {len(raw_records)} records...")
            transformed_records = self.transform_records(raw_records)
            all_records.extend(transformed_records)
            successful_dates = target_dates  # All dates successful if API call succeeds

            self.logger.info(f"✓ Collected and transformed {len(raw_records)} records for {len(target_dates)} days")
            
            if not all_records:
                raise Exception("No records collected from any dates in batch")
            
            # Single BigQuery load job for all records
            # Try to get table, but it's ok if it doesn't exist (autodetect will create it)
            try:
                table = self.bq_client.get_table(self.table_ref)
            except:
                table = self.table_ref  # Use table reference directly if table doesn't exist
            
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                autodetect=True  # ✅ Auto-detect schema like successful examples
                # schema=BNPL_JSON_SCHEMA  # COMMENTED OUT - explicit schema was causing issues
            )
            
            self.logger.info(f"🚀 Starting BigQuery upload: {len(all_records)} records")
            self.logger.info(f"🔧 Table reference: {table}")
            self.logger.info(f"🔧 Job config: source_format={job_config.source_format}, write_disposition={job_config.write_disposition}")

            # Debug: Log sample record WITH VALUES
            if all_records:
                sample = all_records[0]
                self.logger.info(f"🔧 Sample record keys: {list(sample.keys())}")
                self.logger.info(f"🔧 Sample _timestamp: '{sample.get('_timestamp')}'")
                self.logger.info(f"🔧 Sample json_body type: {type(sample.get('json_body'))}")
                self.logger.info(f"🔧 Sample json_body length: {len(sample.get('json_body', ''))}")
                self.logger.info(f"🔧 Sample ingestion_timestamp: '{sample.get('_ingestion_timestamp')}'")

                # Sample transaction data from json_body (now a JSON string)
                import json
                try:
                    json_body = json.loads(sample.get('json_body', '{}'))
                    self.logger.info(f"🔧 Transaction ID from json: '{json_body.get('transaction_id')}'")
                    self.logger.info(f"🔧 Amount from json: {json_body.get('amount')}")
                except:
                    self.logger.info(f"🔧 JSON body preview: {sample.get('json_body', '')[:100]}...")

            try:
                job = self.bq_client.load_table_from_json(
                    all_records,
                    table,
                    job_config=job_config
                )
                self.logger.info(f"✓ Job object created successfully")
                self.logger.info(f"⏳ BigQuery job started: {job.job_id}")
                self.logger.info(f"🔧 Job state: {job.state}")
                self.logger.info(f"🔧 Job created: {job.created}")
            except Exception as e:
                self.logger.error(f"❌ Failed to create BigQuery job: {e}")
                raise

            # Wait for job completion with progress updates
            import time
            start_time = time.time()
            while not job.done():
                elapsed = time.time() - start_time
                if elapsed > 10:  # Log progress every 10 seconds for long jobs
                    self.logger.info(f"⏳ BigQuery job still running... ({elapsed:.1f}s elapsed)")
                time.sleep(2)
                job.reload()

            # Final job result
            job.result()  # This will raise exception if job failed

            # Comprehensive job verification
            elapsed = time.time() - start_time

            if job.errors:
                raise Exception(f"❌ BigQuery load job errors: {job.errors}")

            # Check if job actually processed data
            if not hasattr(job, 'output_rows') or job.output_rows is None:
                raise Exception(f"❌ Job object missing output_rows - job may not have been created properly")

            if job.output_rows == 0:
                raise Exception(f"❌ BigQuery job succeeded but inserted 0 rows! Expected: {len(all_records)}")

            if job.output_rows != len(all_records):
                self.logger.warning(f"⚠️ Row count mismatch: expected {len(all_records)}, inserted {job.output_rows}")

            self.logger.info(f"✅ BigQuery upload completed in {elapsed:.1f}s - {job.output_rows} rows inserted")

            # Quick verification - check if records were actually inserted
            self.logger.info(f"🔍 Verifying upload to table: {self.table_ref}")
            self.logger.info(f"🔍 Date range for verification: {start_date_str} to {end_date_str}")

            query = f"SELECT COUNT(*) as row_count FROM `{self.table_ref}` WHERE DATE(_timestamp) = '{start_date_str}'"
            self.logger.info(f"🔍 Verification query: {query}")

            result = list(self.bq_client.query(query))[0]
            actual_rows = result.row_count

            self.logger.info(f"🔍 Query returned: {actual_rows} rows")

            if actual_rows == 0:
                raise Exception(f"❌ CRITICAL: No rows found in BigQuery after upload!")
            elif actual_rows != len(all_records):
                self.logger.warning(f"⚠️  Row count mismatch: uploaded {len(all_records)}, found {actual_rows} in table")
            else:
                self.logger.info(f"✅ Verification passed: {actual_rows} rows confirmed in BigQuery")
            
            # Mark all successful dates as completed
            total_records = len(all_records)
            for target_date in successful_dates:
                # Estimate records per date for progress tracking
                records_per_date = total_records // len(successful_dates)
                self.progress_tracker.mark_date_completed(target_date, records_per_date)
            
            self.logger.info(f"Successfully ingested {total_records} records for {len(successful_dates)} days")
            return total_records
            
        except (SimtomAPIError, GoogleCloudError, Exception) as e:
            self.logger.error(f"Failed to ingest batch {date_range_str}: {e}")
            # Mark any remaining dates as failed
            for target_date in target_dates:
                if target_date not in successful_dates:
                    self.progress_tracker.mark_date_failed(target_date)
            raise
    
    def run_historical_ingestion(self) -> Dict[str, Any]:
        """Run complete historical ingestion process."""
        self.logger.info("Starting BNPL historical data ingestion")
        
        # Mark ingestion start
        self.progress_tracker.start_ingestion()
        
        # Setup BigQuery table
        self.setup_bigquery_table()
        
        # Use batch processing if batch_days > 1
        if self.config.batch_days > 1:
            return self._run_batch_ingestion()
        else:
            return self._run_daily_ingestion()
    
    def _run_batch_ingestion(self) -> Dict[str, Any]:
        """Run ingestion using multi-day batches for performance."""
        # Get remaining batches to process
        remaining_batches = self.progress_tracker.get_remaining_batches(
            self.config.start_date, 
            self.config.end_date,
            self.config.batch_days
        )
        
        if not remaining_batches:
            self.logger.info("No batches remaining to process!")
            return {
                "status": "completed",
                "total_records": self.progress_tracker.progress["total_records_ingested"],
                "message": "All batches already completed"
            }
        
        self.logger.info(f"Processing {len(remaining_batches)} batches of {self.config.batch_days} days each")
        
        # Process each batch
        successful_batches = 0
        failed_batches = 0
        total_records = 0
        
        start_time = time.time()
        
        for i, (batch_start, batch_end, batch_dates) in enumerate(remaining_batches, 1):
            try:
                records_ingested = self.ingest_multi_day_batch(batch_dates)
                total_records += records_ingested
                successful_batches += 1
                
                # Progress logging
                elapsed = time.time() - start_time
                rate = i / elapsed if elapsed > 0 else 0
                eta = (len(remaining_batches) - i) / rate if rate > 0 else 0
                self.logger.info(f"Progress: {i}/{len(remaining_batches)} batches, "
                               f"{total_records} records, "
                               f"ETA: {eta:.1f}s")
                
                # Small delay between batches
                time.sleep(0.5)
                
            except Exception as e:
                self.logger.error(f"Failed to process batch {batch_start} to {batch_end}: {e}")
                failed_batches += 1
                
                # Continue with next batch rather than failing completely
                continue
        
        # Final summary
        total_time = time.time() - start_time
        summary = {
            "status": "completed" if failed_batches == 0 else "partial",
            "successful_batches": successful_batches,
            "failed_batches": failed_batches,
            "total_records_ingested": total_records,
            "overall_progress": self.progress_tracker.progress["total_records_ingested"],
            "total_time_seconds": total_time,
            "records_per_second": total_records / total_time if total_time > 0 else 0
        }
        
        self.logger.info(f"Batch ingestion summary: {summary}")
        return summary
    
    def _run_daily_ingestion(self) -> Dict[str, Any]:
        """Run ingestion using daily batches (original approach)."""
        # Get remaining dates to process
        remaining_dates = self.progress_tracker.get_remaining_dates(
            self.config.start_date, 
            self.config.end_date
        )
        
        if not remaining_dates:
            self.logger.info("No dates remaining to process!")
            return {
                "status": "completed",
                "total_records": self.progress_tracker.progress["total_records_ingested"],
                "message": "All dates already completed"
            }
        
        self.logger.info(f"Processing {len(remaining_dates)} remaining dates")
        
        # Process each date
        successful_dates = 0
        failed_dates = 0
        total_records = 0
        
        start_time = time.time()
        
        for i, target_date in enumerate(remaining_dates, 1):
            try:
                records_ingested = self.ingest_daily_batch(target_date)
                total_records += records_ingested
                successful_dates += 1
                
                # Progress logging
                if i % 10 == 0:
                    elapsed = time.time() - start_time
                    rate = i / elapsed if elapsed > 0 else 0
                    eta = (len(remaining_dates) - i) / rate if rate > 0 else 0
                    self.logger.info(f"Progress: {i}/{len(remaining_dates)} dates, "
                                   f"{total_records} records, "
                                   f"ETA: {eta:.1f}s")
                
                # Add small delay between requests for rate limiting
                time.sleep(0.5)
                
            except Exception as e:
                self.logger.error(f"Failed to process {target_date}: {e}")
                failed_dates += 1
                
                # Continue with next date rather than failing completely
                continue
        
        # Final summary
        total_time = time.time() - start_time
        summary = {
            "status": "completed" if failed_dates == 0 else "partial",
            "successful_dates": successful_dates,
            "failed_dates": failed_dates,
            "total_records_ingested": total_records,
            "overall_progress": self.progress_tracker.progress["total_records_ingested"],
            "total_time_seconds": total_time,
            "records_per_second": total_records / total_time if total_time > 0 else 0
        }
        
        self.logger.info(f"Ingestion summary: {summary}")
        return summary


def main():
    """Main entry point for historical ingestion."""
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Configuration for full historical ingestion with 10-day batches
    config = IngestionConfig(
        start_date=date(2024, 9, 1),
        end_date=date(2025, 8, 31),  # Full 365 days
        base_daily_volume=5000,  # Average volume (varies realistically by day/season)
        batch_days=10,  # 10-day batches for performance
        seed=42,  # Reproducible realistic patterns
        parallel_workers=1  # Keep simple for now
    )

    print(f"BNPL Historical Data Ingestion with Realistic Volume Patterns")
    print(f"=============================================================")
    print(f"Date range: {config.start_date} to {config.end_date}")
    print(f"Base daily volume: {config.base_daily_volume:,} (varies realistically)")
    print(f"Expected patterns:")
    print(f"  • Weekends: ~{int(config.base_daily_volume * 0.7):,} transactions (30% lower)")
    print(f"  • Black Friday: ~{int(config.base_daily_volume * 1.6):,} transactions (60% spike)")
    print(f"  • Christmas: ~{int(config.base_daily_volume * 0.1):,} transactions (90% reduction)")
    print(f"Batch size: {config.batch_days} days per BigQuery job")
    print(f"Expected batches: ~{(config.end_date - config.start_date).days // config.batch_days}")
    print(f"Estimated completion: 2-3 minutes (125,000x faster than real-time)")
    print()
    
    # Run ingestion
    ingester = BNPLHistoricalIngester(config)
    result = ingester.run_historical_ingestion()
    
    # Print summary
    print(f"\n" + "="*50)
    print(f"INGESTION COMPLETED")
    print(f"="*50)
    print(f"Status: {result['status']}")
    print(f"Total records: {result.get('total_records_ingested', 0):,}")
    print(f"Time taken: {result.get('total_time_seconds', 0):.1f} seconds")
    print(f"Throughput: {result.get('records_per_second', 0):.1f} records/second")
    if 'successful_batches' in result:
        print(f"Successful batches: {result['successful_batches']}")
        print(f"Failed batches: {result['failed_batches']}")
    else:
        print(f"Successful dates: {result.get('successful_dates', 0)}")
        print(f"Failed dates: {result.get('failed_dates', 0)}")


if __name__ == "__main__":
    main()