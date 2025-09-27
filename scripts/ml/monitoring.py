"""
Redis and BigQuery Upload Monitoring

Provides health checks and monitoring for ML prediction caching infrastructure.
Tracks Redis health, cache usage, and BigQuery upload success rates.
"""

import logging
import json
from datetime import datetime, timedelta
from typing import Dict, Any, List
import os

from google.cloud import bigquery
from .redis_client import MLRedisClient
from .batch_upload import RedisBigQueryUploader, UploadConfig


class MLInfrastructureMonitor:
    """Monitor Redis cache and BigQuery upload health."""

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.redis_client = MLRedisClient()

        # BigQuery client for upload monitoring
        self.bq_client = bigquery.Client(project="flit-data-platform")

    def check_redis_health(self) -> Dict[str, Any]:
        """Check Redis connection and performance."""
        health_report = {
            "service": "redis",
            "timestamp": datetime.utcnow().isoformat(),
            "status": "unknown",
            "details": {}
        }

        try:
            # Basic connectivity
            is_connected = self.redis_client.health_check()

            if not is_connected:
                health_report["status"] = "down"
                health_report["details"]["error"] = "Redis connection failed"
                return health_report

            # Get cache statistics
            stats = self.redis_client.get_cache_stats()

            health_report["status"] = "healthy"
            health_report["details"] = {
                "connected": True,
                "transaction_keys": stats.get("transaction_keys", 0),
                "prediction_keys": stats.get("prediction_keys", 0),
                "upload_queue_size": stats.get("upload_queue_size", 0),
                "memory_used": stats.get("memory_used", "Unknown"),
                "connected_clients": stats.get("connected_clients", 0)
            }

            # Check for alerts
            alerts = []

            # Memory usage warning (if we can parse it)
            memory_str = stats.get("memory_used", "0")
            if "M" in memory_str:
                try:
                    memory_mb = float(memory_str.replace("M", "").replace("B", ""))
                    if memory_mb > 400:  # 400MB threshold
                        alerts.append(f"High memory usage: {memory_str}")
                except:
                    pass

            # Queue size warning
            queue_size = stats.get("upload_queue_size", 0)
            if queue_size > 10000:
                alerts.append(f"Large upload queue: {queue_size} items")

            if alerts:
                health_report["details"]["alerts"] = alerts

        except Exception as e:
            health_report["status"] = "error"
            health_report["details"]["error"] = str(e)

        return health_report

    def check_bigquery_upload_health(self) -> Dict[str, Any]:
        """Check recent BigQuery upload success rates."""
        health_report = {
            "service": "bigquery_uploads",
            "timestamp": datetime.utcnow().isoformat(),
            "status": "unknown",
            "details": {}
        }

        try:
            # Check recent uploads to prediction logs
            yesterday = datetime.utcnow() - timedelta(days=1)

            # Query recent prediction uploads
            pred_query = f"""
            SELECT
                COUNT(*) as record_count,
                MIN(_ingestion_timestamp) as earliest_upload,
                MAX(_ingestion_timestamp) as latest_upload
            FROM `flit-data-platform.flit_ml_raw.raw_bnpl_prediction_logs`
            WHERE _ingestion_timestamp >= '{yesterday.isoformat()}'
            """

            try:
                pred_result = list(self.bq_client.query(pred_query))[0]
                pred_count = pred_result.record_count
                latest_pred = pred_result.latest_upload
            except Exception as e:
                # Table might not exist yet
                pred_count = 0
                latest_pred = None

            # Query recent transaction uploads
            tx_query = f"""
            SELECT
                COUNT(*) as record_count,
                MAX(_ingestion_timestamp) as latest_upload
            FROM `flit-data-platform.flit_bnpl_raw.raw_bnpl_txs_json`
            WHERE _ingestion_timestamp >= '{yesterday.isoformat()}'
            """

            try:
                tx_result = list(self.bq_client.query(tx_query))[0]
                tx_count = tx_result.record_count
                latest_tx = tx_result.latest_upload
            except Exception as e:
                tx_count = 0
                latest_tx = None

            health_report["status"] = "healthy"
            health_report["details"] = {
                "prediction_uploads_24h": pred_count,
                "transaction_uploads_24h": tx_count,
                "latest_prediction_upload": latest_pred.isoformat() if latest_pred else None,
                "latest_transaction_upload": latest_tx.isoformat() if latest_tx else None
            }

            # Check for alerts
            alerts = []

            # No recent uploads
            if pred_count == 0 and tx_count == 0:
                alerts.append("No uploads in last 24 hours")

            # Stale uploads (no uploads in last 2 hours)
            two_hours_ago = datetime.utcnow() - timedelta(hours=2)

            if latest_pred and latest_pred < two_hours_ago:
                alerts.append("Prediction uploads may be stale")

            if latest_tx and latest_tx < two_hours_ago:
                alerts.append("Transaction uploads may be stale")

            if alerts:
                health_report["details"]["alerts"] = alerts
                health_report["status"] = "warning"

        except Exception as e:
            health_report["status"] = "error"
            health_report["details"]["error"] = str(e)

        return health_report

    def generate_health_report(self) -> Dict[str, Any]:
        """Generate comprehensive health report."""
        overall_report = {
            "timestamp": datetime.utcnow().isoformat(),
            "overall_status": "healthy",
            "services": {}
        }

        # Check Redis
        redis_health = self.check_redis_health()
        overall_report["services"]["redis"] = redis_health

        # Check BigQuery uploads
        bq_health = self.check_bigquery_upload_health()
        overall_report["services"]["bigquery_uploads"] = bq_health

        # Determine overall status
        service_statuses = [svc["status"] for svc in overall_report["services"].values()]

        if "error" in service_statuses or "down" in service_statuses:
            overall_report["overall_status"] = "critical"
        elif "warning" in service_statuses:
            overall_report["overall_status"] = "warning"
        else:
            overall_report["overall_status"] = "healthy"

        return overall_report

    def log_health_status(self):
        """Log current health status with appropriate log levels."""
        report = self.generate_health_report()

        overall_status = report["overall_status"]

        if overall_status == "critical":
            self.logger.error(f"ML Infrastructure CRITICAL: {json.dumps(report, indent=2)}")
        elif overall_status == "warning":
            self.logger.warning(f"ML Infrastructure WARNING: {json.dumps(report, indent=2)}")
        else:
            self.logger.info(f"ML Infrastructure HEALTHY: {json.dumps(report, indent=2)}")


def main():
    """Main monitoring entry point."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    print("ML Infrastructure Health Monitor")
    print("=" * 40)

    monitor = MLInfrastructureMonitor()
    report = monitor.generate_health_report()

    # Print formatted report
    print(f"Overall Status: {report['overall_status'].upper()}")
    print(f"Timestamp: {report['timestamp']}")
    print()

    for service_name, service_report in report["services"].items():
        status = service_report["status"].upper()
        print(f"{service_name}: {status}")

        if "alerts" in service_report["details"]:
            for alert in service_report["details"]["alerts"]:
                print(f"  ⚠️  {alert}")

        # Show key metrics
        details = service_report["details"]
        if service_name == "redis":
            print(f"  📊 Transactions: {details.get('transaction_keys', 0)}")
            print(f"  📊 Predictions: {details.get('prediction_keys', 0)}")
            print(f"  📊 Queue Size: {details.get('upload_queue_size', 0)}")
            print(f"  💾 Memory: {details.get('memory_used', 'Unknown')}")
        elif service_name == "bigquery_uploads":
            print(f"  📈 Predictions (24h): {details.get('prediction_uploads_24h', 0)}")
            print(f"  📈 Transactions (24h): {details.get('transaction_uploads_24h', 0)}")

        print()


if __name__ == "__main__":
    main()