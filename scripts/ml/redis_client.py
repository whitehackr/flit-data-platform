"""
Redis Client for ML Prediction Caching

Provides utilities for ML team to cache predictions and transactions
with optimized Redis operations for the BNPL prediction API.

Architecture:
- Database 0: Transaction data (tx:*)
- Database 1: Prediction data (pred:*)
- Upload queue: Keys to be batch uploaded to BigQuery
"""

import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
import redis
import os
from dotenv import load_dotenv

# Load environment variables from .env.redis
load_dotenv('.env.redis')


class MLRedisClient:
    """Redis client optimized for ML prediction caching."""

    def __init__(self, redis_url: Optional[str] = None):
        """Initialize Redis client with ML-specific configuration."""
        self.redis_url = redis_url or os.getenv("REDIS_URL", "redis://localhost:6379")
        self.client = redis.from_url(self.redis_url)
        self.logger = logging.getLogger(__name__)

        # Database assignments
        self.DB_TRANSACTIONS = 0
        self.DB_PREDICTIONS = 1

        # TTL settings (7 days - data uploaded daily)
        self.TTL_SECONDS = 7 * 24 * 60 * 60  # 7 days

    def cache_transaction(self, transaction_id: str, transaction_data: Dict[str, Any]) -> bool:
        """Cache transaction data for batch upload to BigQuery."""
        try:
            # Select transactions database
            self.client.select(self.DB_TRANSACTIONS)

            # Add timestamp if not present
            if '_timestamp' not in transaction_data:
                transaction_data['_timestamp'] = datetime.utcnow().isoformat()

            # Cache transaction with TTL
            key = f"tx:{transaction_id}"
            self.client.setex(
                key,
                self.TTL_SECONDS,
                json.dumps(transaction_data)
            )

            # Add to upload queue
            self.client.lpush("upload_queue", key)

            self.logger.info(f"Cached transaction: {transaction_id}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to cache transaction {transaction_id}: {e}")
            return False

    def cache_prediction(self, prediction_id: str, prediction_data: Dict[str, Any]) -> bool:
        """Cache prediction data for batch upload to BigQuery."""
        try:
            # Select predictions database
            self.client.select(self.DB_PREDICTIONS)

            # Add timestamp if not present
            if 'prediction_timestamp' not in prediction_data:
                prediction_data['prediction_timestamp'] = datetime.utcnow().isoformat()

            # Cache prediction with TTL
            key = f"pred:{prediction_id}"
            self.client.setex(
                key,
                self.TTL_SECONDS,
                json.dumps(prediction_data)
            )

            # Add to upload queue
            self.client.lpush("upload_queue", key)

            self.logger.info(f"Cached prediction: {prediction_id}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to cache prediction {prediction_id}: {e}")
            return False

    def get_transaction(self, transaction_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve transaction data from cache."""
        try:
            self.client.select(self.DB_TRANSACTIONS)
            data = self.client.get(f"tx:{transaction_id}")

            if data:
                return json.loads(data.decode('utf-8'))
            return None

        except Exception as e:
            self.logger.error(f"Failed to get transaction {transaction_id}: {e}")
            return None

    def get_prediction(self, prediction_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve prediction data from cache."""
        try:
            self.client.select(self.DB_PREDICTIONS)
            data = self.client.get(f"pred:{prediction_id}")

            if data:
                return json.loads(data.decode('utf-8'))
            return None

        except Exception as e:
            self.logger.error(f"Failed to get prediction {prediction_id}: {e}")
            return None

    def get_cache_stats(self) -> Dict[str, Any]:
        """Get Redis cache statistics for monitoring."""
        try:
            # Transaction cache stats
            self.client.select(self.DB_TRANSACTIONS)
            tx_keys = len(self.client.keys("tx:*"))

            # Prediction cache stats
            self.client.select(self.DB_PREDICTIONS)
            pred_keys = len(self.client.keys("pred:*"))

            # Upload queue stats
            queue_size = self.client.llen("upload_queue")

            # Redis info
            info = self.client.info()
            memory_used = info.get('used_memory_human', 'Unknown')
            connected_clients = info.get('connected_clients', 0)

            return {
                "transaction_keys": tx_keys,
                "prediction_keys": pred_keys,
                "upload_queue_size": queue_size,
                "memory_used": memory_used,
                "connected_clients": connected_clients,
                "timestamp": datetime.utcnow().isoformat()
            }

        except Exception as e:
            self.logger.error(f"Failed to get cache stats: {e}")
            return {"error": str(e)}

    def health_check(self) -> bool:
        """Check Redis connection health."""
        try:
            return self.client.ping()
        except Exception as e:
            self.logger.error(f"Redis health check failed: {e}")
            return False


# Example usage for ML team
def example_ml_api_integration():
    """Example of how ML team would integrate Redis caching."""

    # Initialize client
    ml_cache = MLRedisClient()

    # Example transaction data (from eCommerce app)
    transaction_data = {
        "transaction_id": "tx_123456",
        "customer_id": "cust_789",
        "amount": 299.99,
        "transaction_timestamp": "2025-09-27T14:30:00Z",
        "customer_credit_score_range": "good",
        "customer_age_bracket": "25-34",
        "device_type": "mobile",
        "product_category": "electronics"
    }

    # Example prediction data (from ML API)
    prediction_data = {
        "prediction_id": "pred_abc123",
        "transaction_id": "tx_123456",
        "customer_id": "cust_789",
        "model_predictions": {
            "ridge": 0.234,
            "logistic": 0.251,
            "elastic": 0.240,
            "ensemble": 0.242
        },
        "selected_model": "ridge",
        "selected_prediction": 0.234,
        "business_decision": "approve",
        "risk_level": "LOW",
        "processing_time_ms": 23.4
    }

    # Cache both records
    ml_cache.cache_transaction("tx_123456", transaction_data)
    ml_cache.cache_prediction("pred_abc123", prediction_data)

    # Get cache stats for monitoring
    stats = ml_cache.get_cache_stats()
    print(f"Cache stats: {stats}")


if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(level=logging.INFO)

    # Run example
    example_ml_api_integration()