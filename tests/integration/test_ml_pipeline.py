# tests/integration/test_ml_pipeline.py
import pytest
import sys
import os
from unittest.mock import Mock, patch
import json

# Add scripts directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'scripts', 'ml'))

from redis_client import MLRedisClient
from batch_upload import RedisBigQueryUploader, UploadConfig
from monitoring import MLInfrastructureMonitor
from tests.fixtures.ml_sample_data import sample_transaction_data, sample_prediction_data


class TestMLPipelineIntegration:
    """Integration tests for the complete ML caching pipeline"""

    @pytest.fixture
    def mock_redis_and_bigquery(self):
        """Mock both Redis and BigQuery for integration testing"""
        with patch('redis_client.redis.from_url') as mock_redis, \
             patch('batch_upload.bigquery.Client') as mock_bq:

            # Redis mock setup
            redis_instance = Mock()
            mock_redis.return_value = redis_instance
            redis_instance.ping.return_value = True
            redis_instance.setex.return_value = True
            redis_instance.lpush.return_value = 1
            redis_instance.lrange.return_value = [b'tx:test_123', b'pred:test_456']
            redis_instance.get.return_value = json.dumps(sample_transaction_data()).encode()
            redis_instance.delete.return_value = 1

            # BigQuery mock setup
            bq_instance = Mock()
            mock_bq.return_value = bq_instance

            # Mock BigQuery job
            mock_job = Mock()
            mock_job.result.return_value = None
            mock_job.errors = None
            mock_job.output_rows = 2
            mock_job.done.return_value = True
            bq_instance.load_table_from_json.return_value = mock_job

            yield redis_instance, bq_instance

    def test_complete_caching_and_upload_flow(self, mock_redis_and_bigquery):
        """Test complete flow: cache data → batch upload → cleanup"""
        redis_mock, bq_mock = mock_redis_and_bigquery

        # Step 1: Cache data using ML Redis client
        client = MLRedisClient("redis://localhost:6379")

        transaction_data = sample_transaction_data()
        prediction_data = sample_prediction_data()

        # Cache both types of data
        assert client.cache_transaction("tx_test_123", transaction_data) is True
        assert client.cache_prediction("pred_test_456", prediction_data) is True

        # Step 2: Run batch upload
        config = UploadConfig(redis_url="redis://localhost:6379")
        uploader = RedisBigQueryUploader(config)

        result = uploader.run_daily_upload()

        # Verify upload was successful
        assert result["predictions_success"] is True
        assert result["transactions_success"] is True

        # Verify BigQuery calls
        assert bq_mock.load_table_from_json.call_count == 2  # One for predictions, one for transactions

    def test_monitoring_integration(self, mock_redis_and_bigquery):
        """Test monitoring system integration"""
        redis_mock, bq_mock = mock_redis_and_bigquery

        # Configure Redis stats
        redis_mock.keys.return_value = [b'tx:1', b'tx:2']
        redis_mock.llen.return_value = 3
        redis_mock.info.return_value = {
            'used_memory_human': '1.5M',
            'connected_clients': 2
        }

        # Configure BigQuery query results
        mock_result = Mock()
        mock_result.record_count = 5
        mock_result.latest_upload = None
        bq_mock.query.return_value = [mock_result]

        monitor = MLInfrastructureMonitor()
        report = monitor.generate_health_report()

        assert report["overall_status"] in ["healthy", "warning", "critical"]
        assert "redis" in report["services"]
        assert "bigquery_uploads" in report["services"]

    def test_error_handling_in_pipeline(self, mock_redis_and_bigquery):
        """Test error handling throughout the pipeline"""
        redis_mock, bq_mock = mock_redis_and_bigquery

        # Simulate Redis failure
        redis_mock.ping.return_value = False

        client = MLRedisClient("redis://localhost:6379")
        health_check = client.health_check()

        assert health_check is False

        # Simulate BigQuery failure
        bq_mock.load_table_from_json.side_effect = Exception("BigQuery error")

        config = UploadConfig(redis_url="redis://localhost:6379")
        uploader = RedisBigQueryUploader(config)

        result = uploader.run_daily_upload()

        assert result["predictions_success"] is False or result["transactions_success"] is False
        assert len(result["errors"]) > 0

    @pytest.mark.skip(reason="Requires actual Redis connection")
    def test_real_redis_connectivity(self):
        """Test actual Redis connectivity (skipped by default)"""
        # This test would run against real Redis if environment is configured
        if os.getenv("REDIS_URL"):
            client = MLRedisClient()
            assert client.health_check() is True
        else:
            pytest.skip("No REDIS_URL configured")