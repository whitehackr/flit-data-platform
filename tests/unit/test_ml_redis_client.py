# tests/unit/test_ml_redis_client.py
import pytest
import sys
import os
from unittest.mock import Mock, patch

# Add scripts directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'scripts', 'ml'))

from redis_client import MLRedisClient
from tests.fixtures.ml_sample_data import sample_transaction_data, sample_prediction_data


class TestMLRedisClient:
    """Test the ML Redis client functionality"""

    @pytest.fixture
    def mock_redis_client(self):
        """Mock Redis client for testing"""
        with patch('redis_client.redis.from_url') as mock_redis:
            mock_instance = Mock()
            mock_redis.return_value = mock_instance

            # Configure mock responses
            mock_instance.ping.return_value = True
            mock_instance.setex.return_value = True
            mock_instance.lpush.return_value = 1
            mock_instance.get.return_value = b'{"test": "data"}'
            mock_instance.keys.return_value = [b'tx:1', b'tx:2']
            mock_instance.llen.return_value = 5
            mock_instance.info.return_value = {
                'used_memory_human': '2.5M',
                'connected_clients': 3
            }

            yield mock_instance

    def test_cache_transaction(self, mock_redis_client):
        """Test caching transaction data"""
        client = MLRedisClient()
        transaction_data = sample_transaction_data()

        result = client.cache_transaction("tx_123456", transaction_data)

        assert result is True
        mock_redis_client.select.assert_called_with(0)  # Transactions database
        mock_redis_client.setex.assert_called()
        mock_redis_client.lpush.assert_called_with("upload_queue", "tx:tx_123456")

    def test_cache_prediction(self, mock_redis_client):
        """Test caching prediction data"""
        client = MLRedisClient()
        prediction_data = sample_prediction_data()

        result = client.cache_prediction("pred_abc123", prediction_data)

        assert result is True
        mock_redis_client.select.assert_called_with(1)  # Predictions database
        mock_redis_client.setex.assert_called()
        mock_redis_client.lpush.assert_called_with("upload_queue", "pred:pred_abc123")

    def test_get_transaction(self, mock_redis_client):
        """Test retrieving transaction data"""
        client = MLRedisClient()

        result = client.get_transaction("tx_123456")

        mock_redis_client.select.assert_called_with(0)
        mock_redis_client.get.assert_called_with("tx:tx_123456")
        assert result == {"test": "data"}

    def test_get_prediction(self, mock_redis_client):
        """Test retrieving prediction data"""
        client = MLRedisClient()

        result = client.get_prediction("pred_abc123")

        mock_redis_client.select.assert_called_with(1)
        mock_redis_client.get.assert_called_with("pred:pred_abc123")
        assert result == {"test": "data"}

    def test_health_check(self, mock_redis_client):
        """Test Redis health check"""
        client = MLRedisClient()

        result = client.health_check()

        assert result is True
        mock_redis_client.ping.assert_called_once()

    def test_get_cache_stats(self, mock_redis_client):
        """Test cache statistics retrieval"""
        client = MLRedisClient()

        result = client.get_cache_stats()

        assert "transaction_keys" in result
        assert "prediction_keys" in result
        assert "memory_used" in result
        assert result["memory_used"] == "2.5M"

    def test_connection_failure_handling(self):
        """Test handling of Redis connection failures"""
        with patch('redis_client.redis.from_url') as mock_redis:
            # Mock successful client creation but failing operations
            mock_instance = Mock()
            mock_redis.return_value = mock_instance
            mock_instance.select.side_effect = Exception("Connection failed")

            client = MLRedisClient()
            result = client.cache_transaction("tx_123", {"test": "data"})

            assert result is False