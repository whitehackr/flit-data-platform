# tests/fixtures/ml_sample_data.py
"""Sample data for ML prediction caching tests"""

from datetime import datetime


def sample_transaction_data():
    """Sample transaction data for testing Redis caching"""
    return {
        "transaction_id": "tx_123456",
        "customer_id": "cust_789",
        "amount": 299.99,
        "transaction_timestamp": "2025-09-27T14:30:00Z",
        "customer_credit_score_range": "good",
        "customer_age_bracket": "25-34",
        "device_type": "mobile",
        "product_category": "electronics"
    }


def sample_prediction_data():
    """Sample prediction data for testing Redis caching"""
    return {
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