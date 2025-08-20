import pandas as pd
from datetime import datetime, timedelta
import pytest

@pytest.fixture
def sample_users_df():
    """Create sample user data for testing"""
    return pd.DataFrame({
        'user_id': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        'email': [f'user{i}@test.com' for i in range(1, 11)],
        'first_name': ['John', 'Jane', 'Bob', 'Alice', 'Charlie', 'Diana', 'Eve', 'Frank', 'Grace', 'Henry'],
        'last_name': ['Doe', 'Smith', 'Johnson', 'Brown', 'Davis', 'Miller', 'Wilson', 'Moore', 'Taylor', 'Anderson'],
        'age': [25, 30, 35, 28, 45, 33, 29, 41, 37, 26],
        'gender': ['M', 'F', 'M', 'F', 'M', 'F', 'F', 'M', 'F', 'M'],
        'state': ['CA', 'NY', 'TX', 'FL', 'CA', 'NY', 'TX', 'FL', 'CA', 'NY'],
        'country': ['United States'] * 10,
        'registration_date': [
            '2022-01-15', '2022-02-20', '2022-03-10', '2022-04-05',
            '2022-05-12', '2022-06-18', '2022-07-22', '2022-08-14',
            '2022-09-09', '2022-10-30'
        ],
        'acquisition_channel': ['organic', 'paid_search', 'social', 'email', 'organic', 
                              'paid_search', 'social', 'email', 'organic', 'paid_search']
    })

@pytest.fixture
def sample_orders_df():
    """Create sample order data for testing"""
    return pd.DataFrame({
        'order_id': [101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115],
        'user_id': [1, 2, 3, 1, 4, 2, 5, 3, 1, 6, 7, 8, 9, 10, 4],
        'status': ['Complete'] * 15,
        'order_date': [
            '2023-01-15', '2023-01-20', '2023-02-05', '2023-02-15', '2023-03-01',
            '2023-03-10', '2023-03-20', '2023-04-02', '2023-04-15', '2023-05-01',
            '2023-05-10', '2023-05-20', '2023-06-01', '2023-06-15', '2023-07-01'
        ],
        'shipped_at': [
            '2023-01-16', '2023-01-21', '2023-02-06', '2023-02-16', '2023-03-02',
            '2023-03-11', '2023-03-21', '2023-04-03', '2023-04-16', '2023-05-02',
            '2023-05-11', '2023-05-21', '2023-06-02', '2023-06-16', '2023-07-02'
        ],
        'delivered_at': [
            '2023-01-18', '2023-01-24', '2023-02-09', '2023-02-19', '2023-03-05',
            '2023-03-14', '2023-03-24', '2023-04-06', '2023-04-19', '2023-05-05',
            '2023-05-14', '2023-05-24', '2023-06-05', '2023-06-19', '2023-07-05'
        ],
        'num_of_item': [1, 2, 1, 3, 1, 2, 1, 1, 2, 1, 1, 2, 3, 1, 2]
    })

@pytest.fixture
def expected_experiment_config():
    """Expected experiment configuration for validation"""
    return {
        'checkout_button_color': {
            'variants': ['blue_button', 'green_button', 'orange_button'],
            'expected_allocation': 3  # Should have 3 variants
        },
        'free_shipping_threshold': {
            'variants': ['threshold_50', 'threshold_75', 'threshold_100'],
            'expected_allocation': 3
        },
        'product_recommendations': {
            'variants': ['collaborative_filtering', 'content_based', 'hybrid'],
            'expected_allocation': 3
        },
        'email_frequency': {
            'variants': ['weekly', 'biweekly', 'monthly'],
            'expected_allocation': 3
        }
    }

def create_minimal_test_data():
    """Create minimal test data for quick unit tests"""
    users = pd.DataFrame({
        'user_id': [1, 2, 3],
        'email': ['test1@test.com', 'test2@test.com', 'test3@test.com'],
        'registration_date': ['2022-01-01', '2022-06-01', '2022-12-01']
    })
    
    orders = pd.DataFrame({
        'order_id': [101, 102, 103],
        'user_id': [1, 2, 3],
        'status': ['Complete', 'Complete', 'Complete'],
        'order_date': ['2023-01-01', '2023-06-01', '2023-12-01'],
        'num_of_item': [1, 2, 1]
    })
    
    return users, orders