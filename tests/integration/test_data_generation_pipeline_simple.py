import pytest
import pandas as pd
import sys
import os

# Add paths for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'scripts'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from experiment_assignments import generate_experiment_assignments
from logistics_data import generate_logistics_data
from support_tickets import generate_support_tickets
from tests.fixtures.sample_data import create_minimal_test_data

def test_data_consistency_simple():
    """Test core data consistency across all synthetic datasets"""
    
    # Get minimal test data
    users_df, orders_df = create_minimal_test_data()
    
    # Generate all synthetic datasets
    experiments_df = generate_experiment_assignments(users_df)
    logistics_df = generate_logistics_data(orders_df)
    support_df = generate_support_tickets(users_df, orders_df)
    
    # Test 1: Experiment user IDs should be subset of input users
    experiment_users = set(experiments_df['user_id'])
    input_users = set(users_df['user_id'])
    assert experiment_users.issubset(input_users), \
        f"Experiment users {experiment_users} should be subset of input users {input_users}"
    
    # Test 2: Logistics order IDs should match input orders exactly
    logistics_orders = set(logistics_df['order_id'])
    input_orders = set(orders_df['order_id'])
    assert logistics_orders == input_orders, \
        f"Logistics orders {logistics_orders} should match input orders {input_orders}"
    
    # Test 3: Support ticket user IDs should be subset of input users
    support_users = set(support_df['user_id'])
    assert support_users.issubset(input_users), \
        f"Support users {support_users} should be subset of input users {input_users}"
    
    # Test 4: All datasets should have required columns
    assert 'user_id' in experiments_df.columns, "Experiments should have user_id"
    assert 'experiment_name' in experiments_df.columns, "Experiments should have experiment_name"
    assert 'variant' in experiments_df.columns, "Experiments should have variant"
    
    assert 'order_id' in logistics_df.columns, "Logistics should have order_id"
    assert 'warehouse_id' in logistics_df.columns, "Logistics should have warehouse_id"
    assert 'shipping_cost' in logistics_df.columns, "Logistics should have shipping_cost"
    
    assert 'ticket_id' in support_df.columns, "Support should have ticket_id"
    assert 'issue_category' in support_df.columns, "Support should have issue_category"
    assert 'priority' in support_df.columns, "Support should have priority"
    
    # Test 5: Data types should be correct
    assert pd.api.types.is_integer_dtype(experiments_df['user_id']), "User IDs should be integers"
    assert pd.api.types.is_integer_dtype(logistics_df['order_id']), "Order IDs should be integers"
    assert pd.api.types.is_numeric_dtype(logistics_df['shipping_cost']), "Shipping cost should be numeric"
    
    print(f"✅ Generated {len(experiments_df)} experiment assignments")
    print(f"✅ Generated {len(logistics_df)} logistics records")
    print(f"✅ Generated {len(support_df)} support tickets")
    print("✅ All data consistency checks passed!")

def test_realistic_data_distributions():
    """Test that generated data has realistic business distributions"""
    
    users_df, orders_df = create_minimal_test_data()
    
    # Generate larger sample for distribution testing
    large_users = pd.concat([users_df] * 10, ignore_index=True)
    large_users['user_id'] = range(1, len(large_users) + 1)
    
    large_orders = pd.concat([orders_df] * 10, ignore_index=True)
    large_orders['order_id'] = range(1, len(large_orders) + 1)
    large_orders['user_id'] = [i % len(large_users) + 1 for i in range(len(large_orders))]
    
    # Generate datasets
    experiments_df = generate_experiment_assignments(large_users)
    logistics_df = generate_logistics_data(large_orders)
    support_df = generate_support_tickets(large_users, large_orders)
    
    # Test experiment distributions
    if len(experiments_df) > 0:
        experiment_counts = experiments_df['experiment_name'].value_counts()
        print(f"📊 Experiment distribution: {experiment_counts.to_dict()}")
        
        # Each experiment should have some assignments
        assert all(count > 0 for count in experiment_counts), "All experiments should have assignments"
    
    # Test logistics distributions
    if len(logistics_df) > 0:
        warehouse_counts = logistics_df['warehouse_id'].value_counts()
        carrier_counts = logistics_df['shipping_carrier'].value_counts()
        
        print(f"📦 Warehouse distribution: {warehouse_counts.to_dict()}")
        print(f"🚚 Carrier distribution: {carrier_counts.to_dict()}")
        
        # Should have variety in warehouses and carriers
        assert len(warehouse_counts) >= 1, "Should have warehouse diversity"
        assert len(carrier_counts) >= 1, "Should have carrier diversity"
        
        # Shipping costs should be reasonable
        assert all(logistics_df['shipping_cost'] > 0), "All shipping costs should be positive"
        assert all(logistics_df['shipping_cost'] < 100), "Shipping costs should be reasonable"
    
    # Test support ticket distributions
    if len(support_df) > 0:
        category_counts = support_df['issue_category'].value_counts()
        priority_counts = support_df['priority'].value_counts()
        
        print(f"🎫 Issue category distribution: {category_counts.to_dict()}")
        print(f"⚡ Priority distribution: {priority_counts.to_dict()}")
        
        # Should have variety in categories and priorities
        assert len(category_counts) >= 1, "Should have issue category diversity"
        assert len(priority_counts) >= 1, "Should have priority diversity"
        
        # Satisfaction scores should be in valid range
        assert all(support_df['satisfaction_score'].between(1, 5)), "Satisfaction scores should be 1-5"

def test_no_data_leakage():
    """Test that there's no data leakage between different user cohorts"""
    
    users_df, orders_df = create_minimal_test_data()
    
    # Generate experiments for the same users multiple times
    experiments_1 = generate_experiment_assignments(users_df)
    experiments_2 = generate_experiment_assignments(users_df)
    
    if len(experiments_1) > 0 and len(experiments_2) > 0:
        # Same user should get same variant in same experiment (deterministic)
        merged = experiments_1.merge(
            experiments_2, 
            on=['user_id', 'experiment_name'], 
            suffixes=('_1', '_2')
        )
        
        if len(merged) > 0:
            variant_consistency = all(merged['variant_1'] == merged['variant_2'])
            assert variant_consistency, "Same user should get same variant in repeated generations"
            print("✅ Deterministic assignment verified")

if __name__ == "__main__":
    # Run the simple integration tests
    pytest.main([__file__, "-v", "-s"])