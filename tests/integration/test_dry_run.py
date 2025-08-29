"""
Integration test for free shipping threshold experiment overlay generation (dry run)
"""

import pytest
import pandas as pd
import sys
import os

# Add paths for imports (following existing test pattern)
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'scripts'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from experiment_assignments import generate_free_shipping_threshold_assignments
from flit_experiment_configs import get_experiment_config
from tests.fixtures.sample_data import create_minimal_test_data

def create_shipping_threshold_test_data():
    """Create test data suitable for shipping threshold experiment"""
    return pd.DataFrame({
        'user_id': range(1, 201),  # 200 users for better balanced testing
        'email': [f'user{i}@test.com' for i in range(1, 201)],
        'first_name': ['Test'] * 200,
        'last_name': [f'User{i}' for i in range(1, 201)],
        'age': [25 + (i % 40) for i in range(200)],
        'gender': ['M', 'F'] * 100,
        'state': ['CA', 'NY', 'TX', 'FL'] * 50,
        'country': ['United States'] * 200,
        'registration_date': ['2024-01-01'] * 200,
        'acquisition_channel': ['organic', 'paid_search', 'social', 'email'] * 50
    })

def test_experiment_config_loading():
    """Test that experiment config loads correctly with expected structure"""
    
    config = get_experiment_config('free_shipping_threshold_test_v1_1_1')
    
    # Test config structure
    assert 'design' in config, "Config should have 'design' section"
    assert 'metrics' in config, "Config should have 'metrics' section"
    assert 'variants' in config, "Config should have 'variants' section"
    assert 'population' in config, "Config should have 'population' section"
    
    # Test experiment details
    assert config['design']['experiment_name'] == 'free_shipping_threshold_test_v1_1_1', \
        f"Expected experiment name free_shipping_threshold_test_v1_1_1, got {config['design']['experiment_name']}"
    
    # Test metrics structure
    primary_metric = config['metrics']['primary']
    assert primary_metric['name'] == 'orders_per_eligible_user', \
        f"Expected primary metric orders_per_eligible_user, got {primary_metric['name']}"
    
    # Test baseline assumptions
    baseline_assumptions = primary_metric['baseline_assumptions']
    assert 'historical_orders_per_user' in baseline_assumptions, "Config should have historical baseline"
    assert 'expected_treatment_rate' in baseline_assumptions, "Config should have treatment target"
    
    baseline = baseline_assumptions['historical_orders_per_user']
    target = baseline_assumptions['expected_treatment_rate']
    effect_size = (target - baseline) / baseline
    
    # Validate effect size is reasonable (should be around 20%)
    assert 0.15 <= effect_size <= 0.25, \
        f"Effect size {effect_size:.1%} should be between 15-25% for realistic testing"
    
    print(f"✅ Config loaded: {config['design']['experiment_name']}")
    print(f"📊 Baseline: {baseline}, Target: {target}, Effect: {effect_size:.1%}")

def test_experiment_assignment_generation():
    """Test that experiment assignments are generated correctly"""
    
    users_df = create_shipping_threshold_test_data()
    assignments_df = generate_free_shipping_threshold_assignments(users_df)
    
    # Test basic structure
    assert len(assignments_df) > 0, "Should generate assignments for eligible users"
    assert 'user_id' in assignments_df.columns, "Assignments should have user_id"
    assert 'experiment_name' in assignments_df.columns, "Assignments should have experiment_name"
    assert 'variant' in assignments_df.columns, "Assignments should have variant"
    
    # Test experiment name consistency
    unique_experiments = assignments_df['experiment_name'].unique()
    assert len(unique_experiments) == 1, f"Should have exactly 1 experiment, got {len(unique_experiments)}"
    assert unique_experiments[0] == 'free_shipping_threshold_test_v1_1_1', \
        f"Expected experiment name free_shipping_threshold_test_v1_1_1, got {unique_experiments[0]}"
    
    # Test variant distribution
    variant_counts = assignments_df['variant'].value_counts()
    print(f"📊 Variant distribution: {variant_counts.to_dict()}")
    
    # Should have exactly 2 variants for this experiment
    assert len(variant_counts) == 2, f"Should have 2 variants, got {len(variant_counts)}"
    
    # Variants should be roughly balanced (within 20% of each other)
    min_count = variant_counts.min()
    max_count = variant_counts.max()
    balance_ratio = min_count / max_count
    assert balance_ratio >= 0.8, f"Variants should be roughly balanced, got ratio {balance_ratio:.2f}"
    
    # Test user IDs should be subset of input users
    assignment_users = set(assignments_df['user_id'])
    input_users = set(users_df['user_id'])
    assert assignment_users.issubset(input_users), \
        f"Assignment users should be subset of input users"
    
    print(f"✅ Generated {len(assignments_df)} assignments with balanced variants")

def test_assignment_determinism():
    """Test that assignments are deterministic (same input produces same output)"""
    
    users_df = create_shipping_threshold_test_data()
    
    # Generate assignments twice
    assignments_1 = generate_free_shipping_threshold_assignments(users_df)
    assignments_2 = generate_free_shipping_threshold_assignments(users_df)
    
    # Should be identical
    assert len(assignments_1) == len(assignments_2), "Assignment count should be deterministic"
    
    if len(assignments_1) > 0 and len(assignments_2) > 0:
        # Sort for comparison
        assignments_1_sorted = assignments_1.sort_values('user_id').reset_index(drop=True)
        assignments_2_sorted = assignments_2.sort_values('user_id').reset_index(drop=True)
        
        # Compare variants for each user
        try:
            pd.testing.assert_frame_equal(assignments_1_sorted, assignments_2_sorted, 
                                        check_dtype=False)
        except AssertionError:
            raise AssertionError("Assignments should be deterministic")
    
    print("✅ Assignment generation is deterministic")

def test_country_eligibility_filtering():
    """Test that only eligible countries are included in assignments"""
    
    # Create mixed country data
    mixed_users = pd.DataFrame({
        'user_id': range(1, 11),
        'email': [f'user{i}@test.com' for i in range(1, 11)],
        'country': ['United States', 'Canada', 'United Kingdom', 'France', 'Germany'] * 2,
        'registration_date': ['2024-01-01'] * 10,
        'state': ['CA'] * 10,
        'first_name': ['Test'] * 10,
        'last_name': ['User'] * 10,
        'age': [30] * 10,
        'gender': ['M'] * 10,
        'acquisition_channel': ['organic'] * 10
    })
    
    assignments_df = generate_free_shipping_threshold_assignments(mixed_users)
    
    if len(assignments_df) > 0:
        # Get config to check eligible countries
        config = get_experiment_config('free_shipping_threshold_test_v1_1_1')
        eligible_countries = config['population']['eligibility_criteria']['include']['countries']
        
        # Check that all assigned users are from eligible countries
        assigned_users = assignments_df['user_id'].tolist()
        assigned_countries = mixed_users[mixed_users['user_id'].isin(assigned_users)]['country'].unique()
        
        for country in assigned_countries:
            assert country in eligible_countries, \
                f"Assigned user from {country} but only {eligible_countries} are eligible"
        
        print(f"✅ Only eligible countries assigned: {list(assigned_countries)}")
    else:
        print("ℹ️ No assignments generated (expected if no eligible users)")

if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v", "-s"])