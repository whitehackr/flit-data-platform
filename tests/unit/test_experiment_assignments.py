# tests/unit/test_experiment_assignments.py
import pytest
import pandas as pd
import sys
import os
from collections import Counter

# Add scripts directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'scripts'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))  # Project root

from experiment_assignments import assign_variant_deterministic, generate_experiment_assignments
from tests.fixtures.sample_data import sample_users_df, expected_experiment_config, create_minimal_test_data

class TestVariantAssignment:
    """Test the core variant assignment logic"""
    
    def test_deterministic_assignment_consistency(self):
        """Test that assignment is deterministic - same input always gives same output"""
        user_id = 12345
        experiment = "test_experiment"
        variants = ["A", "B", "C"]
        allocation = [0.33, 0.33, 0.34]
        
        # Run assignment multiple times
        results = []
        for _ in range(10):
            result = assign_variant_deterministic(user_id, experiment, variants, allocation)
            results.append(result)
        
        # All results should be identical
        assert len(set(results)) == 1, "Assignment should be deterministic"
        assert results[0] in variants, "Result should be a valid variant"
    
    def test_different_users_get_variety(self):
        """Test that different users can get different variants"""
        experiment = "test_experiment"
        variants = ["A", "B", "C"]
        allocation = [0.33, 0.33, 0.34]
        
        assignments = []
        for user_id in range(1, 101):  # Test 100 users
            variant = assign_variant_deterministic(user_id, experiment, variants, allocation)
            assignments.append(variant)
        
        # Should have variety (not all users get same variant)
        unique_variants = set(assignments)
        assert len(unique_variants) > 1, "Different users should get different variants"
        
        # All assignments should be valid
        for assignment in assignments:
            assert assignment in variants, f"Invalid variant: {assignment}"
    
    def test_allocation_roughly_balanced(self):
        """Test that allocation is roughly balanced across many users"""
        experiment = "balance_test"
        variants = ["A", "B", "C"]
        allocation = [0.33, 0.33, 0.34]
        
        assignments = []
        for user_id in range(1, 1001):  # 1000 users for good statistics
            variant = assign_variant_deterministic(user_id, experiment, variants, allocation)
            assignments.append(variant)
        
        # Count each variant
        variant_counts = Counter(assignments)
        
        # Each variant should get roughly 33% (allowing 25-40% range for randomness)
        for variant in variants:
            count = variant_counts[variant]
            percentage = count / len(assignments)
            assert 0.25 <= percentage <= 0.40, f"Variant {variant}: {percentage:.1%} (expected ~33%)"
    
    def test_same_user_different_experiments(self):
        """Test that same user can get different variants in different experiments"""
        user_id = 12345
        variants = ["A", "B"]
        allocation = [0.5, 0.5]
        
        assignments = []
        for i in range(20):  # Different experiment names
            experiment = f"experiment_{i}"
            variant = assign_variant_deterministic(user_id, experiment, variants, allocation)
            assignments.append(variant)
        
        # Should have some variety across experiments
        unique_assignments = set(assignments)
        assert len(unique_assignments) >= 1, "Should have at least one variant assigned"

class TestExperimentGeneration:
    """Test the full experiment generation pipeline"""
    
    def test_generate_with_sample_data(self, sample_users_df, expected_experiment_config):
        """Test experiment generation with known sample data"""
        result_df = generate_experiment_assignments(sample_users_df)
        
        # Basic structure validation
        assert isinstance(result_df, pd.DataFrame), "Should return DataFrame"
        assert len(result_df) > 0, "Should generate assignments"
        
        # Check required columns
        required_columns = [
            'user_id', 'experiment_name', 'variant', 'assigned_date',
            'experiment_start_date', 'experiment_end_date', 'experiment_description'
        ]
        for col in required_columns:
            assert col in result_df.columns, f"Missing column: {col}"
        
        # Validate user IDs
        input_user_ids = set(sample_users_df['user_id'])
        output_user_ids = set(result_df['user_id'])
        assert output_user_ids.issubset(input_user_ids), "All output user_ids should be from input"
        
        # Check experiment names match expected
        expected_experiments = set(expected_experiment_config.keys())
        actual_experiments = set(result_df['experiment_name'])
        assert actual_experiments == expected_experiments, f"Expected {expected_experiments}, got {actual_experiments}"
    
    def test_experiment_variant_validity(self, sample_users_df, expected_experiment_config):
        """Test that all generated variants are valid for each experiment"""
        result_df = generate_experiment_assignments(sample_users_df)
        
        for experiment_name, config in expected_experiment_config.items():
            experiment_data = result_df[result_df['experiment_name'] == experiment_name]
            
            # Check that variants are valid
            actual_variants = set(experiment_data['variant'])
            expected_variants = set(config['variants'])
            assert actual_variants.issubset(expected_variants), \
                f"Invalid variants in {experiment_name}: {actual_variants - expected_variants}"
    
    def test_user_registration_date_filtering(self):
        """Test that users registered after experiment start don't get assigned"""
        # Create users with registration dates after all experiment start dates
        future_users = pd.DataFrame({
            'user_id': [100, 101, 102],
            'email': ['future1@test.com', 'future2@test.com', 'future3@test.com'],
            'registration_date': ['2024-01-01', '2024-06-01', '2024-12-01']  # Future dates
        })
        
        result_df = generate_experiment_assignments(future_users)
        
        # Should have minimal or no assignments for future users
        assert len(result_df) == 0, "Users registered after experiment start shouldn't be assigned"
    
    def test_minimal_data_handling(self):
        """Test with minimal dataset"""
        users, _ = create_minimal_test_data()
        
        result_df = generate_experiment_assignments(users)
        
        # Should handle minimal data gracefully
        assert isinstance(result_df, pd.DataFrame), "Should return DataFrame even with minimal data"
        
        if len(result_df) > 0:  # If any assignments made
            # Validate structure
            assert 'user_id' in result_df.columns
            assert 'experiment_name' in result_df.columns
            assert 'variant' in result_df.columns

class TestDataQuality:
    """Test data quality and validation"""
    
    def test_user_id_data_types(self, sample_users_df):
        """Test that user_ids are proper integers"""
        result_df = generate_experiment_assignments(sample_users_df)
        
        # User IDs should be integers
        assert result_df['user_id'].dtype in ['int64', 'int32'], "User IDs should be integers"
        
        # All user IDs should be positive
        assert all(result_df['user_id'] > 0), "All user IDs should be positive"
    
    def test_date_format_consistency(self, sample_users_df):
        """Test that dates are in consistent format"""
        result_df = generate_experiment_assignments(sample_users_df)
        
        # Check date columns exist and are string format
        date_columns = ['assigned_date', 'experiment_start_date', 'experiment_end_date']
        for col in date_columns:
            assert col in result_df.columns, f"Missing date column: {col}"
            # Should be convertible to datetime
            pd.to_datetime(result_df[col])  # Will raise error if invalid
    
    def test_no_null_critical_fields(self, sample_users_df):
        """Test that critical fields are never null"""
        result_df = generate_experiment_assignments(sample_users_df)
        
        critical_fields = ['user_id', 'experiment_name', 'variant']
        for field in critical_fields:
            null_count = result_df[field].isnull().sum()
            assert null_count == 0, f"Field {field} has {null_count} null values"
    
    def test_assignment_method_recorded(self, sample_users_df):
        """Test that assignment method is properly recorded"""
        result_df = generate_experiment_assignments(sample_users_df)
        
        # Should have assignment_method field
        assert 'assignment_method' in result_df.columns, "Missing assignment_method column"
        
        # All should be 'deterministic_hash'
        unique_methods = result_df['assignment_method'].unique()
        assert list(unique_methods) == ['deterministic_hash'], f"Unexpected assignment methods: {unique_methods}"

class TestEdgeCases:
    """Test edge cases and error conditions"""
    
    def test_empty_dataframe_handling(self):
        """Test handling of empty user dataframe"""
        empty_users = pd.DataFrame({
            'user_id': [],
            'email': [],
            'registration_date': []
        })
        
        result_df = generate_experiment_assignments(empty_users)
        
        # Should return empty DataFrame without errors
        assert isinstance(result_df, pd.DataFrame), "Should return DataFrame"
        assert len(result_df) == 0, "Should return empty DataFrame for empty input"
    
    def test_single_user_handling(self):
        """Test handling of single user"""
        single_user = pd.DataFrame({
            'user_id': [1],
            'email': ['single@test.com'],
            'registration_date': ['2022-01-01']
        })
        
        result_df = generate_experiment_assignments(single_user)
        
        # Should handle single user gracefully
        assert isinstance(result_df, pd.DataFrame), "Should return DataFrame"
        
        if len(result_df) > 0:
            # All assignments should be for the single user
            assert all(result_df['user_id'] == 1), "All assignments should be for user 1"

if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v"])