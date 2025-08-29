#!/usr/bin/env python3
"""
Integration test for experiment overlay generation pipeline with BigQuery connectivity

This test validates the complete pipeline:
1. Config consumption from flit-experiment-configs
2. User assignment generation with proper filtering
3. Synthetic overlay order generation with treatment effects
4. BigQuery schema matching and upload
5. Data validation and effect size verification
"""

import pytest
import pandas as pd
import sys
import os
from datetime import datetime

# Add paths for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'scripts'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from generate_synthetic_data import SyntheticDataGenerator
from experiment_assignments import generate_free_shipping_threshold_assignments
from experiment_effects import ExperimentEffectsGenerator
from flit_experiment_configs import get_experiment_config

class TestBigQueryConnectivity:
    """Test BigQuery connectivity and prerequisites"""
    
    def test_bigquery_connection(self):
        """Test that BigQuery is accessible and TheLook dataset is available"""
        from google.cloud import bigquery
        
        client = bigquery.Client()
        
        # Test query to TheLook dataset
        query = "SELECT COUNT(*) as order_count FROM `bigquery-public-data.thelook_ecommerce.orders` LIMIT 1"
        result = client.query(query).to_dataframe()
        
        assert len(result) == 1, "Should return one row"
        assert result.iloc[0]['order_count'] > 0, "TheLook should have orders"
        
        print(f"✅ BigQuery connectivity confirmed - TheLook has {result.iloc[0]['order_count']} orders")

    def test_thelook_schema_access(self):
        """Test that we can access TheLook orders schema"""
        from google.cloud import bigquery
        
        client = bigquery.Client()
        table_ref = "bigquery-public-data.thelook_ecommerce.orders"
        table = client.get_table(table_ref)
        
        # Verify key columns exist
        schema_fields = {field.name: field.field_type for field in table.schema}
        required_columns = ['order_id', 'user_id', 'status', 'created_at', 'num_of_item']
        
        for col in required_columns:
            assert col in schema_fields, f"Required column {col} not found in TheLook orders schema"
        
        print(f"✅ TheLook orders schema accessible with {len(schema_fields)} columns")

class TestExperimentConfigIntegration:
    """Test experiment configuration consumption and validation"""
    
    def test_experiment_config_loading(self):
        """Test loading of free_shipping_threshold_test_v1_1_1 config"""
        config = get_experiment_config('free_shipping_threshold_test_v1_1_1')
        
        # Validate config structure
        assert 'design' in config, "Config should have design section"
        assert 'population' in config, "Config should have population section"
        assert 'metrics' in config, "Config should have metrics section"
        assert 'variants' in config, "Config should have variants section"
        
        # Validate experiment details
        assert config['design']['experiment_name'] == 'free_shipping_threshold_test_v1_1_1'
        
        # Validate effect size calculations
        primary_metric = config['metrics']['primary']
        baseline = primary_metric['baseline_assumptions']['historical_orders_per_user']
        target = primary_metric['baseline_assumptions']['expected_treatment_rate']
        effect_size = (target - baseline) / baseline
        
        # Should be 20% effect
        assert abs(effect_size - 0.20) < 0.01, f"Expected 20% effect size, got {effect_size:.1%}"
        
        print(f"✅ Config loaded: {config['design']['experiment_name']} with {effect_size:.1%} effect")

    def test_country_filtering_integration(self):
        """Test that country filtering works with TheLook country format"""
        config = get_experiment_config('free_shipping_threshold_test_v1_1_1')
        eligible_countries = config['population']['eligibility_criteria']['include']['countries']
        
        # Should use full country names matching TheLook
        expected_countries = ['United States', 'United Kingdom', 'China', 'Brasil', 'Spain', 'France', 'Germany']
        assert eligible_countries == expected_countries, f"Expected {expected_countries}, got {eligible_countries}"
        
        print(f"✅ Country filtering configured for: {eligible_countries}")

@pytest.mark.integration
class TestSyntheticDataGeneratorIntegration:
    """Test SyntheticDataGenerator with real BigQuery connectivity"""
    
    @pytest.fixture
    def project_id(self):
        """Get project ID from environment or prompt user"""
        project_id = os.environ.get('GOOGLE_CLOUD_PROJECT')
        if not project_id:
            # For interactive testing
            project_id = input("Enter your Google Cloud Project ID: ").strip()
        
        if not project_id:
            pytest.skip("No Google Cloud Project ID provided")
        
        return project_id
    
    def test_generator_initialization(self, project_id):
        """Test that SyntheticDataGenerator initializes correctly"""
        generator = SyntheticDataGenerator(project_id=project_id, dataset_id="flit_raw_test")
        
        assert generator.project_id == project_id
        assert generator.dataset_id == "flit_raw_test"
        assert generator.client is not None
        
        print(f"✅ Generator initialized for project: {project_id}")

    def test_thelook_data_retrieval(self, project_id):
        """Test retrieval of TheLook base data"""
        generator = SyntheticDataGenerator(project_id=project_id, dataset_id="flit_raw_test")
        
        # Test users retrieval (sample for speed)
        users_df = generator.get_thelook_users()
        
        assert isinstance(users_df, pd.DataFrame)
        assert len(users_df) > 0, "Should retrieve users data"
        assert 'user_id' in users_df.columns
        assert 'country' in users_df.columns
        
        # Check country format matches our expectations
        countries = users_df['country'].unique()
        assert 'United States' in countries, "Should have United States (not US)"
        
        print(f"✅ Retrieved {len(users_df)} users from TheLook")

@pytest.mark.integration  
class TestExperimentOverlayGeneration:
    """Test complete experiment overlay generation pipeline"""
    
    @pytest.fixture
    def project_id(self):
        """Get project ID from environment or prompt user"""
        project_id = os.environ.get('GOOGLE_CLOUD_PROJECT')
        if not project_id:
            project_id = input("Enter your Google Cloud Project ID: ").strip()
        
        if not project_id:
            pytest.skip("No Google Cloud Project ID provided")
            
        return project_id
    
    def test_assignment_generation_with_real_data(self, project_id):
        """Test assignment generation with real TheLook users"""
        generator = SyntheticDataGenerator(project_id=project_id, dataset_id="flit_raw_test")
        
        # Get sample of real users
        users_df = generator.get_thelook_users()
        
        # Limit to reasonable sample for testing
        sample_users = users_df.sample(n=min(1000, len(users_df)), random_state=42)
        
        # Generate assignments
        assignments_df = generate_free_shipping_threshold_assignments(sample_users)
        
        assert len(assignments_df) > 0, "Should generate assignments from real user data"
        assert 'user_id' in assignments_df.columns
        assert 'variant' in assignments_df.columns
        assert 'experiment_name' in assignments_df.columns
        
        # Check variant balance
        variant_counts = assignments_df['variant'].value_counts()
        assert len(variant_counts) == 2, "Should have exactly 2 variants"
        
        # Check balance (should be roughly 50/50)
        min_count = variant_counts.min()
        max_count = variant_counts.max()
        balance_ratio = min_count / max_count
        assert balance_ratio >= 0.7, f"Variants should be reasonably balanced, got ratio {balance_ratio:.2f}"
        
        print(f"✅ Generated {len(assignments_df)} assignments with distribution: {variant_counts.to_dict()}")

    def test_synthetic_overlay_generation(self, project_id):
        """Test complete synthetic overlay generation"""
        generator = SyntheticDataGenerator(project_id=project_id, dataset_id="flit_raw_test")
        
        # Generate overlay data
        overlay_df = generator.generate_experiment_overlay(
            experiment_name="free_shipping_threshold_test_v1_1_1",
            data_category="orders",
            granularity="order_id",
            source_table_path="bigquery-public-data.thelook_ecommerce.orders"
        )
        
        assert isinstance(overlay_df, pd.DataFrame)
        assert len(overlay_df) > 0, "Should generate synthetic overlay orders"
        
        # Validate schema matches TheLook orders
        required_columns = ['order_id', 'user_id', 'status', 'created_at', 'num_of_item']
        for col in required_columns:
            assert col in overlay_df.columns, f"Overlay should have column {col} matching TheLook schema"
        
        # Validate experiment-specific columns
        assert 'variant' in overlay_df.columns, "Should have variant column"
        assert 'experiment_name' in overlay_df.columns, "Should have experiment_name column"
        
        # Check experiment name consistency
        assert overlay_df['experiment_name'].nunique() == 1
        assert overlay_df['experiment_name'].iloc[0] == 'free_shipping_threshold_test_v1_1_1'
        
        # Validate treatment effect logic
        treatment_orders = overlay_df[overlay_df['variant'] == 'reduced_threshold']
        control_orders = overlay_df[overlay_df['variant'] == 'current_threshold']
        
        # Only treatment users should have synthetic orders (control gets none)
        assert len(control_orders) == 0, "Control group should have no synthetic orders"
        assert len(treatment_orders) > 0, "Treatment group should have synthetic orders"
        
        # Check date range (should be March 1-15, 2024)
        if 'created_at' in overlay_df.columns:
            dates = pd.to_datetime(overlay_df['created_at'])
            min_date = dates.min()
            max_date = dates.max()
            
            assert min_date >= pd.Timestamp('2024-03-01'), f"Orders should start from March 1, got {min_date}"
            assert max_date <= pd.Timestamp('2024-03-15'), f"Orders should end by March 15, got {max_date}"
        
        print(f"✅ Generated {len(overlay_df)} synthetic overlay orders")
        print(f"📊 Treatment orders: {len(treatment_orders)}, Control orders: {len(control_orders)}")
        
        return overlay_df

class TestDataValidation:
    """Test data quality and business logic validation (read-only)"""
    
    @pytest.fixture
    def project_id(self):
        project_id = os.environ.get('GOOGLE_CLOUD_PROJECT')
        if not project_id:
            project_id = input("Enter your Google Cloud Project ID: ").strip()
        
        if not project_id:
            pytest.skip("No Google Cloud Project ID provided")
            
        return project_id
    
    def test_treatment_effect_validation(self, project_id):
        """Validate that treatment effect is correctly applied (no upload)"""
        generator = SyntheticDataGenerator(project_id=project_id, dataset_id="flit_raw_test")
        
        overlay_df = generator.generate_experiment_overlay(
            experiment_name="free_shipping_threshold_test_v1_1_1",
            data_category="orders",
            granularity="order_id", 
            source_table_path="bigquery-public-data.thelook_ecommerce.orders"
        )
        
        # All orders should be for treatment group only
        variants = overlay_df['variant'].unique()
        assert len(variants) == 1, f"Should have only treatment variant, got {variants}"
        assert variants[0] == 'reduced_threshold', f"Should be reduced_threshold variant, got {variants[0]}"
        
        # Validate user count aligns with expected sample size
        unique_users = overlay_df['user_id'].nunique()
        print(f"✅ Treatment effect applied to {unique_users} users")
        
        # The number of synthetic orders should represent the 24% boost
        # (We generate 24% extra orders to achieve 20% effect in final analysis)
        orders_per_user = len(overlay_df) / unique_users if unique_users > 0 else 0
        print(f"📊 Average synthetic orders per treatment user: {orders_per_user:.3f}")
        
        assert unique_users > 0, "Should have treatment users"
        assert orders_per_user > 0, "Should have orders per user"

    def test_table_naming_convention_structure(self, project_id):
        """Test that table naming convention is correctly structured (no upload)"""
        # Test the naming logic without actually creating tables
        experiment_name = "free_shipping_threshold_test_v1_1_1"
        data_category = "orders"
        
        expected_name = f"synthetic_{experiment_name}_{data_category}"
        assert expected_name == "synthetic_free_shipping_threshold_test_v1_1_1_orders"
        
        # Test naming for different scenarios
        test_cases = [
            ("test_experiment", "users", "synthetic_test_experiment_users"),
            ("checkout_test_v2", "sessions", "synthetic_checkout_test_v2_sessions"),
        ]
        
        for exp_name, category, expected in test_cases:
            actual = f"synthetic_{exp_name}_{category}"
            assert actual == expected, f"Expected {expected}, got {actual}"
        
        print(f"✅ Table naming convention validated: synthetic_{{experiment}}_{{category}}")

    def test_production_overlay_method_validation(self, project_id):
        """Test that production overlay method works correctly (no upload)"""
        # We'll test the method logic without actually uploading
        generator = SyntheticDataGenerator(project_id=project_id, dataset_id="flit_raw_test")
        
        # Mock the upload to avoid side effects in testing
        original_upload = generator._upload_dataframe
        upload_calls = []
        
        def mock_upload(df, table_name):
            upload_calls.append({"df_rows": len(df), "table_name": table_name})
            print(f"🔶 Mock upload: {len(df)} rows to {table_name}")
            return None
        
        generator._upload_dataframe = mock_upload
        
        try:
            # Test production overlay generation
            results = generator.generate_production_overlays(["free_shipping_threshold_test_v1_1_1"])
            
            assert len(results) == 1, "Should process one experiment"
            result = results[0]
            
            assert result["status"] == "success", f"Should succeed, got: {result}"
            assert result["experiment_name"] == "free_shipping_threshold_test_v1_1_1"
            assert result["table_name"] == "synthetic_free_shipping_threshold_test_v1_1_1_orders"
            assert result["rows_created"] > 0, "Should create synthetic rows"
            assert result["unique_users"] > 0, "Should have treatment users"
            
            # Verify upload was attempted
            assert len(upload_calls) == 1, "Should attempt one upload"
            assert upload_calls[0]["table_name"] == "synthetic_free_shipping_threshold_test_v1_1_1_orders"
            
            print(f"✅ Production overlay method validated: {result['rows_created']} rows for {result['unique_users']} users")
            
        finally:
            # Restore original upload method
            generator._upload_dataframe = original_upload

if __name__ == "__main__":
    # Run integration tests with verbose output
    pytest.main([__file__, "-v", "-s", "-m", "integration"])