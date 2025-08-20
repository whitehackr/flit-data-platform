import pytest
import pandas as pd
import sys
import os
from unittest.mock import patch, MagicMock
import tempfile
import json

# Add scripts directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'scripts'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))  # Project root

from generate_synthetic_data import SyntheticDataGenerator
from tests.fixtures.sample_data import create_minimal_test_data

class TestSyntheticDataGeneratorInitialization:
    """Test SyntheticDataGenerator initialization and setup"""
    
    @patch('generate_synthetic_data.bigquery.Client')
    def test_generator_initialization(self, mock_bigquery_client):
        """Test that generator initializes correctly"""
        mock_client = MagicMock()
        mock_bigquery_client.return_value = mock_client
        
        generator = SyntheticDataGenerator("test-project", "test_dataset")
        
        assert generator.project_id == "test-project"
        assert generator.dataset_id == "test_dataset"
        assert generator.client == mock_client
    
    @patch('generate_synthetic_data.bigquery.Client')
    def test_dataset_creation_attempt(self, mock_bigquery_client):
        """Test that generator attempts to create dataset if it doesn't exist"""
        mock_client = MagicMock()
        mock_bigquery_client.return_value = mock_client
        
        # Mock dataset doesn't exist
        mock_client.get_dataset.side_effect = Exception("Dataset not found")
        
        generator = SyntheticDataGenerator("test-project", "test_dataset")
        
        # Should attempt to create dataset
        mock_client.create_dataset.assert_called_once()

class TestTheLookDataRetrieval:
    """Test retrieval of TheLook base data"""
    
    @patch('generate_synthetic_data.bigquery.Client')
    def test_get_thelook_users(self, mock_bigquery_client):
        """Test TheLook users data retrieval"""
        # Mock BigQuery client and query result
        mock_client = MagicMock()
        mock_bigquery_client.return_value = mock_client
        
        # Create mock user data
        mock_users_data = pd.DataFrame({
            'user_id': [1, 2, 3],
            'email': ['user1@test.com', 'user2@test.com', 'user3@test.com'],
            'first_name': ['John', 'Jane', 'Bob'],
            'last_name': ['Doe', 'Smith', 'Johnson'],
            'age': [25, 30, 35],
            'gender': ['M', 'F', 'M'],
            'state': ['CA', 'NY', 'TX'],
            'country': ['United States', 'United States', 'United States'],
            'registration_date': ['2022-01-01', '2022-02-01', '2022-03-01'],
            'acquisition_channel': ['organic', 'paid_search', 'social']
        })
        
        mock_client.query.return_value.to_dataframe.return_value = mock_users_data
        
        generator = SyntheticDataGenerator("test-project", "test_dataset")
        users_df = generator.get_thelook_users()
        
        # Verify query was executed
        mock_client.query.assert_called_once()
        
        # Verify returned data structure
        assert isinstance(users_df, pd.DataFrame)
        assert len(users_df) == 3
        assert 'user_id' in users_df.columns
        assert 'email' in users_df.columns
        assert 'registration_date' in users_df.columns
    
    @patch('generate_synthetic_data.bigquery.Client')
    def test_get_thelook_orders(self, mock_bigquery_client):
        """Test TheLook orders data retrieval"""
        mock_client = MagicMock()
        mock_bigquery_client.return_value = mock_client
        
        # Create mock order data
        mock_orders_data = pd.DataFrame({
            'order_id': [101, 102, 103],
            'user_id': [1, 2, 3],
            'status': ['Complete', 'Complete', 'Complete'],
            'order_date': ['2023-01-01', '2023-02-01', '2023-03-01'],
            'shipped_at': ['2023-01-02', '2023-02-02', '2023-03-02'],
            'delivered_at': ['2023-01-05', '2023-02-05', '2023-03-05'],
            'num_of_item': [1, 2, 1]
        })
        
        mock_client.query.return_value.to_dataframe.return_value = mock_orders_data
        
        generator = SyntheticDataGenerator("test-project", "test_dataset")
        orders_df = generator.get_thelook_orders()
        
        # Verify query was executed
        mock_client.query.assert_called_once()
        
        # Verify returned data structure
        assert isinstance(orders_df, pd.DataFrame)
        assert len(orders_df) == 3
        assert 'order_id' in orders_df.columns
        assert 'user_id' in orders_df.columns
        assert 'order_date' in orders_df.columns

class TestDataSampling:
    """Test data sampling functionality"""
    
    @patch('generate_synthetic_data.bigquery.Client')
    def test_data_sampling_percentage(self, mock_bigquery_client):
        """Test that data sampling works correctly"""
        mock_client = MagicMock()
        mock_bigquery_client.return_value = mock_client
        
        # Create larger mock dataset
        mock_users_data = pd.DataFrame({
            'user_id': range(1, 101),  # 100 users
            'email': [f'user{i}@test.com' for i in range(1, 101)],
            'registration_date': ['2022-01-01'] * 100,
            'acquisition_channel': ['organic'] * 100
        })
        
        mock_orders_data = pd.DataFrame({
            'order_id': range(1, 201),  # 200 orders
            'user_id': [i % 100 + 1 for i in range(200)],  # Distribute across users
            'status': ['Complete'] * 200,
            'order_date': ['2023-01-01'] * 200,
            'num_of_item': [1] * 200
        })
        
        # Mock the query responses
        def mock_query_side_effect(query):
            mock_result = MagicMock()
            if 'users' in query:
                mock_result.to_dataframe.return_value = mock_users_data
            else:  # orders query
                mock_result.to_dataframe.return_value = mock_orders_data
            return mock_result
        
        mock_client.query.side_effect = mock_query_side_effect
        
        generator = SyntheticDataGenerator("test-project", "test_dataset")
        
        # Test 10% sampling
        with patch.object(generator, '_upload_dataframe') as mock_upload:
            generator.generate_and_upload_all(sample_pct=10.0)
            
            # Should have been called 3 times (experiments, logistics, support)
            assert mock_upload.call_count == 3
            
            # Check that sampling actually happened by examining call arguments
            # The first call should be experiment assignments
            experiments_call = mock_upload.call_args_list[0]
            experiments_df = experiments_call[0][0]  # First argument (DataFrame)
            
            # Should have fewer users than original (10% sample)
            unique_users_in_experiments = experiments_df['user_id'].nunique()
            assert unique_users_in_experiments <= 15, f"Too many users in 10% sample: {unique_users_in_experiments}"

class TestSyntheticDataGeneration:
    """Test synthetic data generation components"""
    
    @patch('generate_synthetic_data.bigquery.Client')
    @patch('generate_synthetic_data.generate_experiment_assignments')
    @patch('generate_synthetic_data.generate_logistics_data')
    @patch('generate_synthetic_data.generate_support_tickets')
    def test_all_synthetic_data_generated(
        self, 
        mock_support_tickets, 
        mock_logistics_data, 
        mock_experiment_assignments,
        mock_bigquery_client
    ):
        """Test that all three synthetic datasets are generated"""
        
        # Setup mocks
        mock_client = MagicMock()
        mock_bigquery_client.return_value = mock_client
        
        users_df, orders_df = create_minimal_test_data()
        
        mock_client.query.return_value.to_dataframe.side_effect = [users_df, orders_df]
        
        # Mock synthetic data generation functions
        mock_experiment_assignments.return_value = pd.DataFrame({
            'user_id': [1, 2, 3],
            'experiment_name': ['test_exp'] * 3,
            'variant': ['A', 'B', 'A']
        })
        
        mock_logistics_data.return_value = pd.DataFrame({
            'order_id': [1, 2, 3],
            'warehouse_id': ['WH_NYC'] * 3,
            'shipping_cost': [10.0] * 3
        })
        
        mock_support_tickets.return_value = pd.DataFrame({
            'ticket_id': ['TICK_1', 'TICK_2'],
            'user_id': [1, 2],
            'issue_category': ['shipping_delay'] * 2
        })
        
        generator = SyntheticDataGenerator("test-project", "test_dataset")
        
        with patch.object(generator, '_upload_dataframe') as mock_upload:
            generator.generate_and_upload_all(sample_pct=100.0)
            
            # Verify all generation functions were called
            mock_experiment_assignments.assert_called_once()
            mock_logistics_data.assert_called_once()
            mock_support_tickets.assert_called_once()
            
            # Verify all uploads were attempted
            assert mock_upload.call_count == 3
            
            # Verify upload was called with correct table names
            upload_calls = mock_upload.call_args_list
            table_names = [call[0][1] for call in upload_calls]  # Second argument is table name
            
            expected_tables = ['experiment_assignments', 'logistics_data', 'support_tickets']
            assert set(table_names) == set(expected_tables)

class TestDataUpload:
    """Test data upload to BigQuery"""
    
    @patch('generate_synthetic_data.bigquery.Client')
    def test_upload_dataframe_success(self, mock_bigquery_client):
        """Test successful dataframe upload"""
        mock_client = MagicMock()
        mock_bigquery_client.return_value = mock_client
        
        # Mock successful job
        mock_job = MagicMock()
        mock_job.result.return_value = None  # Successful completion
        mock_client.load_table_from_dataframe.return_value = mock_job
        
        generator = SyntheticDataGenerator("test-project", "test_dataset")
        
        test_df = pd.DataFrame({
            'col1': [1, 2, 3],
            'col2': ['a', 'b', 'c']
        })
        
        # Should complete without error
        generator._upload_dataframe(test_df, "test_table")
        
        # Verify upload was attempted
        mock_client.load_table_from_dataframe.assert_called_once()
        
        # Verify job completion was awaited
        mock_job.result.assert_called_once()
    
    @patch('generate_synthetic_data.bigquery.Client')
    def test_upload_dataframe_failure(self, mock_bigquery_client):
        """Test handling of upload failure"""
        mock_client = MagicMock()
        mock_bigquery_client.return_value = mock_client
        
        # Mock failed job
        mock_job = MagicMock()
        mock_job.result.side_effect = Exception("Upload failed")
        mock_client.load_table_from_dataframe.return_value = mock_job
        
        generator = SyntheticDataGenerator("test-project", "test_dataset")
        
        test_df = pd.DataFrame({'col1': [1, 2, 3]})
        
        # Should raise exception on upload failure
        with pytest.raises(Exception, match="Upload failed"):
            generator._upload_dataframe(test_df, "test_table")

class TestEndToEndPipeline:
    """Test complete end-to-end pipeline"""
    
    @patch('generate_synthetic_data.bigquery.Client')
    def test_complete_pipeline_execution(self, mock_bigquery_client):
        """Test that complete pipeline executes without errors"""
        
        # Setup comprehensive mocks
        mock_client = MagicMock()
        mock_bigquery_client.return_value = mock_client
        
        # Mock TheLook data retrieval
        users_df, orders_df = create_minimal_test_data()
        
        def mock_query_side_effect(query):
            mock_result = MagicMock()
            if 'users' in query:
                mock_result.to_dataframe.return_value = users_df
            else:
                mock_result.to_dataframe.return_value = orders_df
            return mock_result
        
        mock_client.query.side_effect = mock_query_side_effect
        
        # Mock successful uploads
        mock_job = MagicMock()
        mock_job.result.return_value = None
        mock_client.load_table_from_dataframe.return_value = mock_job
        
        # Execute pipeline
        generator = SyntheticDataGenerator("test-project", "test_dataset")
        
        # Should complete without errors
        generator.generate_and_upload_all(sample_pct=100.0)
        
        # Verify all components were executed
        assert mock_client.query.call_count == 2  # Users and orders queries
        assert mock_client.load_table_from_dataframe.call_count == 3  # Three uploads

class TestDataQualityIntegration:
    """Test data quality across integrated components"""
    
    @patch('generate_synthetic_data.bigquery.Client')
    def test_user_id_consistency_across_datasets(self, mock_bigquery_client):
        """Test that user IDs are consistent across all generated datasets"""
        
        mock_client = MagicMock()
        mock_bigquery_client.return_value = mock_client
        
        # Create test data with known user IDs
        users_df = pd.DataFrame({
            'user_id': [1, 2, 3, 4, 5],
            'email': [f'user{i}@test.com' for i in range(1, 6)],
            'registration_date': ['2022-01-01'] * 5,
            'acquisition_channel': ['organic'] * 5
        })
        
        orders_df = pd.DataFrame({
            'order_id': [101, 102, 103],
            'user_id': [1, 2, 3],  # Subset of users have orders
            'status': ['Complete'] * 3,
            'order_date': ['2023-01-01'] * 3,
            'num_of_item': [1] * 3
        })
        
        def mock_query_side_effect(query):
            mock_result = MagicMock()
            if 'users' in query:
                mock_result.to_dataframe.return_value = users_df
            else:
                mock_result.to_dataframe.return_value = orders_df
            return mock_result
        
        mock_client.query.side_effect = mock_query_side_effect
        
        generator = SyntheticDataGenerator("test-project", "test_dataset")
        
        # Capture uploaded dataframes
        uploaded_dataframes = []
        
        def capture_upload(df, table_name):
            uploaded_dataframes.append((df.copy(), table_name))
            return MagicMock()
        
        with patch.object(generator, '_upload_dataframe', side_effect=capture_upload):
            generator.generate_and_upload_all(sample_pct=100.0)
        
        # Verify we have all three datasets
        assert len(uploaded_dataframes) == 3
        
        # Extract dataframes by table name
        datasets = {table_name: df for df, table_name in uploaded_dataframes}
        
        # Check user ID consistency
        experiment_users = set(datasets['experiment_assignments']['user_id'])
        logistics_users = set(datasets['logistics_data']['user_id'])
        support_users = set(datasets['support_tickets']['user_id'])
        original_users = set(users_df['user_id'])
        
        # All synthetic user IDs should be from original dataset
        assert experiment_users.issubset(original_users), "Experiment user IDs should be from original data"
        assert logistics_users.issubset(original_users), "Logistics user IDs should be from original data"
        assert support_users.issubset(original_users), "Support user IDs should be from original data"
    
    @patch('generate_synthetic_data.bigquery.Client')
    def test_order_id_consistency(self, mock_bigquery_client):
        """Test that order IDs are consistent between orders and logistics"""
        
        mock_client = MagicMock()
        mock_bigquery_client.return_value = mock_client
        
        users_df, orders_df = create_minimal_test_data()
        
        def mock_query_side_effect(query):
            mock_result = MagicMock()
            if 'users' in query:
                mock_result.to_dataframe.return_value = users_df
            else:
                mock_result.to_dataframe.return_value = orders_df
            return mock_result
        
        mock_client.query.side_effect = mock_query_side_effect
        
        generator = SyntheticDataGenerator("test-project", "test_dataset")
        
        uploaded_dataframes = []
        
        def capture_upload(df, table_name):
            uploaded_dataframes.append((df.copy(), table_name))
            return MagicMock()
        
        with patch.object(generator, '_upload_dataframe', side_effect=capture_upload):
            generator.generate_and_upload_all(sample_pct=100.0)
        
        # Extract logistics data
        datasets = {table_name: df for df, table_name in uploaded_dataframes}
        logistics_orders = set(datasets['logistics_data']['order_id'])
        original_orders = set(orders_df['order_id'])
        
        # All logistics order IDs should match original orders
        assert logistics_orders == original_orders, "Logistics should have data for all orders"

class TestErrorHandling:
    """Test error handling in integration scenarios"""
    
    @patch('generate_synthetic_data.bigquery.Client')
    def test_bigquery_connection_failure(self, mock_bigquery_client):
        """Test handling of BigQuery connection failure"""
        
        # Mock connection failure
        mock_bigquery_client.side_effect = Exception("Connection failed")
        
        # Should raise exception during initialization
        with pytest.raises(Exception, match="Connection failed"):
            SyntheticDataGenerator("test-project", "test_dataset")
    
    @patch('generate_synthetic_data.bigquery.Client')
    def test_query_failure_handling(self, mock_bigquery_client):
        """Test handling of query failures"""
        
        mock_client = MagicMock()
        mock_bigquery_client.return_value = mock_client
        
        # Mock query failure
        mock_client.query.side_effect = Exception("Query failed")
        
        generator = SyntheticDataGenerator("test-project", "test_dataset")
        
        # Should raise exception when trying to get data
        with pytest.raises(Exception, match="Query failed"):
            generator.get_thelook_users()

if __name__ == "__main__":
    # Run integration tests
    pytest.main([__file__, "-v"])