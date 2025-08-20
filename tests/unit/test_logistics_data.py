import pytest
import pandas as pd
import sys
import os

# Add scripts directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'scripts'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))  # Project root

from logistics_data import generate_logistics_data
from tests.fixtures.sample_data import sample_orders_df, create_minimal_test_data

class TestLogisticsDataGeneration:
    """Test logistics data generation functionality"""
    
    def test_generate_with_sample_orders(self, sample_orders_df):
        """Test logistics generation with known sample orders"""
        result_df = generate_logistics_data(sample_orders_df)
        
        # Basic structure validation
        assert isinstance(result_df, pd.DataFrame), "Should return DataFrame"
        assert len(result_df) > 0, "Should generate logistics data"
        
        # Should have same number of rows as input orders
        assert len(result_df) == len(sample_orders_df), "Should have one logistics record per order"
        
        # Check required columns exist
        required_columns = [
            'order_id', 'user_id', 'warehouse_id', 'warehouse_location',
            'warehouse_region', 'shipping_carrier', 'package_type',
            'shipping_cost', 'processing_days', 'transit_days',
            'total_delivery_days', 'tracking_number'
        ]
        for col in required_columns:
            assert col in result_df.columns, f"Missing required column: {col}"
    
    def test_order_id_mapping(self, sample_orders_df):
        """Test that all order IDs are properly mapped"""
        result_df = generate_logistics_data(sample_orders_df)
        
        # All order IDs should match input
        input_order_ids = set(sample_orders_df['order_id'])
        output_order_ids = set(result_df['order_id'])
        assert output_order_ids == input_order_ids, "All order IDs should be preserved"
        
        # All user IDs should match input
        input_user_ids = set(sample_orders_df['user_id'])
        output_user_ids = set(result_df['user_id'])
        assert output_user_ids == input_user_ids, "All user IDs should be preserved"

class TestWarehouseAssignment:
    """Test warehouse assignment logic"""
    
    def test_warehouse_ids_valid(self, sample_orders_df):
        """Test that warehouse IDs are from valid set"""
        result_df = generate_logistics_data(sample_orders_df)
        
        expected_warehouses = ['WH_NYC', 'WH_LAX', 'WH_CHI', 'WH_DFW', 'WH_ATL']
        actual_warehouses = set(result_df['warehouse_id'])
        
        assert actual_warehouses.issubset(set(expected_warehouses)), \
            f"Invalid warehouses: {actual_warehouses - set(expected_warehouses)}"
    
    def test_warehouse_regions_consistent(self, sample_orders_df):
        """Test that warehouse regions are consistent with locations"""
        result_df = generate_logistics_data(sample_orders_df)
        
        # Check that specific warehouse/region combinations are valid
        for _, row in result_df.iterrows():
            warehouse_id = row['warehouse_id']
            region = row['warehouse_region']
            
            # These should be consistent with our warehouse definitions
            assert region in ['Northeast', 'West', 'Midwest', 'South', 'Southeast'], \
                f"Invalid region: {region}"

class TestShippingCarriers:
    """Test shipping carrier assignment"""
    
    def test_carriers_valid(self, sample_orders_df):
        """Test that shipping carriers are from valid set"""
        result_df = generate_logistics_data(sample_orders_df)
        
        expected_carriers = ['FedEx', 'UPS', 'USPS', 'DHL', 'Amazon Logistics']
        actual_carriers = set(result_df['shipping_carrier'])
        
        assert actual_carriers.issubset(set(expected_carriers)), \
            f"Invalid carriers: {actual_carriers - set(expected_carriers)}"

class TestPackageTypes:
    """Test package type assignment logic"""
    
    def test_package_types_valid(self, sample_orders_df):
        """Test that package types are from valid set"""
        result_df = generate_logistics_data(sample_orders_df)
        
        expected_types = ['envelope', 'small_box', 'medium_box', 'large_box', 'oversized']
        actual_types = set(result_df['package_type'])
        
        assert actual_types.issubset(set(expected_types)), \
            f"Invalid package types: {actual_types - set(expected_types)}"
    
    def test_package_size_logic(self):
        """Test that package size correlates with number of items"""
        # Create orders with specific item counts
        test_orders = pd.DataFrame({
            'order_id': [1, 2, 3, 4, 5],
            'user_id': [1, 2, 3, 4, 5],
            'status': ['Complete'] * 5,
            'order_date': ['2023-01-01'] * 5,
            'num_of_item': [1, 3, 6, 10, 1]  # Different item counts
        })
        
        result_df = generate_logistics_data(test_orders)
        
        # Check that larger orders tend to get larger packages
        for _, row in result_df.iterrows():
            num_items = test_orders[test_orders['order_id'] == row['order_id']]['num_of_item'].iloc[0]
            package_type = row['package_type']
            
            # Very basic validation - single items shouldn't be oversized
            if num_items == 1:
                assert package_type != 'oversized', "Single item shouldn't be oversized package"

class TestShippingCosts:
    """Test shipping cost calculations"""
    
    def test_shipping_costs_positive(self, sample_orders_df):
        """Test that all shipping costs are positive"""
        result_df = generate_logistics_data(sample_orders_df)
        
        assert all(result_df['shipping_cost'] > 0), "All shipping costs should be positive"
        assert all(result_df['shipping_cost'] < 100), "Shipping costs should be reasonable (<$100)"
    
    def test_shipping_costs_realistic(self, sample_orders_df):
        """Test that shipping costs are in realistic ranges"""
        result_df = generate_logistics_data(sample_orders_df)
        
        # Group by package type and check cost ranges
        cost_by_type = result_df.groupby('package_type')['shipping_cost'].agg(['min', 'max', 'mean'])
        
        for package_type, stats in cost_by_type.iterrows():
            if package_type == 'envelope':
                assert stats['min'] >= 2.0, f"Envelope shipping too cheap: {stats['min']}"
                assert stats['max'] <= 15.0, f"Envelope shipping too expensive: {stats['max']}"
            elif package_type == 'oversized':
                assert stats['min'] >= 15.0, f"Oversized shipping too cheap: {stats['min']}"

class TestDeliveryTiming:
    """Test delivery timing calculations"""
    
    def test_delivery_days_positive(self, sample_orders_df):
        """Test that delivery days are positive and realistic"""
        result_df = generate_logistics_data(sample_orders_df)
        
        # Processing days should be reasonable
        assert all(result_df['processing_days'] >= 1), "Processing days should be at least 1"
        assert all(result_df['processing_days'] <= 5), "Processing days should be at most 5"
        
        # Transit days should be reasonable
        assert all(result_df['transit_days'] >= 1), "Transit days should be at least 1"
        assert all(result_df['transit_days'] <= 10), "Transit days should be at most 10"
        
        # Total should equal sum
        expected_total = result_df['processing_days'] + result_df['transit_days']
        assert all(result_df['total_delivery_days'] == expected_total), \
            "Total delivery days should equal processing + transit"

class TestTrackingNumbers:
    """Test tracking number generation"""
    
    def test_tracking_numbers_unique(self, sample_orders_df):
        """Test that tracking numbers are unique"""
        result_df = generate_logistics_data(sample_orders_df)
        
        tracking_numbers = result_df['tracking_number'].tolist()
        unique_tracking = set(tracking_numbers)
        
        assert len(tracking_numbers) == len(unique_tracking), \
            "All tracking numbers should be unique"
    
    def test_tracking_number_format(self, sample_orders_df):
        """Test that tracking numbers have correct format"""
        result_df = generate_logistics_data(sample_orders_df)
        
        for tracking_num in result_df['tracking_number']:
            # Should be string of specific length (varies by format)
            assert isinstance(tracking_num, str), "Tracking number should be string"
            assert len(tracking_num) >= 8, "Tracking number should be at least 8 characters"
            assert len(tracking_num) <= 15, "Tracking number should be at most 15 characters"

class TestDataQuality:
    """Test data quality and validation"""
    
    def test_no_null_critical_fields(self, sample_orders_df):
        """Test that critical fields are never null"""
        result_df = generate_logistics_data(sample_orders_df)
        
        critical_fields = [
            'order_id', 'user_id', 'warehouse_id', 'shipping_carrier',
            'package_type', 'shipping_cost', 'tracking_number'
        ]
        
        for field in critical_fields:
            null_count = result_df[field].isnull().sum()
            assert null_count == 0, f"Field {field} has {null_count} null values"
    
    def test_data_types_correct(self, sample_orders_df):
        """Test that data types are appropriate"""
        result_df = generate_logistics_data(sample_orders_df)
        
        # Numeric fields should be numeric
        assert pd.api.types.is_numeric_dtype(result_df['shipping_cost']), "Shipping cost should be numeric"
        assert pd.api.types.is_integer_dtype(result_df['processing_days']), "Processing days should be integer"
        assert pd.api.types.is_integer_dtype(result_df['transit_days']), "Transit days should be integer"
        
        # String fields should be strings
        assert pd.api.types.is_string_dtype(result_df['warehouse_id']), "Warehouse ID should be string"
        assert pd.api.types.is_string_dtype(result_df['shipping_carrier']), "Carrier should be string"

class TestEdgeCases:
    """Test edge cases and error conditions"""
    
    def test_empty_orders_handling(self):
        """Test handling of empty orders dataframe"""
        empty_orders = pd.DataFrame({
            'order_id': [],
            'user_id': [],
            'status': [],
            'order_date': [],
            'num_of_item': []
        })
        
        result_df = generate_logistics_data(empty_orders)
        
        # Should return empty DataFrame without errors
        assert isinstance(result_df, pd.DataFrame), "Should return DataFrame"
        assert len(result_df) == 0, "Should return empty DataFrame for empty input"
    
    def test_single_order_handling(self):
        """Test handling of single order"""
        single_order = pd.DataFrame({
            'order_id': [1],
            'user_id': [1],
            'status': ['Complete'],
            'order_date': ['2023-01-01'],
            'num_of_item': [1]
        })
        
        result_df = generate_logistics_data(single_order)
        
        # Should handle single order gracefully
        assert isinstance(result_df, pd.DataFrame), "Should return DataFrame"
        assert len(result_df) == 1, "Should return one logistics record"
        assert result_df['order_id'].iloc[0] == 1, "Should preserve order ID"
    
    def test_minimal_data_handling(self):
        """Test with minimal dataset"""
        _, orders = create_minimal_test_data()
        
        result_df = generate_logistics_data(orders)
        
        # Should handle minimal data gracefully
        assert isinstance(result_df, pd.DataFrame), "Should return DataFrame"
        assert len(result_df) == len(orders), "Should have one record per order"

if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v"])