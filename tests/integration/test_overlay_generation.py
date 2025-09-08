#!/usr/bin/env python3
"""
Test script for synthetic overlay data generation
"""

import os
import sys
from pathlib import Path

# Add scripts directory to path
scripts_dir = Path(__file__).parent / "scripts"
sys.path.insert(0, str(scripts_dir))

from generate_synthetic_data import SyntheticDataGenerator

def test_overlay_generation():
    """Test synthetic overlay data generation for free shipping threshold experiment"""
    
    print("🧪 Testing synthetic overlay data generation...")
    
    # Initialize generator (using a test project - you'll need to replace with real project)
    project_id = "your-test-project"  # Replace with actual project ID
    generator = SyntheticDataGenerator(project_id=project_id, dataset_id="flit_raw_test")
    
    try:
        # Test overlay generation
        print("\n📊 Generating overlay data for free_shipping_threshold_test_v1_1_1...")
        overlay_df = generator.generate_experiment_overlay(
            experiment_name="free_shipping_threshold_test_v1_1_1",
            data_category="orders",
            granularity="order_id",
            source_table_path="bigquery-public-data.thelook_ecommerce.orders"
        )
        
        print(f"✅ Successfully generated {len(overlay_df)} synthetic overlay records")
        print(f"📋 Columns: {list(overlay_df.columns)}")
        
        if len(overlay_df) > 0:
            print(f"🎯 Sample record:\n{overlay_df.head(1).to_dict('records')[0]}")
            
            # Check if table naming follows convention
            table_name = f"synthetic_free_shipping_threshold_test_v1_1_1_orders"
            print(f"📝 Expected table name: {table_name}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error during overlay generation: {str(e)}")
        return False

if __name__ == "__main__":
    success = test_overlay_generation()
    if success:
        print("\n🎉 All tests passed!")
    else:
        print("\n💥 Tests failed!")
        sys.exit(1)