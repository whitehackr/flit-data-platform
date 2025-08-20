
import pandas as pd
import numpy as np
from faker import Faker

fake = Faker()
Faker.seed(42)

def generate_logistics_data(orders_df: pd.DataFrame) -> pd.DataFrame:
    """Generate logistics and fulfillment data for orders"""
    
    logistics_data = []
    
    # Define realistic warehouse and shipping data
    warehouses = [
        {'id': 'WH_NYC', 'location': 'New York, NY', 'region': 'Northeast'},
        {'id': 'WH_LAX', 'location': 'Los Angeles, CA', 'region': 'West'},
        {'id': 'WH_CHI', 'location': 'Chicago, IL', 'region': 'Midwest'},
        {'id': 'WH_DFW', 'location': 'Dallas, TX', 'region': 'South'},
        {'id': 'WH_ATL', 'location': 'Atlanta, GA', 'region': 'Southeast'}
    ]
    
    carriers = ['FedEx', 'UPS', 'USPS', 'DHL', 'Amazon Logistics']
    package_types = ['envelope', 'small_box', 'medium_box', 'large_box', 'oversized']
    
    for _, order in orders_df.iterrows():
        order_id = order['order_id']
        order_date = pd.to_datetime(order['order_date'])
        num_items = order['num_of_item']
        
        # Select warehouse (could be based on user location in real scenario)
        warehouse = fake.random_element(warehouses)
        
        # Generate shipping details
        carrier = fake.random_element(carriers)
        
        # Package type based on number of items
        if num_items == 1:
            package_type = fake.random_element(['envelope', 'small_box'])
        elif num_items <= 3:
            package_type = fake.random_element(['small_box', 'medium_box'])
        elif num_items <= 6:
            package_type = fake.random_element(['medium_box', 'large_box'])
        else:
            package_type = 'oversized'
        
        # Shipping cost based on package type and carrier
        base_costs = {
            'envelope': 4.99,
            'small_box': 8.99,
            'medium_box': 12.99,
            'large_box': 18.99,
            'oversized': 29.99
        }
        
        carrier_multipliers = {
            'USPS': 0.8,
            'FedEx': 1.1,
            'UPS': 1.0,
            'DHL': 1.3,
            'Amazon Logistics': 0.9
        }
        
        shipping_cost = base_costs[package_type] * carrier_multipliers[carrier]
        shipping_cost = round(shipping_cost + fake.random.uniform(-2, 2), 2)
        
        # Delivery timing
        processing_days = fake.random_int(1, 3)
        transit_days = fake.random_int(1, 7)
        total_delivery_days = processing_days + transit_days
        
        logistics_data.append({
            'order_id': order_id,
            'user_id': order['user_id'],
            'warehouse_id': warehouse['id'],
            'warehouse_location': warehouse['location'],
            'warehouse_region': warehouse['region'],
            'shipping_carrier': carrier,
            'package_type': package_type,
            'shipping_cost': max(0, shipping_cost),  # Ensure non-negative
            'processing_days': processing_days,
            'transit_days': transit_days,
            'total_delivery_days': total_delivery_days,
            'tracking_number': fake.bothify('##??########'),
            'insurance_cost': round(fake.random.uniform(0, 5), 2),
            'is_expedited': fake.boolean(chance_of_getting_true=15),  # 15% chance of True
            'delivery_instructions': fake.random_element([
                None, 'Leave at door', 'Ring doorbell', 'Signature required'
            ])
        })
    
    return pd.DataFrame(logistics_data)