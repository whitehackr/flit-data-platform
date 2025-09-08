
# =============================================================================
# DEPRECATED - SPRINT 1 ONLY
# =============================================================================  
# This script was used in Sprint 1 for full synthetic data generation.
# In Sprint 2, we moved to overlay-only approach where assignments are created
# but not persisted to BigQuery. The assignments are only used internally 
# to filter treatment users for synthetic order generation.
# 
# For Sprint 2 flow, see experiment_effects.py which handles both assignment 
# creation and overlay data upload.
# =============================================================================

import pandas as pd
import numpy as np
from faker import Faker
import hashlib
from typing import Dict, List
from flit_experiment_configs import get_experiment_config

fake = Faker()
Faker.seed(42)  # Reproducible results

def generate_experiment_assignments(users_df: pd.DataFrame) -> pd.DataFrame:
    """Generate A/B test assignments for all users"""
    
    # Define realistic e-commerce experiments
    experiments_config = {
        'checkout_button_color': {
            'variants': ['blue_button', 'green_button', 'orange_button'],
            'allocation': [0.33, 0.33, 0.34],
            'start_date': '2023-01-15',
            'end_date': '2023-12-31',
            'description': 'Testing checkout button color impact on conversion',
            'success_metric': 'conversion_rate'
        },
        'free_shipping_threshold': {
            'variants': ['threshold_50', 'threshold_75', 'threshold_100'],
            'allocation': [0.33, 0.33, 0.34],
            'start_date': '2023-02-01', 
            'end_date': '2023-12-31',
            'description': 'Testing free shipping threshold on average order value',
            'success_metric': 'average_order_value'
        },
        'product_recommendations': {
            'variants': ['collaborative_filtering', 'content_based', 'hybrid'],
            'allocation': [0.33, 0.33, 0.34],
            'start_date': '2023-03-01',
            'end_date': '2023-12-31', 
            'description': 'Testing recommendation algorithm effectiveness',
            'success_metric': 'click_through_rate'
        },
        'email_frequency': {
            'variants': ['weekly', 'biweekly', 'monthly'],
            'allocation': [0.33, 0.33, 0.34],
            'start_date': '2023-04-01',
            'end_date': '2023-12-31',
            'description': 'Testing email marketing frequency impact on engagement',
            'success_metric': 'email_engagement_rate'
        }
    }
    
    assignments = []
    
    for _, user in users_df.iterrows():
        user_id = user['user_id']
        registration_date = pd.to_datetime(user['registration_date'])
        
        for exp_name, config in experiments_config.items():
            exp_start = pd.to_datetime(config['start_date'])
            
            # Only assign users who registered before experiment start
            if registration_date.date() <= pd.to_datetime(exp_start).date():
                variant = assign_variant_deterministic(
                    user_id, exp_name, config['variants'], config['allocation']
                )
                
                assignments.append({
                    'user_id': user_id,
                    'experiment_name': exp_name,
                    'variant': variant,
                    'assigned_date': config['start_date'],
                    'experiment_start_date': config['start_date'],
                    'experiment_end_date': config['end_date'],
                    'experiment_description': config['description'],
                    'success_metric': config['success_metric'],
                    'assignment_method': 'deterministic_hash'
                })
    
    return pd.DataFrame(assignments)

def assign_variant_deterministic(
    user_id: int, 
    experiment_name: str, 
    variants: List[str], 
    allocation: List[float]
) -> str:
    """Deterministically assign user to experiment variant"""
    
    # Create deterministic hash
    hash_input = f"{user_id}_{experiment_name}"
    hash_value = int(hashlib.md5(hash_input.encode()).hexdigest(), 16)
    
    # Convert to probability between 0 and 1
    probability = (hash_value % 1000000) / 1000000
    
    # Assign based on allocation
    cumulative_prob = 0
    for i, (variant, alloc) in enumerate(zip(variants, allocation)):
        cumulative_prob += alloc
        if probability <= cumulative_prob:
            return variant
    
    # Fallback (shouldn't reach here)
    return variants[0]

def generate_free_shipping_threshold_assignments(users_df: pd.DataFrame = None) -> pd.DataFrame:
    """Generate assignments for free_shipping_threshold_test_v1_1_1 experiment
    
    Assigns only users who actually placed orders during the experiment period.
    This implements 'live assignment' approach for realistic experiment conditions.
    """
    from google.cloud import bigquery
    
    # Load experiment config
    config = get_experiment_config('free_shipping_threshold_test_v1_1_1')
    
    # Extract experiment parameters using config variables
    exp_name = config['design']['experiment_name']
    start_date = config['design']['temporal_schedule']['experiment_period_start']
    end_date = config['design']['temporal_schedule']['experiment_period_end']
    
    # Extract variant information
    control_variant = config['variants']['control']['name']
    treatment_variant = config['variants']['treatment']['name']
    control_allocation = config['variants']['control']['allocation']
    treatment_allocation = config['variants']['treatment']['allocation']
    
    variants = [control_variant, treatment_variant]
    allocations = [control_allocation, treatment_allocation]
    
    # Get users who actually placed orders during experiment period
    client = bigquery.Client()
    
    # Use config variables for dates instead of hardcoding
    participants_query = f"""
    SELECT DISTINCT user_id
    FROM `bigquery-public-data.thelook_ecommerce.orders`
    WHERE created_at >= '{start_date}'
      AND created_at < '{end_date}'
    ORDER BY user_id
    """
    
    experiment_participants_df = client.query(participants_query).to_dataframe()
    
    assignments = []
    
    # Assign only users who actually participated during experiment period
    for _, row in experiment_participants_df.iterrows():
        user_id = row['user_id']
        
        # Assign variant using deterministic hash
        variant = assign_variant_deterministic(user_id, exp_name, variants, allocations)
        
        assignments.append({
            'user_id': user_id,
            'experiment_name': exp_name,
            'variant': variant,
            'assigned_date': start_date,
            'experiment_start_date': start_date,
            'experiment_end_date': end_date,
            'assignment_method': 'deterministic_hash'
        })
    
    return pd.DataFrame(assignments)