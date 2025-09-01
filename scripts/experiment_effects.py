import pandas as pd
import numpy as np
from google.cloud import bigquery
from faker import Faker
import random
import hashlib
from datetime import datetime, timedelta
from typing import Dict, List
from flit_experiment_configs import get_experiment_config
import logging

fake = Faker()
Faker.seed(42)  # Reproducible results

class ExperimentEffectsGenerator:
    """Generate synthetic behavioral overlay data to simulate experiment treatment effects"""
    
    def __init__(self, project_id: str, dataset_id: str = "flit_raw"):
        self.client = bigquery.Client(project=project_id)
        self.project_id = project_id
        self.dataset_id = dataset_id
    
    def generate_experiment_overlay(
        self,
        experiment_name: str,
        data_category: str,
        granularity: str,
        source_table_path: str,
        assignments_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Generate synthetic overlay data for experiment treatment effects
        
        Args:
            experiment_name: Name of experiment (e.g., 'free_shipping_threshold_test_v1_1_1')
            data_category: Type of data (e.g., 'orders', 'users', 'events')
            granularity: Primary key level (e.g., 'order_id', 'user_id', 'session_id')
            source_table_path: Full BigQuery path to copy schema from
            assignments_df: DataFrame with user assignments from experiment_assignments
        
        Returns:
            DataFrame with synthetic overlay records for treatment users only
        """
        
        # Load experiment configuration
        config = get_experiment_config(experiment_name)
        
        # Extract key parameters
        treatment_variant = config['variants']['treatment']['name']
        effect_size = config['power_analysis']['effect_size']['magnitude']
        baseline_rate = config['metrics']['primary']['baseline_assumptions']['historical_orders_per_user']
        
        # Extract temporal parameters
        start_date = pd.to_datetime(config['design']['temporal_schedule']['experiment_period_start'])
        end_date = pd.to_datetime(config['design']['temporal_schedule']['experiment_period_end'])
        duration_days = (end_date - start_date).days + 1
        
        # Filter to treatment users only
        treatment_assignments = assignments_df[
            assignments_df['variant'] == treatment_variant
        ].copy()
        
        if treatment_assignments.empty:
            logging.warning(f"No treatment users found for variant: {treatment_variant}")
            return pd.DataFrame()
        
        # Apply experimental sample size constraints
        daily_eligible_users = config['power_analysis']['traffic_analysis']['daily_eligible_users']
        total_experiment_users = daily_eligible_users * duration_days
        
        # Randomly sample from treatment users to match experimental design
        if len(treatment_assignments) > total_experiment_users // 2:  # Assuming 50/50 split
            treatment_sample_size = total_experiment_users // 2
            treatment_assignments = treatment_assignments.sample(n=treatment_sample_size, random_state=42)
            logging.info(f"Applied sample size constraint: {treatment_sample_size} treatment users (from {len(treatment_assignments)} available)")
        
        treatment_users = treatment_assignments['user_id'].tolist()
        logging.info(f"Generating overlay data for {len(treatment_users)} treatment users (constrained by experimental design)")
        
        # Get source table schema
        source_schema = self._get_table_schema(source_table_path)
        
        # Generate synthetic overlay records
        overlay_records = self._generate_overlay_records(
            treatment_users=treatment_users,
            effect_size=effect_size,
            baseline_rate=baseline_rate,
            start_date=start_date,
            duration_days=duration_days,
            granularity=granularity,
            source_schema=source_schema,
            experiment_name=experiment_name,
            treatment_variant=treatment_variant
        )
        
        # Upload to BigQuery
        table_name = f"synthetic_{experiment_name}_{data_category}"
        self._upload_overlay_data(overlay_records, table_name)
        
        return overlay_records
    
    def _get_table_schema(self, table_path: str) -> Dict:
        """Get schema information from source BigQuery table"""
        
        query = f"SELECT * FROM `{table_path}` LIMIT 1"
        sample_df = self.client.query(query).to_dataframe()
        
        schema_info = {}
        for col in sample_df.columns:
            schema_info[col] = {
                'dtype': sample_df[col].dtype,
                'sample_value': sample_df[col].iloc[0] if not pd.isna(sample_df[col].iloc[0]) else None
            }
        
        return schema_info
    
    def _generate_overlay_records(
        self,
        treatment_users: List[int],
        effect_size: float,
        baseline_rate: float,
        start_date: datetime,
        duration_days: int,
        granularity: str,
        source_schema: Dict,
        experiment_name: str,
        treatment_variant: str
    ) -> pd.DataFrame:
        """Generate synthetic overlay records for treatment users"""
        
        overlay_records = []
        
        # Calculate target rate with buffer (24% for 20% target)
        buffer_multiplier = 1.2  # 20% buffer above target effect
        target_rate = baseline_rate * (1 + effect_size * buffer_multiplier)
        additional_orders_per_user = target_rate - baseline_rate
        
        logging.info(f"Baseline rate: {baseline_rate:.4f}, Target rate: {target_rate:.4f}")
        logging.info(f"Additional orders per user: {additional_orders_per_user:.4f}")
        
        # Generate realistic daily distribution weights
        daily_weights = self._get_realistic_daily_weights(duration_days)
        
        for user_id in treatment_users:
            # Calculate total additional orders for this user over the entire experiment period
            total_additional_orders = np.random.poisson(additional_orders_per_user)
            
            if total_additional_orders == 0:
                continue
                
            # Distribute orders across days based on realistic patterns
            daily_orders = np.random.multinomial(total_additional_orders, daily_weights)
            
            # Generate records for each day
            for day_idx, num_orders in enumerate(daily_orders):
                if num_orders == 0:
                    continue
                    
                order_date = start_date + timedelta(days=day_idx)
                
                # Generate orders for this day
                for _ in range(num_orders):
                    record = self._create_synthetic_record(
                        user_id=user_id,
                        order_date=order_date,
                        granularity=granularity,
                        source_schema=source_schema,
                        experiment_name=experiment_name,
                        variant=treatment_variant
                    )
                    overlay_records.append(record)
        
        logging.info(f"Generated {len(overlay_records)} synthetic overlay records")
        return pd.DataFrame(overlay_records)
    
    def _get_realistic_daily_weights(self, duration_days: int) -> List[float]:
        """Generate realistic daily distribution weights (higher on weekends, etc.)"""
        
        weights = []
        for day in range(duration_days):
            # Simple pattern: slightly higher on weekends (days 5,6 of each week)
            day_of_week = day % 7
            if day_of_week in [5, 6]:  # Weekend
                weight = 1.2
            elif day_of_week == 0:  # Monday
                weight = 1.1
            else:  # Weekdays
                weight = 1.0
            weights.append(weight)
        
        # Normalize to sum to 1
        total_weight = sum(weights)
        return [w / total_weight for w in weights]
    
    def _create_synthetic_record(
        self,
        user_id: int,
        order_date: datetime,
        granularity: str,
        source_schema: Dict,
        experiment_name: str,
        variant: str
    ) -> Dict:
        """Create a single synthetic record matching source schema"""
        
        record = {}
        
        # Generate primary key (granularity field)
        if granularity == 'order_id':
            # Generate unique order ID (timestamp + user_id + random)
            timestamp_part = int(order_date.timestamp())
            random_part = random.randint(1000, 9999)
            record[granularity] = timestamp_part * 100000 + user_id % 10000 + random_part
        
        # Set user_id
        record['user_id'] = user_id
        
        # Handle datetime fields
        for field, schema_info in source_schema.items():
            if field in [granularity, 'user_id']:
                continue  # Already handled
                
            if 'datetime' in str(schema_info['dtype']).lower():
                if 'created' in field.lower() or 'order' in field.lower():
                    record[field] = order_date
                elif 'shipped' in field.lower():
                    # Shipped 1-3 days later
                    record[field] = order_date + timedelta(days=random.randint(1, 3))
                elif 'delivered' in field.lower():
                    # Delivered 3-7 days later
                    record[field] = order_date + timedelta(days=random.randint(3, 7))
                else:
                    record[field] = None
            elif schema_info['dtype'] == 'object':  # String fields
                if 'status' in field.lower():
                    record[field] = 'Complete'
                elif 'gender' in field.lower():
                    record[field] = random.choice(['M', 'F'])
                else:
                    record[field] = schema_info['sample_value']
            elif 'int' in str(schema_info['dtype']).lower():
                if 'num_of_item' in field.lower() or 'quantity' in field.lower():
                    record[field] = random.randint(1, 3)  # Most orders have 1-3 items
                else:
                    record[field] = schema_info['sample_value']
            else:
                record[field] = schema_info['sample_value']
        
        # Add experiment-specific columns
        record['experiment_name'] = experiment_name
        record['variant'] = variant
        
        return record
    
    def _upload_overlay_data(self, overlay_df: pd.DataFrame, table_name: str):
        """Upload overlay data to BigQuery using naming convention"""
        
        if overlay_df.empty:
            logging.warning(f"No overlay data to upload for table: {table_name}")
            return
        
        table_ref = f"{self.project_id}.{self.dataset_id}.{table_name}"
        
        # Convert DataFrame to records for JSON upload (avoids pyarrow issues)
        # Convert timestamps to strings for JSON serialization
        overlay_df_copy = overlay_df.copy()
        for col in overlay_df_copy.columns:
            if overlay_df_copy[col].dtype == 'datetime64[ns]':
                overlay_df_copy[col] = overlay_df_copy[col].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        records = overlay_df_copy.to_dict('records')
        
        # Configure load job for JSON format
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",  # Replace existing data
            autodetect=True,  # Auto-detect schema
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        )
        
        # Upload data using JSON format
        job = self.client.load_table_from_json(
            records, table_ref, job_config=job_config
        )
        job.result()  # Wait for completion
        
        logging.info(f"✅ Uploaded {len(overlay_df)} overlay records to {table_ref}")

def generate_free_shipping_threshold_overlay(
    project_id: str,
    users_df: pd.DataFrame,
    dataset_id: str = "flit_raw"
) -> pd.DataFrame:
    """Convenience function for free shipping threshold experiment overlay generation
    
    DEPRECATED: This function is no longer used. The main flow is now in 
    generate_synthetic_data.py which has consolidated assignment generation.
    """
    
    # NOTE: This is kept for backward compatibility but should not be used
    # The main flow is now: generate_synthetic_data.py -> SyntheticDataGenerator.generate_experiment_overlay()
    
    raise DeprecationWarning(
        "This function is deprecated. Use generate_synthetic_data.py overlay mode instead:\n"
        "python generate_synthetic_data.py --project-id=X overlay"
    )

# REMOVED: _enhance_assignments_schema() function
# This functionality has been consolidated into SyntheticDataGenerator._generate_free_shipping_threshold_assignments()
# in generate_synthetic_data.py for better cohesion and to eliminate the separate enhancement step.