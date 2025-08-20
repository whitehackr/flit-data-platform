
import pandas as pd
from google.cloud import bigquery
import argparse
from datetime import datetime, timedelta
import logging

from experiment_assignments import generate_experiment_assignments
from logistics_data import generate_logistics_data  
from support_tickets import generate_support_tickets

class SyntheticDataGenerator:
    """Generate synthetic data overlays for TheLook dataset"""
    
    def __init__(self, project_id: str, dataset_id: str = "flit_raw"):
        self.client = bigquery.Client(project=project_id)
        self.project_id = project_id
        self.dataset_id = dataset_id
        
        # Ensure dataset exists
        self._create_dataset_if_not_exists()
        
    def _create_dataset_if_not_exists(self):
        """Create BigQuery dataset if it doesn't exist"""
        dataset_ref = f"{self.project_id}.{self.dataset_id}"
        
        try:
            self.client.get_dataset(dataset_ref)
            logging.info(f"Dataset {dataset_ref} already exists")
        except:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "US"
            dataset.description = "Raw synthetic data for Flit platform"
            
            self.client.create_dataset(dataset)
            logging.info(f"Created dataset {dataset_ref}")
    
    def get_thelook_users(self) -> pd.DataFrame:
        """Get all users from TheLook public dataset"""
        query = """
        SELECT 
            id as user_id,
            email,
            first_name,
            last_name,
            age,
            gender,
            state,
            country,
            created_at as registration_date,
            traffic_source as acquisition_channel
        FROM `bigquery-public-data.thelook_ecommerce.users`
        ORDER BY created_at
        """
        
        return self.client.query(query).to_dataframe()
    
    def get_thelook_orders(self) -> pd.DataFrame:
        """Get all orders from TheLook public dataset"""
        query = """
        SELECT 
            order_id,
            user_id,
            status,
            created_at as order_date,
            shipped_at,
            delivered_at,
            num_of_item
        FROM `bigquery-public-data.thelook_ecommerce.orders`
        WHERE status = 'Complete'
        ORDER BY created_at
        """
        
        return self.client.query(query).to_dataframe()
    
    def generate_and_upload_all(self, sample_pct: float = 100.0):
        """Generate all synthetic datasets and upload to BigQuery"""
        
        logging.info("Fetching TheLook base data...")
        users_df = self.get_thelook_users()
        orders_df = self.get_thelook_orders()
        
        # Sample data if requested (for faster development)
        if sample_pct < 100.0:
            sample_size = int(len(users_df) * sample_pct / 100)
            users_df = users_df.sample(n=sample_size, random_state=42)
            user_ids = set(users_df['user_id'])
            orders_df = orders_df[orders_df['user_id'].isin(user_ids)]
            logging.info(f"Sampled {sample_pct}% of data: {len(users_df)} users")
        
        # Generate synthetic datasets
        logging.info("Generating experiment assignments...")
        experiments_df = generate_experiment_assignments(users_df)
        self._upload_dataframe(experiments_df, "experiment_assignments")
        
        logging.info("Generating logistics data...")
        logistics_df = generate_logistics_data(orders_df)
        self._upload_dataframe(logistics_df, "logistics_data")
        
        logging.info("Generating support tickets...")
        support_df = generate_support_tickets(users_df, orders_df)
        self._upload_dataframe(support_df, "support_tickets")
        
        logging.info("✅ All synthetic data generated and uploaded!")
        
    def _upload_dataframe(self, df: pd.DataFrame, table_name: str):
        """Upload DataFrame to BigQuery table"""
        
        table_ref = f"{self.project_id}.{self.dataset_id}.{table_name}"
        
        # Configure load job
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",  # Replace existing data
            autodetect=True  # Auto-detect schema
        )
        
        # Upload data
        job = self.client.load_table_from_dataframe(
            df, table_ref, job_config=job_config
        )
        job.result()  # Wait for completion
        
        logging.info(f"✅ Uploaded {len(df)} rows to {table_ref}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate synthetic data for Flit")
    parser.add_argument("--project-id", required=True, help="GCP project ID")
    parser.add_argument("--sample-pct", type=float, default=100.0, 
                       help="Percentage of data to sample (for development)")
    parser.add_argument("--dataset", default="flit_raw", 
                       help="BigQuery dataset name")
    
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(level=logging.INFO, 
                       format='%(asctime)s - %(levelname)s - %(message)s')
    
    # Generate data
    generator = SyntheticDataGenerator(args.project_id, args.dataset)
    generator.generate_and_upload_all(args.sample_pct)