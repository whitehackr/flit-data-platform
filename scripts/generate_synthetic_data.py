
import pandas as pd
from google.cloud import bigquery
import argparse
from datetime import datetime, timedelta
import logging

from experiment_assignments import generate_experiment_assignments, generate_free_shipping_threshold_assignments
from logistics_data import generate_logistics_data  
from support_tickets import generate_support_tickets
from experiment_effects import ExperimentEffectsGenerator

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
    
    def generate_experiment_overlay(self, experiment_name: str, data_category: str = "orders", 
                                  granularity: str = "order_id", 
                                  source_table_path: str = "bigquery-public-data.thelook_ecommerce.orders"):
        """Generate synthetic overlay data for any experiment"""
        
        logging.info(f"Generating overlay data for experiment: {experiment_name}")
        
        # Get base data for assignments
        users_df = self.get_thelook_users()
        
        # Generate experiment assignments (currently only supports free shipping threshold)
        # TODO: Make this truly generic when we add more experiments
        if experiment_name == "free_shipping_threshold_test_v1_1_1":
            assignments_df = generate_free_shipping_threshold_assignments(users_df)
        else:
            raise NotImplementedError(f"Assignment generation not implemented for experiment: {experiment_name}")
        
        # Initialize effects generator
        effects_generator = ExperimentEffectsGenerator(
            project_id=self.project_id,
            dataset_id=self.dataset_id
        )
        
        # Generate synthetic overlay using the experiment config
        overlay_df = effects_generator.generate_experiment_overlay(
            experiment_name=experiment_name,
            data_category=data_category,
            granularity=granularity,
            source_table_path=source_table_path,
            assignments_df=assignments_df
        )
        
        logging.info(f"Generated {len(overlay_df)} synthetic overlay records for {experiment_name}")
        logging.info("✅ Experiment overlay generated successfully!")
        
        return overlay_df
    
    def generate_production_overlays(self, experiments: list = None):
        """Generate and upload experiment overlays for production use
        
        Args:
            experiments: List of experiment names to generate. If None, uses default experiments.
        """
        
        if experiments is None:
            experiments = ["free_shipping_threshold_test_v1_1_1"]  # Default production experiment
        
        logging.info(f"🚀 Starting production overlay generation for {len(experiments)} experiment(s)")
        
        results = []
        
        for experiment_name in experiments:
            try:
                logging.info(f"📊 Generating overlay for: {experiment_name}")
                
                # Generate overlay data
                overlay_df = self.generate_experiment_overlay(
                    experiment_name=experiment_name,
                    data_category="orders",
                    granularity="order_id", 
                    source_table_path="bigquery-public-data.thelook_ecommerce.orders"
                )
                
                # Production table naming convention
                table_name = f"synthetic_{experiment_name}_orders"
                
                # Upload to production dataset (flit_raw, not flit_raw_test)
                self._upload_dataframe(overlay_df, table_name)
                
                # Collect results
                result = {
                    "experiment_name": experiment_name,
                    "table_name": table_name,
                    "rows_created": len(overlay_df),
                    "unique_users": overlay_df['user_id'].nunique() if len(overlay_df) > 0 else 0,
                    "status": "success"
                }
                
                results.append(result)
                logging.info(f"✅ {experiment_name}: {result['rows_created']} rows → {table_name}")
                
            except Exception as e:
                error_result = {
                    "experiment_name": experiment_name,
                    "status": "failed",
                    "error": str(e)
                }
                results.append(error_result)
                logging.error(f"❌ {experiment_name} failed: {str(e)}")
        
        # Summary
        successful = [r for r in results if r.get("status") == "success"]
        failed = [r for r in results if r.get("status") == "failed"]
        
        logging.info(f"\n🎉 Production overlay generation complete!")
        logging.info(f"✅ Successful: {len(successful)}")
        logging.info(f"❌ Failed: {len(failed)}")
        
        if successful:
            total_rows = sum(r["rows_created"] for r in successful)
            logging.info(f"📊 Total rows created: {total_rows}")
        
        return results
        
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
    parser = argparse.ArgumentParser(description="Generate synthetic data for Flit experiments")
    parser.add_argument("--project-id", required=True, help="GCP project ID")
    parser.add_argument("--dataset", default="flit_raw", help="BigQuery dataset name")
    
    # Mode selection
    subparsers = parser.add_subparsers(dest="mode", help="Generation mode")
    
    # Legacy mode (backward compatibility)
    legacy_parser = subparsers.add_parser("legacy", help="Generate legacy synthetic data")
    legacy_parser.add_argument("--sample-pct", type=float, default=100.0,
                              help="Percentage of data to sample (for development)")
    
    # Production overlay mode (new)
    overlay_parser = subparsers.add_parser("overlay", help="Generate experiment overlay data for production")
    overlay_parser.add_argument("--experiments", nargs="+", 
                                default=["free_shipping_threshold_test_v1_1_1"],
                                help="List of experiment names to generate overlays for")
    
    # Single experiment mode (convenience)
    single_parser = subparsers.add_parser("single", help="Generate single experiment overlay")
    single_parser.add_argument("--experiment", required=True, help="Experiment name")
    single_parser.add_argument("--data-category", default="orders", help="Data category (orders, users, etc.)")
    single_parser.add_argument("--granularity", default="order_id", help="Record granularity (order_id, user_id, etc.)")
    single_parser.add_argument("--source-table", default="bigquery-public-data.thelook_ecommerce.orders",
                              help="Source table for schema matching")
    
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(level=logging.INFO, 
                       format='%(asctime)s - %(levelname)s - %(message)s')
    
    # Initialize generator
    generator = SyntheticDataGenerator(args.project_id, args.dataset)
    
    # Execute based on mode
    if args.mode == "legacy":
        logging.info("🔄 Running legacy synthetic data generation...")
        generator.generate_and_upload_all(args.sample_pct)
        
    elif args.mode == "overlay":
        logging.info("🚀 Running production overlay generation...")
        results = generator.generate_production_overlays(args.experiments)
        
        # Print summary
        print("\n" + "="*60)
        print("PRODUCTION OVERLAY GENERATION SUMMARY")
        print("="*60)
        for result in results:
            if result.get("status") == "success":
                print(f"✅ {result['experiment_name']}: {result['rows_created']} rows → {result['table_name']}")
            else:
                print(f"❌ {result['experiment_name']}: {result['error']}")
        print("="*60)
        
    elif args.mode == "single":
        logging.info(f"🎯 Running single experiment overlay generation for: {args.experiment}")
        overlay_df = generator.generate_experiment_overlay(
            experiment_name=args.experiment,
            data_category=args.data_category,
            granularity=args.granularity,
            source_table_path=args.source_table
        )
        
        table_name = f"synthetic_{args.experiment}_{args.data_category}"
        generator._upload_dataframe(overlay_df, table_name)
        
        print(f"\n✅ Generated {len(overlay_df)} rows → {table_name}")
        
    else:
        parser.print_help()
        print("\nExample usage:")
        print("  # Legacy mode (backward compatibility)")
        print("  python generate_synthetic_data.py --project-id=my-project legacy --sample-pct=10")
        print("")
        print("  # Production overlay mode (recommended)")
        print("  python generate_synthetic_data.py --project-id=my-project overlay")
        print("")
        print("  # Single experiment mode")
        print("  python generate_synthetic_data.py --project-id=my-project single --experiment=free_shipping_threshold_test_v1_1_1")