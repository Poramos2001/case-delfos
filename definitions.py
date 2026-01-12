import json
from dagster import (
    asset, Definitions, ConfigurableResource, 
    DailyPartitionsDefinition, AssetExecutionContext,
    define_asset_job, ScheduleDefinition
)

# --- 1. IMPORT YOUR EXISTING LOGIC ---
# We import functions just like you did in main.py
from src.extract import extract_date_data
from src.transform import resample_to_long_format
from src.load import load_data, ensure_database_and_tables
from logging_config import setup_orchestration_logging

# --- 2. DEFINE RESOURCES (Replaces config.json) ---
class APIResource(ConfigurableResource):
    api_url: str


class PostgresResource(ConfigurableResource):
    username: str
    password: str
    host: str
    port: int
    db_name: str

    def get_db_engine(self):
        return ensure_database_and_tables(
            self.username, self.password, self.host, self.port, self.db_name
        )

# --- 3. DEFINE PARTITIONS (Replaces argparse) ---
# This creates a list of dates starting from Jan 1, 2025
daily_partitions = DailyPartitionsDefinition(start_date="2025-01-01")

# --- 4. DEFINE ASSETS (Replaces the main execution flow) ---
@asset(partitions_def=daily_partitions)
def daily_etl_asset(context: AssetExecutionContext,
                    source_db_API: APIResource,
                    target_db: PostgresResource):
    """
    A combined asset that extracts, transforms, and loads data for a given date.
    """

    context.log.info("Starting ETL process...")

    # Set up logging for the compute logs in dagster
    setup_orchestration_logging()

    # Date in YYYY-MM-DD format
    partition_date = context.partition_key 
    # Convert to DD-MM-YYYY
    formatted_date = f"{partition_date[8:10]}-{partition_date[5:7]}-{partition_date[0:4]}"
    
    context.log.info(f"Extracting wind and power data for date: {formatted_date}")
    
    # Extract
    raw_df = extract_date_data(formatted_date, source_db_API.api_url)
    
    if raw_df.empty:
        context.log.warning("No data found for the given date. Skipping next steps.")
        context.log.info(f"ETL completed for date: {formatted_date}")
        return None
    else:
        context.log.info(f"Success! Extracted {len(raw_df)} rows.")
        context.log.info(f"First rows of extracted data:\n{raw_df.head()}")
        # Transform
        context.log.info("Resampling data...")
        long_df = resample_to_long_format(raw_df)
        
        # Load
        context.log.info("Loading data to database...")
        
        engine = target_db.get_db_engine()
        
        load_data(long_df, engine)
        
        context.log.info("Load complete.")
        context.log.info(f"ETL completed for date: {formatted_date}")

        # We return None as the data is now in the DB
        return None

# --- 4. Define Job & Schedule ---

# A job that targets our specific asset
etl_job = define_asset_job(
    name="daily_etl_job",
    selection="data_transfer_asset"
)

# A schedule that runs the job every day at midnight
etl_schedule = ScheduleDefinition(
    job=etl_job,
    cron_schedule="0 0 * * *", # Run at 00:00 daily
)

# --- 5. BIND IT ALL TOGETHER ---

# Load config once (optional, or pass via env vars)
with open('config.json', 'r') as f:
    config_data = json.load(f)

defs = Definitions(
    assets=[daily_etl_asset],
    schedules=[etl_schedule],
    resources={
        "source_db_API": APIResource(
            api_url="http://localhost:8000"
        ),
        "target_db": PostgresResource(
            username=config_data['username'],
            password=config_data['password'],
            host=config_data['host'],
            port=config_data['port'],
            db_name="delfos-target"
        )
    }
)