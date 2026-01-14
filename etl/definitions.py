from dagster import (
    asset, AssetExecutionContext, AssetSelection,
    build_schedule_from_partitioned_job, ConfigurableResource, 
    DailyPartitionsDefinition, define_asset_job, Definitions, EnvVar
    )
import httpx
import logging
import os
from sqlalchemy.exc import IntegrityError
from src.extract import extract_date_data, date_to_params
from src.load import ensure_database, ensure_tables, load_data
from src.logging_config import setup_orchestration_logging
from src.transform import resample_10minute_blocks, pivot_to_long_format


# --- DEFINE RESOURCES ---
class APIResource(ConfigurableResource):
    api_url: str


class PostgresResource(ConfigurableResource):
    username: str
    password: str
    host: str
    port: int
    db_name: str

    def get_db_engine(self):
        ensure_database(self.username, self.password, self.host,
                        self.port, self.db_name)
        return ensure_tables(
            self.username, self.password, self.host, self.port, self.db_name
        )


# --- DEFINE PARTITIONS ---
daily_partitions = DailyPartitionsDefinition(start_date="2026-01-01")


# --- DEFINE ASSETS ---
@asset(partitions_def=daily_partitions)
def daily_etl_asset(context: AssetExecutionContext,
                    source_db_API: APIResource,
                    target_db: PostgresResource):
    """
    A combined asset that extracts, transforms, and loads data for a given date.
    """

    context.log.info("Starting ETL process...")
    logger = logging.getLogger(__name__)

    # Set up logging for the compute logs in dagster
    setup_orchestration_logging()

    # Date in YYYY-MM-DD format
    partition_date = context.partition_key 
    logger.debug(f"Parsed context arguments: {partition_date}")

    # Convert to DD-MM-YYYY
    formatted_date = f"{partition_date[8:10]}-{partition_date[5:7]}-{partition_date[0:4]}"
    params = date_to_params(formatted_date)

    context.log.info(f"Extracting wind and power data for date: {formatted_date}")
    
    # Extract
    context.log.info(f"Extracting data from {source_db_API.api_url}...")

    with httpx.Client(timeout=10.0) as client:
        response = client.get(f"{source_db_API.api_url}/health")
        if response.status_code == 200:
            context.log.debug("API and Database are healthy. Starting extraction...")
            raw_df = extract_date_data(params, source_db_API.api_url)
        else:
            context.log.error(f"API is up but unhealthy: {response.json()}")
            context.log.error("Extraction failed due to API or database health check failure.")
            raise Exception("API did not return 200.")
    
    if raw_df.empty:
        context.log.warning("No data found for the given date. Skipping next steps.")
        context.log.info(f"ETL completed for date: {formatted_date}")
        return None
    
    context.log.info(f"Success! Extracted {len(raw_df)} rows.")
    context.log.info(f"First rows of extracted data:\n{raw_df.head()}")

    # Transform
    context.log.info("Resampling data...")
    resampled_df = resample_10minute_blocks(raw_df)

    context.log.info("Transforming data to long format...")
    long_df = pivot_to_long_format(resampled_df)

    context.log.info(f"Transformation to long format complete. Sample of final data:\n{long_df.head()}")
    
    # Load
    context.log.info("Loading data to database...")
    
    try:
        engine = target_db.get_db_engine()
    except Exception as e:
        context.log.error("Critical Error during DB or tables check/creation.",
                    " Check if the credentials in config.json are from a super user.")
        raise Exception(e)

    try:
        final_df = load_data(long_df, engine)
        context.log.info(f"Successfully loaded {len(final_df)} rows into {target_db.db_name}.")
    except IntegrityError as e:
        # This block catches the specific "UniqueViolation" / Duplicate Key error
        context.log.info("This data is already present in the database ",
                         "(Duplicate Key). Upload skipped.")
        logger.debug(f"IntegrityError details:\n {e}")
    
    context.log.info(f"ETL completed for date: {formatted_date}")

    return None


# --- DEFINE JOB & SCHEDULE ---
etl_job = define_asset_job(
    name="daily_wind_power_etl_job",
    selection=AssetSelection.assets(daily_etl_asset)
)
etl_schedule = build_schedule_from_partitioned_job(
    job=etl_job,
    hour_of_day=2, 
    minute_of_hour=0,
    description="Runs at 2 AM daily, processing the previous day's data."
)

# --- FINAL DEFINITIONS ---

defs = Definitions(
    assets=[daily_etl_asset],
    schedules=[etl_schedule],
    resources={
        "source_db_API": APIResource(
            api_url=EnvVar("API_URL")
        ),
        "target_db": PostgresResource(
            username=EnvVar("TARGET_DB_USER"),
            password=EnvVar("TARGET_DB_PASSWORD"),
            host=EnvVar("TARGET_DB_HOST"),
            port=int(os.getenv("TARGET_DB_PORT")), # read port as int
            db_name=EnvVar("TARGET_DB")
        )
    }
)