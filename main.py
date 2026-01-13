import argparse
import httpx
import json
import logging
from logging_config import setup_script_logging
import os
from sqlalchemy.exc import IntegrityError
from src.extract import check_api_health, date_to_params, extract_date_data
from src.load import ensure_database, ensure_tables, load_data
from src.transform import resample_10minute_blocks, pivot_to_long_format
import sys


def error_handling_extraction(params):
    """
    Extracts data with error handling for HTTP requests.
    """
    try:
        df = extract_date_data(params, API_URL)

        if df.empty:
            logger.warning("No data found for the given date.")
            sys.exit(0)  # Exit gracefully if no data
        else:
            return df

    except httpx.HTTPStatusError as e:
        # Handles 4xx and 5xx errors specifically
        logger.error(f"HTTP Error: {e.response.status_code} - {e.response.text}")
        sys.exit(1)
    except httpx.RequestError as e:
        # Handles connection issues (DNS, timeout, refused connection)
        logger.error(f"Connection Error: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Data extraction failed: {e}")
        sys.exit(1)


setup_script_logging()
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    logger.info("Starting ETL process...")

    API_URL = "http://localhost:8000"
    REQUIRED_KEYS = {'username', 'password', 'host', 'port'}

    # Get DB connection credentials
    with open('config.json', 'r') as f:
        DATABASE_CONFIG = json.load(f)

    logger.debug(f"Database config loaded: {DATABASE_CONFIG.keys()}")

    missing_keys = REQUIRED_KEYS - set(DATABASE_CONFIG.keys())

    if missing_keys:
        logger.error(f"Missing required config fields: {', '.join(missing_keys)}")
        sys.exit(1)

    user = os.getenv("PG_USER")
    passwd = os.getenv("PG_PASSWORD")
    host = os.getenv("PG_HOST")
    port = int(os.getenv("PG_PORT"))

    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Aggregate wind data for a specific date.")
    parser.add_argument(
        "date", 
        type=str, 
        help="The date to extract in DD-MM-YYYY format"
    )

    args = parser.parse_args()
    logger.debug(f"Parsed command-line arguments: {args}")
    
    # Validate date format
    try:
        params = date_to_params(args.date)
    except ValueError:
        logger.error(f"Error: Date '{args.date}' must be in DD-MM-YYYY format.")
        sys.exit(1)

    # -- ETL process --

    # Extract
    logger.info(f"Extracting data from {API_URL}...")

    if not check_api_health(httpx.Client(timeout=10.0), API_URL):
        sys.exit(1)

    df = error_handling_extraction(params)
    
    logger.info(f"Success! Extracted {len(df)} rows.")
    logger.info(f"First rows of extracted data:\n{df.head()}")

    # Transform
    logger.info("Transforming data to long format with 10-minute resampling...")

    try:
        resampled_df = resample_10minute_blocks(df)
    except Exception as e:
        logger.error(f"Encountered an error during resampling:\n {e}")
        sys.exit(1)
    
    try:
        long_df = pivot_to_long_format(resampled_df)
    except Exception as e:
        logger.error(f"Encountered an error during pivoting to long format:\n {e}")
        sys.exit(1)
    
    logger.info(f"Transformation to long format complete. Sample of final data:\n{long_df.head()}")

    # Load
    try:
        ensure_database(user, passwd, host, port)
    except Exception as e:
        logger.error("Critical Error during DB check/creation.",
                     " Check if the credentials in config.json are from a super user.")
        logger.debug(f"Error details: {e}")
        sys.exit(1)
    
    try:
        db_engine = ensure_tables(user, passwd, host, port)
    except Exception as e:
        logger.error(f"Error creating schema: {e}")
        sys.exit(1)
    
    logger.info("Loading data into the database...")

    try:
        final_df = load_data(long_df, db_engine)
        logger.info(f"Successfully loaded {len(final_df)} rows into 'delfos-target'.")
    except IntegrityError as e:
        # This block catches the specific "UniqueViolation" / Duplicate Key error
        logger.warning("This data is already present in the database (Duplicate Key). Upload skipped.")
        logger.debug(f"IntegrityError details:\n {e}")
    except Exception as e:
        logger.error(f"Error during bulk load: {e}")
        sys.exit(1)
    
    logger.info("ETL process completed successfully.")
    