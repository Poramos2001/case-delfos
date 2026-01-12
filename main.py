import argparse
import json
import logging
from logging_config import setup_logging
from src.extract import extract_date_data
from src.load import ensure_database_and_tables, load_data
from src.transform import resample_to_long_format
import sys


setup_logging()
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    logger.info("Starting ETL process...")

    API_URL = "http://localhost:8000"
    REQUIRED_KEYS = {'username', 'password', 'host', 'port'}

    # Get DB connection credentials
    with open('config.json', 'r') as f:
        DATABASE_CONFIG = json.load(f)

    missing_keys = REQUIRED_KEYS - set(DATABASE_CONFIG.keys())

    if missing_keys:
        logger.error(f"Missing required config fields: {', '.join(missing_keys)}")
        sys.exit(1)

    user = DATABASE_CONFIG['username']
    passwd = DATABASE_CONFIG['password'] 
    host = DATABASE_CONFIG['host']
    port = DATABASE_CONFIG['port']

    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Aggregate wind data for a specific date.")
    parser.add_argument(
        "date", 
        type=str, 
        help="The date to extract in DD-MM-YYYY format"
    )

    args = parser.parse_args()

    # ETL process
    df = extract_date_data(args.date, API_URL)

    long_df = resample_to_long_format(df)

    db_engine = ensure_database_and_tables(user, passwd, host, port)
    load_data(long_df, db_engine)
    
    logger.info("ETL process completed successfully.")
    