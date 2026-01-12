import argparse
import json
import logging
from src.extract import extract_date_data
from src.load import ensure_database_and_tables, load_data
from src.transform import resample_to_long_format

# Set up logging
logger = logging.getLogger('etl_logger')
logger.setLevel(logging.DEBUG) 

console_handler = logging.StreamHandler() # for the terminal
console_handler.setLevel(logging.INFO)

file_handler = logging.FileHandler('wind_etl.log') # for the log file
file_handler.setLevel(logging.DEBUG)

console_format = logging.Formatter('%(levelname)s: %(message)s')
file_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

console_handler.setFormatter(console_format)
file_handler.setFormatter(file_format)

logger.addHandler(console_handler)
logger.addHandler(file_handler)


if __name__ == "__main__":
    API_URL = "http://localhost:8000"

    with open('config.json', 'r') as f:
        DATABASE_CONFIG = json.load(f)

    user = DATABASE_CONFIG['username']
    passwd = DATABASE_CONFIG['password'] 
    host = DATABASE_CONFIG['host']
    port = DATABASE_CONFIG['port']

    parser = argparse.ArgumentParser(description="Aggregate wind data for a specific date.")
    parser.add_argument(
        "date", 
        type=str, 
        help="The date to extract in DD-MM-YYYY format"
    )

    args = parser.parse_args()
    df = extract_date_data(args.date, API_URL)
    long_df = resample_to_long_format(df)
    print(long_df)
    db_engine = ensure_database_and_tables(user, passwd, host, port)
    load_data(long_df, db_engine)
    