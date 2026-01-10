import argparse
import logging





if __name__ == "__main__":
    API_URL = "http://localhost:8000"

    parser = argparse.ArgumentParser(description="Aggregate wind data for a specific date.")
    parser.add_argument(
        "date", 
        type=str, 
        help="The date to extract in DD-MM-YYYY format"
    )

    args = parser.parse_args()
    # run_extraction(args.date)