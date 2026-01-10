from datetime import datetime, time
import httpx
import logging
import pandas as pd
import sys


logger = logging.getLogger(__name__)


def check_api_health(client: httpx.Client, url: str):
    try:
        response = client.get(f"{url}/health")
        if response.status_code == 200:
            print("API and Database are healthy. Starting extraction...")
            return True
        else:
            print(f"API is up but unhealthy: {response.json()}")
            return False
    except httpx.RequestError as e:
        print(f"API is offline. When reaching {url}, found error: {e}")
        return False

    
def extract(target_date_str: str, api_url: str):

    # Parse string into datetime object
    try:
        target_date = datetime.strptime(target_date_str, "%d-%m-%Y")
    except ValueError:
        print(f"Error: Date '{target_date_str}' must be in DD-MM-YYYY format.")
        sys.exit(1)

    # From 00:00:00 to 23:59:59 of target date
    start_time = datetime.combine(target_date, time.min)
    end_time = datetime.combine(target_date, time.max)

    params = {
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat(),
        "cols": ["timestamp", "power", "wind_speed"] 
    }

    try:
        print(f"Extracting data from {api_url}...")
        
        with httpx.Client(timeout=10.0) as client:
            if not check_api_health(client, api_url):
                sys.exit(1)
            
            response = client.get(f"{api_url}/data", params=params)
            response.raise_for_status() #check for errors (4xx or 5xx)

        data = response.json()
        
        if not data:
            print("No data found for this time range.")
            sys.exit(1)
        else:
            df = pd.DataFrame(data)
            df["timestamp"] = pd.to_datetime(df["timestamp"]) #string to datetime objects
            
            print(f"Success! Extracted {len(df)} rows.")
            print(df.head())
            return df

    except httpx.HTTPStatusError as e:
        # Handles 4xx and 5xx errors specifically
        print(f"HTTP Error: {e.response.status_code} - {e.response.text}")
        sys.exit(1)
    except httpx.RequestError as e:
        # Handles connection issues (DNS, timeout, refused connection)
        print(f"Connection Error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        sys.exit(1)

if __name__ == "__main__":
    df = extract("02-01-2025","http://localhost:8000")
    df.sort_values("timestamp", inplace=True)
    print(df.head())
    print(df.tail())