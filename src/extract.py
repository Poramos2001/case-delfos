from datetime import datetime, time, timezone
import httpx
import logging
import pandas as pd


logger = logging.getLogger(__name__)


def check_api_health(client: httpx.Client, url: str):
    try:
        response = client.get(f"{url}/health")
        if response.status_code == 200:
            logger.debug("API and Database are healthy. Starting extraction...")
            return True
        else:
            logger.error(f"API is up but unhealthy: {response.json()}")
            return False
    except httpx.RequestError as e:
        logger.error(f"API is offline. When reaching {url}, found error: {e}")
        return False


def date_to_params(target_date_str: str):
    """
    Converts a date string in DD-MM-YYYY format to API query parameters.
    Parameters:
        target_date_str (str): Date string in DD-MM-YYYY format.
    Returns:
        dict: Dictionary with 'start_time', 'end_time', and 'cols'.
    """
    # Parse string into datetime object
    target_date = datetime.strptime(target_date_str, "%d-%m-%Y")

    # From 00:00:00 to 23:59:59 of target date
    start_time = datetime.combine(target_date, time.min, tzinfo=timezone.utc)
    end_time = datetime.combine(target_date, time.max, tzinfo=timezone.utc)

    params = {
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat(),
        "cols": ["timestamp", "power", "wind_speed"] 
    }

    return params
    
def extract_date_data(query_params: dict, api_url: str):
    """
    Extracts wind and power data from the API for a specific date.
    Parameters:
        query_params (dict): Dictionary with 'start_time', 'end_time', and 'cols'.
        api_url (str): Base URL of the API.
    Returns:
        pd.DataFrame: DataFrame containing the extracted data.
    """
 
    with httpx.Client(timeout=10.0) as client: 
        response = client.get(f"{api_url}/data", params=query_params)
        response.raise_for_status() # check for errors (4xx or 5xx)

    data = response.json()
    
    if not data:
        return pd.DataFrame()  # Return empty DataFrame if no data
    else:
        return pd.DataFrame(data)


if __name__ == "__main__":
    params = date_to_params("02-01-2025")
    df = extract_date_data(params,"http://localhost:8000")
    df.sort_values("timestamp", inplace=True)
    print(df.head())
    print(df.tail())