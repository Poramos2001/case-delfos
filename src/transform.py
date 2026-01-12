import logging
import pandas as pd


logger = logging.getLogger(__name__)


def resample_10minute_blocks(df):
    """
    Resamples data to 10-minute blocks and computes mean, min, max, std and 
    flattens column names.
    """

    # Parse strings into datetime objects
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    resampled_df = df.resample('10min', on='timestamp').agg(['mean', 'min', 'max', 'std'])

    # Flatten MultiIndex columns
    resampled_df.columns = ['_'.join(col) for col in resampled_df.columns]
    resampled_df = resampled_df.reset_index() # bring timestamp back as a column
    logger.debug(f"Resampling complete. Sample of transformed data:\n{resampled_df.head()}")

    return resampled_df


def pivot_to_long_format(resampled_df):
    """
    Takes a resampled DataFrame and pivots the result into a long format: 
    [timestamp, name, value].
    """
    
    # Pivot from wide to long format
    long_df = resampled_df.melt(
        id_vars=['timestamp'], 
        var_name='name', 
        value_name='value'
    )
    return long_df