import logging
import pandas as pd


logger = logging.getLogger(__name__)


def resample_to_long_format(df):
    """
    Resamples data to 10-minute blocks, flattens column names,
    and pivots the result into a long format: [timestamp, name, value].
    """
    # Parse strings into datetime objects
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    resampled_df = df.resample('10min', on='timestamp').agg(['mean', 'min', 'max', 'std'])
    
    # Flatten MultiIndex columns
    resampled_df.columns = ['_'.join(col) for col in resampled_df.columns]
    resampled_df = resampled_df.reset_index() # bring timestamp back as a column
    
    # Pivot from wide to long format
    long_df = resampled_df.melt(
        id_vars=['timestamp'], 
        var_name='name', 
        value_name='value'
    )
    
    return long_df