import logging
import pandas as pd
import sys


logger = logging.getLogger(__name__)


def resample_to_long_format(df):
    """
    Resamples data to 10-minute blocks, flattens column names,
    and pivots the result into a long format: [timestamp, name, value].
    """
    logger.info("Transforming data to long format with 10-minute resampling...")
    
    try:
        # Parse strings into datetime objects
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        resampled_df = df.resample('10min', on='timestamp').agg(['mean', 'min', 'max', 'std'])
    except Exception as e:
        logger.error(f"Encountered an error during resampling:\n {e}")
        sys.exit(1)

    # Flatten MultiIndex columns
    resampled_df.columns = ['_'.join(col) for col in resampled_df.columns]
    resampled_df = resampled_df.reset_index() # bring timestamp back as a column
    logger.debug(f"Resampling complete. Sample of transformed data:\n{resampled_df.head()}")
    
    try:
        # Pivot from wide to long format
        long_df = resampled_df.melt(
            id_vars=['timestamp'], 
            var_name='name', 
            value_name='value'
        )
    except Exception as e:
        logger.error(f"Encountered an error during pivoting to long format:\n {e}")
        sys.exit(1)

    logger.info(f"Transformation to long format complete. Sample of final data:\n{long_df.head()}")

    return long_df