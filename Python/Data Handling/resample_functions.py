import numpy as np
import pandas as pd
from datetime import timedelta

def resample_data(df: pd.DataFrame, 
                  freq: int,
                  timestamp_col_name: str,
                  logs: bool = True) -> pd.DataFrame:
    """
    Resample a DataFrame to a specified frequency and compute daily average values.

    Args:
        interval_df (pd.DataFrame): DataFrame with a datetime index to resample.
        freq (int): Resampling frequency in seconds.

    Returns:
        pd.DataFrame: Resampled DataFrame with daily average values.
    """
    freq = str(freq) + 'S'

    # Set the DataFrame index to the timestamp column and sort it.
    df = df.set_index(timestamp_col_name).sort_index()
    # Remove duplicated index values (if any).
    df = df[~df.index.duplicated()]
    print(df.head(), df.info()) if logs else None
    # Resample the DataFrame to the specified frequency and compute the daily average.
    resampled_df = df.resample(freq).mean()
    # Interpolate missing values using a linear method and reset the index.
    resampled_df = resampled_df.interpolate(method='linear').reset_index()
    return resampled_df


def slice_and_resample(interval_df: pd.DataFrame,
                       slice_count: int,
                       start_freq: int,
                       end_freq: int,
                       timestamp_column_name: str):
    """
    This function resamples the data decreasing the density based on the arguments. 

    Args:
        interval_df (pd.DataFrame): The input DataFrame containing time-series data.
        slice_count (int): The number of intervals to create.
        start_freq (int): The starting resampling frequency in seconds.
        end_freq (int): The ending resampling frequency in seconds.
        timestamp_column_name (str): The name of the timestamp column in the DataFrame.

    Returns:
        pd.DataFrame: The final DataFrame with resampled data.
    """
    # Reset the DataFrame index for consistency.
    interval_df = interval_df.reset_index(drop=True)
    interval_length = round((interval_df.loc[interval_df.shape[0]-1, timestamp_column_name] - interval_df.loc[0, timestamp_column_name]).total_seconds() / 3600, 2)
    slice_duration = interval_length / slice_count

    # Generate a list of resampling frequencies based on a linear space.
    freqs = list(np.round(np.linspace(start_freq, end_freq, slice_count), 0).astype(int))
    interval_start = interval_df.loc[0, timestamp_column_name]
    final_df = pd.DataFrame()

    for i in range(slice_count):
        # Define the time range for the current slice.
        slice_start = interval_start + timedelta(hours=slice_duration * i)
        slice_end = interval_start + timedelta(hours=slice_duration * (i + 1))
        # Select data within the current time range.
        temp_df = interval_df[(interval_df[timestamp_column_name] >= slice_start) & (interval_df[timestamp_column_name] <= slice_end)]
        # Resample the selected data using the corresponding frequency.
        resampled_df = resample_data(temp_df, freqs[i], 'Date')
        # Concatenate the resampled data to the final DataFrame.
        final_df = pd.concat([resampled_df, final_df], axis=0)
    return final_df


# Create a sample DataFrame with a datetime index.
data = {'Date': pd.date_range(start='2023-01-01', periods=10, freq='D'),
        'Value': [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]}

df = pd.DataFrame(data)

# Specify the resampling frequency in seconds (e.g., 86400 seconds for daily resampling).
resampling_frequency_seconds = 2000

resampled_df = resample_data(df, resampling_frequency_seconds, 'Date')
print(resampled_df)

slice_count = 10
start_freq = 10 # seconds
end_freq = 2000 # seconds

# Example call slice method
data = {'Date': pd.date_range(start='2023-01-01', periods=100, freq='H'),
        'Value': range(100)}

df = pd.DataFrame(data)

# Call the slice_and_resample function.
resampled_data = slice_and_resample(df, slice_count, start_freq, end_freq, 'Date')
print(resampled_data)