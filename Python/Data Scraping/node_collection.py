import pandas as pd
import foundryts.functions as F
from foundryts import NodeCollection

def node_collection(ssid: str, 
                    start_time, 
                    end_time, 
                    column_name: str) -> pd.DataFrame:
    """
    Gather data for a specified column from a data source.
    Args:
        ssid (str): The unique identifier for the data source.
        start_time: The start time of the data collection period.
        end_time: The end time of the data collection period.
        column_name (str): The name to assign to the collected data column.

    Returns:
        pd.DataFrame: A DataFrame containing timestamp and the specified column data.
    """
    # Replace the following line with your actual data collection code.
    collected_dataset = NodeCollection(ssid).map(F.time_range(start_time, end_time)).to_pandas()
    # Drop the 'series' column if it exists (replace with your actual logic).
    collected_dataset = collected_dataset.drop(['series'], axis=1)
    # Rename the 'value' column to the specified 'column_name'.
    collected_dataset = collected_dataset.rename(columns={"value": column_name})
    # Sort the DataFrame by timestamp in ascending order.
    return collected_dataset.sort_values(by='timestamp', ascending=True)
