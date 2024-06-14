import pandas as pd

from data_pull import read_data

df = read_data(filename='sensor_series.csv',
               rid='ri.foundry.lava-catalog.dataset.4c5c453a-19ea-42c4-bd9c-c3ff5aa4dc9c',
               read_mode=False,
               data_path='data/')

ssid_table = read_data(filename='all_ssids_producers.csv',
               rid='ri.foundry.lava-catalog.dataset.121f8f82-9cfd-401d-be51-6dccd48efa49',
               read_mode=False,
               data_path='data/')

# Columns of df: 'SSID', 'DESCRIPTION', 'UNIT', 'WELL_NAME', 'FIELD', 'PLATFORM_NAME', 'REGION', 'SOURCE', 'IS_DEPRECATED'

# Create empty dataframe to store the final result and their types
final_result = pd.DataFrame(columns=['wellname', 'well_status', 'tag', 'tag_status', 'proposed_tag'])

# Function to propse tag for the given problematic tag
def get_proposed_tag(ssid, sensor_series = df):
    return 'tbd'

# Function to check whether the tag is in sensor series data and depreciated or not
def check_if_depreciated(tag, sensor_series):
    """
    Function to check the given tag from the sensor series table whether is depreciated or not

    Args:
        tag: str: tag to check
        sensor_series: pd.DataFrame: sensor series data

    Returns:
        str: status of the tag
    """
    # if tag in sensor series and is depreciated
    if tag in sensor_series['SSID'].values and sensor_series[sensor_series['SSID'] == tag]['IS_DEPRECATED'].values[0]:
        return 'depreciated tag', get_proposed_tag(tag)
    if tag not in sensor_series['SSID'].values:
        return 'tag not found', get_proposed_tag(tag)
    else:
        return 'active tag', None


# Function to scrap data from the tag and check whether data is valid or not
def node_collection(ssid: str, 
                    start, 
                    end, 
                    col_name: str) -> pd.DataFrame:
    """
    Returns: Dataframe timestamp, col_name
    """
    import NodeCollection, F # type: ignore
    collected_dataset = NodeCollection(ssid).map(F.time_range(start, end)).to_pandas()
    collected_dataset = collected_dataset.drop(['series'], axis=1)
    collected_dataset = collected_dataset.rename(columns={"value": col_name})
    return collected_dataset.sort_values(by='timestamp', ascending=True)




