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
final_result = pd.DataFrame(columns=['wellname', 'tag', 'tag_type', 'tag_status', 'proposed_tag', 'comment'])

# Function to propse tag for the given problematic tag
def get_proposed_tag(ssid, sensor_series = df):
    return 'tbd'


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


def check_data_in_tag(tag):
    """
    Function to check the data inside the tag

    Args:
        tag: str: tag to check

    Returns:
        str: status of the tag
        str: comment
    """
    # set start and end end is today, start is 1 month before
    end = pd.Timestamp.now()
    start = end - pd.Timedelta(days=30)
    # data = node_collection(tag, start, end, 'values')
    data = pd.DataFrame()
    if data.empty:
        # Further tag propose mechanism
        return 'no data in tag', 'data points are empty'
    else:
        # count the amount of zero points if more than 98% then tag is not valid
        zero_points = data[data['values'] == 0].shape[0]
        if zero_points / data.shape[0] > 0.98:
            return 'no data in tag', 'most of the data is zero'
        


# Function to check whether the tag is in sensor series data and depreciated or not
def check_tag(tag, sensor_series):
    """
    Function to check the given tag from the sensor series table whether is depreciated or not

    Args:
        tag: str: tag to check
        sensor_series: pd.DataFrame: sensor series data

    Returns:
        str: status of the tag
        str: proposed tag if the tag is depreciated
        str: comment
    """
    if pd.isna(tag):
        return 'no tag', None, None
    if tag in sensor_series['SSID'].values and sensor_series[sensor_series['SSID'] == tag]['IS_DEPRECATED'].values[0]:
        # here get proposed tag then check if there is data or not in the tag
        return 'depreciated tag', get_proposed_tag(tag), None
    if tag not in sensor_series['SSID'].values and not pd.isna(tag):
        # Check the tag data with node collection
        print('Tag not found in sensor series table, starting to collect data from the tag...')
        status, comment = check_data_in_tag(tag)
        return status, get_proposed_tag(tag), comment
    else:
        # Double check the data inside the tag
        return 'active tag', None, 'will check tag if behaves normal'



# Check all ssid columns in frm ssid table

ssid_columns = [
    "dhp",
    "master_valve",
    "wing_valve",
    "drawdown",
    "theoretical_oil",
    "theoretical_water",
    "test_oil",
    "test_water",
    "alloc_oil",
    "gl_choke",
    "a_annulus_pressure",
    "b_annulus_pressure",
    "c_annulus_pressure",
    "GL_rate",
    "HP_enabled",
    "LP_enabled",
    "Test_enabled",
    "vertex_routing",
    "arthemis_QOil",
    "arthemis_QWater",
    "arthemis_QGas",
    "arthemis_QLiquid",
    "arthemis_QGOR",
    "arthemis_QWC",
    "watercut",
    "well_mode",
    "sand_concentration",
]


for index, row in ssid_table.iterrows():
    print('____'*10)
    print(f'Checking well: {row["wellname"]}')
    for column in ssid_columns:
        print(f'{column} - {row[column]}', end='   ')
        tag_status, proposed_tag, comment = check_tag(row[column], df)
        print(f'status: {tag_status}, Proposed tag: {proposed_tag}')
        line = pd.DataFrame(data={'wellname': row['wellname'], 'tag': row[column], 'tag_type': column, 'tag_status': tag_status, 'proposed_tag': proposed_tag, 'comment': comment}, index=[0])
        final_result = pd.concat([final_result, line], axis=0, ignore_index=True)
    if index == 5:
        break

print(final_result.reset_index(drop=True).head())
final_result.to_csv('tags_validation.csv', index=False)
