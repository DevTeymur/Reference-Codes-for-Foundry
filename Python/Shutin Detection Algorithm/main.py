import pandas as pd
import numpy as np
from datetime import datetime, timedelta

MIN_SHUTIN_TIME_LIMIT = 7 #HOURS # difference between 2 consecutive shutin intervals if less than specified value then will be joined
SHUTIN_INTERVAL_TIME_LIMIT = 2 #HOURS # if the duration of the interval is less than specified value this will be ignored
PREVIOUS_INTERVAL_DURATION = 3 


# ASSUMING DATA IS CLEANED AND FILTERED
# COLUMNS: timestamp, choke, master_valve, wing_valve

def group_start_end_intervals(func_df: pd.DataFrame, 
                            time_limit: float) -> pd.DataFrame:
    """
    Args:
        func_df: interval times with start_ and end_ columns
        time_limit: min hours between 2 intervals
    Returns: 
        same dataframe with func_df
    """
    row = 0
    nRows = func_df.shape[0] - 1
    while row < nRows:
        if func_df.loc[row, 'end_'] >= func_df.loc[row+1, 'start_'] - timedelta(hours = time_limit):
            func_df.loc[row, 'end_'] = func_df.loc[row+1, 'end_']
            func_df.drop(index = row+1, axis=0, inplace=True)
            func_df.reset_index(drop=True, inplace=True)
            row = 0
            nRows = func_df.shape[0] - 1
            continue
        else:
            row += 1
    return func_df

def return_intervals(df: pd.DataFrame, 
                        from_: int, 
                        to_: int, 
                        time_limit: float = MIN_SHUTIN_TIME_LIMIT, 
                        interval_duration_limit: float = SHUTIN_INTERVAL_TIME_LIMIT):
    """
    Args:
        df: original dataframe (timestamp, dhp, shutin)
        from_ and to_: switch points
        time_limit: min hours between 2 intervals which will be sent to group_start_end_intervals
        interval_duration_limit: min length of shutin interval
        logs: whether to print or not
    Returns: 
        all data of the shutin intervals together and dataframe with columns start_ and end_
    """
    df = df.sort_values(by='timestamp', ascending=True)
    first_false_index = df.index[df['shutin'] == False].min()
    if first_false_index == np.nan:
        df = df.iloc[first_false_index:]
        df.reset_index(drop=True, inplace=True) ####
    # print(df.loc[first_false_index, 'timestamp'])

    
    # Find the indices where the label changes from 1 to 2 and from 2 to 1
    label_change_1_to_2 = df.index[(df['shutin'] == from_) & (df['shutin'].shift(1) == to_)]  # closure date
    label_change_2_to_1 = df.index[(df['shutin'] == to_) & (df['shutin'].shift(1) == from_)]  # opening date
    print(f'This well closed {len(label_change_1_to_2)} times, opened {len(label_change_2_to_1)} times')
    # print(label_change_1_to_2, label_change_2_to_1)

    if len(label_change_1_to_2) > len(label_change_2_to_1):
        label_change_1_to_2 = label_change_1_to_2[:-1]
    if len(label_change_1_to_2) < len(label_change_2_to_1):
        label_change_2_to_1 = label_change_2_to_1[:-1]

    # Depending on the closure time here we set the property that cause well to close
    properties = []
    for i in label_change_2_to_1:
        # print(df.loc[i+1, 'choke'], df.loc[i+1, 'master_valve'], df.loc[i+1, 'wing_valve'])
        if df.loc[i, 'choke'] < 0.1:
            properties.append('choke')
            continue
        elif df.loc[i, 'master_valve'] == 0:
            properties.append('master_valve')
            continue
        elif df.loc[i, 'wing_valve'] == 0:
            properties.append('wing_valve')
            continue
        else:
            properties.append('unknown')

    intervals = pd.DataFrame({
        'start': df.loc[label_change_2_to_1, 'timestamp'].values,
        'end': df.loc[label_change_1_to_2, 'timestamp'].values,
        'closed': properties
    })

    # print(intervals.head(20))
    # Create a DataFrame with just the start and end timestamps
    start_end_df = intervals.copy()
    start_end_df = start_end_df[['start', 'end', 'closed']]
    start_end_df.rename(columns = {'end': 'end_', 'start': 'start_'}, inplace=True)
    start_end_df = start_end_df.sort_values(by = 'start_', ascending = True).reset_index(drop=True)
    start_end_df = group_start_end_intervals(start_end_df, time_limit)
    # Get the rows of the original DataFrame between each start and end timestamp
    interval_data = []
    for index, (start, end) in enumerate(zip(start_end_df['start_'], start_end_df['end_'])):
        # print(index, round((end - start).total_seconds()/3600, 2), end = ' ')
        if (end - start).total_seconds()/3600 > interval_duration_limit and (end - start).total_seconds()/3600  < 2880:
            interval = df[(df['timestamp'] >= start - timedelta(hours = PREVIOUS_INTERVAL_DURATION)) & 
                        (df['timestamp'] <= end + timedelta(hours = PREVIOUS_INTERVAL_DURATION))]
            interval_data.append(interval)
            print(f'{start} - {end}: {round((end - start).total_seconds()/3600, 2)} hours - valid' )
        else:
            start_end_df.drop(index=index, inplace=True)
            print(f'{start} - {end}: {round((end - start).total_seconds()/3600, 2)} hours - invalid' )

    try:
        interval_df = pd.concat(interval_data)
    except:
        print(f'[{(datetime.now() + timedelta(hours = 4)).strftime("%H:%M:%S")}] No valid shutin intervals found in data')
        return pd.DataFrame(), start_end_df
    return interval_df.reset_index(drop=True), start_end_df.reset_index(drop=True)


# Example call
YOUR_FILTERED_DATA = pd.DataFrame()
pta_intervals, start_ends_c = return_intervals(YOUR_FILTERED_DATA, 0, 1, MIN_SHUTIN_TIME_LIMIT, SHUTIN_INTERVAL_TIME_LIMIT)

# START ENDS Dataframe will contain start and end times of the shutin intervals