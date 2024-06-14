# File: data_pull.py
# Author: Tima
# Date: June 13, 2024

import pandas as pd
import pyodbc
import gc, os
from datetime import datetime, timedelta

# CONSTANTS FOR DATA COLLECTION
f = open(r'../../../token.txt', 'r')
TOKEN = f.readline()
f.close()

CHUNKSIZE = 100000
timezone = 'Baku'
HOURS_TO_ADD = 0 if timezone == 'Baku' else 4

# COLORS
RED = '\033[91m'
GREEN = '\033[92m'
YELLOW = '\033[93m'
RESET = '\033[0m'

SELECTED_DATE = datetime.now()


# This part is to take data with pyodbc reader
class FoundryClient:
    def __init__(self, base_url="https://lava.palantircloud.com/", token = None):
        if token:
            self.conn = pyodbc.connect(f'Driver=FoundrySqlDriver;BaseUrl={base_url};Pwd={token};Dialect=SPARK')
        else:
            raise Exception("Please pass the token from Palantir")
        
    @property
    def get_column_names(self):
        cursor = self.conn.cursor()
        columns = [column[0] for column in cursor.description]
        cursor.close()
        return columns

    def get_data(self, rid = None, condition = None, chunksize = None):
        if rid:
            statement = f"""
                        SELECT *
                        FROM `{rid}` {f"WHERE {condition}" if condition else ""};
                        """
            if chunksize:
                return pd.read_sql_query(statement, self.conn, chunksize = chunksize)
            else:
                return pd.read_sql_query(statement, self.conn)
        else:
            raise Exception("Please pass the RID of dataset to fetch")
        

def get_data(RID, logs = False):
    print(f'[{(datetime.now() + timedelta(hours = HOURS_TO_ADD)).strftime("%H:%M:%S")}] Starting to scrap data...')

    requested_data = pd.DataFrame() 

    foundry = FoundryClient(token=TOKEN)
    temp_df = foundry.get_data(rid=RID, chunksize=CHUNKSIZE)
    
    for data in temp_df:
        requested_data = pd.concat([requested_data, data], axis=0)
    gc.collect()

    print(GREEN + f'[{(datetime.now() + timedelta(hours = HOURS_TO_ADD)).strftime("%H:%M:%S")}] Data collection done!')
    print("____________________Ready data____________________") if logs else None
    print(requested_data.head()) if logs else None
    print(f'{len(requested_data)} lines scraped' + RESET)
    return requested_data


def get_ssid_data(path = 'input/TAGS.csv'):
    ssid_table = pd.read_csv(path)
    ssid_table = ssid_table[(ssid_table.status == 'Active') & (ssid_table['wellname'].str.startswith(('B', 'C', 'D')))].reset_index()
    list_of_wells_in_azeri = ssid_table['wellname'].tolist()
    return ssid_table, list_of_wells_in_azeri


def file_updated_last_day(file_path) -> bool:
    """
    Returns whether file updated in the last 24 hours or not
    Returns: True if modified
    """
    current_time = datetime.now()
    stat_info = os.stat(file_path)
    modification_time = datetime.fromtimestamp(stat_info.st_mtime)
    print(f"Current time: {current_time}\nModified datetime: {modification_time}")

    if modification_time > current_time - timedelta(hours=24):
        return True
    return False


def file_exists_in_path(path, filename):
    """
    Returns true if there is specified file in given path
    """
    return filename in os.listdir(path)



def read_data(filename: str, 
              rid: str, 
              read_mode: bool, 
              data_path: str = 'input/'):
    """
    Reads the data from the file if it is up to date, else scrapes the data

    Args:
        filename: str: name of the file
        rid: str: RID of the dataset
        read_mode: bool: whether to read from the file or scrape
        data_path: str: path of the file

    Returns:
        pd.DataFrame: data
    """
    print(f'[{(datetime.now() + timedelta(hours = HOURS_TO_ADD)).strftime("%H:%M:%S")}] Reading data...')
    print(YELLOW + f'Current parameters: filename: {filename}, rid: {rid}, read_mode: {read_mode}, data_path: {data_path}' + RESET)
    if file_exists_in_path(data_path, filename) and file_updated_last_day(f'{data_path}{filename}') and read_mode == False:
        print(GREEN + f'File is up to date, reading...' + RESET)
        data = pd.read_csv(f'{data_path}{filename}') 
    if read_mode == True or not file_exists_in_path(data_path, filename) or not file_updated_last_day(f'{data_path}{filename}'):
        print(YELLOW + f'File is not up to date, scraping...' + RESET)
        data = get_data(rid)
        data.to_csv(f'{data_path}{filename}', index=False, date_format='%Y-%m-%d %H:%M:%S')
    return data
