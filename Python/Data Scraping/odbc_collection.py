import pyodbc
import pandas as pd

CHUNKSIZE = 100000  # max 500k
DATASET_RID = "your_dataset's_rid"
TOKEN = "your_token_here"

class FoundryClient:
    def __init__(self, base_url="https://lava.palantircloud.com/", token=None):
        """
        Initialize a client for interacting with Palantir data.

        Args:
            base_url (str, optional): The base URL of the Palantir data source. Defaults to "https://lava.palantircloud.com/".
            token (str, optional): The authentication token. Required if not None.
        """
        if token:
            # Establish a connection using pyodbc.
            self.conn = pyodbc.connect(f'Driver=FoundrySqlDriver;BaseUrl={base_url};Pwd={token};Dialect=SPARK')
        else:
            raise Exception("Please pass the token from Palantir")

    @property
    def get_column_names(self):
        """
        Get the column names of the selected dataset.

        Returns:
            list: A list of column names.
        """
        cursor = self.conn.cursor()
        columns = [column[0] for column in cursor.description]
        cursor.close()
        return columns

    def get_data(self, rid=None, condition=None, chunksize_=None):
        """
        Get data from a Palantir dataset.

        Args:
            rid (str, optional): The RID (Resource ID) of the dataset to fetch.
            condition (str, optional): A SQL condition to filter the data (e.g., "column_name > 10").
            chunksize_ (int, optional): The maximum number of rows to retrieve per query (for chunked data retrieval).

        Returns:
            pd.DataFrame: A DataFrame containing the requested data.
        """
        if rid:
            statement = f"""
                        SELECT *
                        FROM `{rid}` {f"WHERE {condition}" if condition else ""};
                        """
            if chunksize_:
                return pd.read_sql_query(statement, self.conn, chunksize=chunksize_)
            else:
                return pd.read_sql_query(statement, self.conn)
        else:
            raise Exception("Please pass the RID of the dataset to fetch")
        
# Example call
foundry = FoundryClient(token=TOKEN)
temp_df = foundry.get_data(rid=DATASET_RID, chunksize=CHUNKSIZE)  # scraping data part by part to prevent errors

# A dataframe which will hold all the data
final_df = pd.DataFrame()

for data in temp_df:
    final_df = pd.concat([final_df, data], axis=0)

print(final_df)