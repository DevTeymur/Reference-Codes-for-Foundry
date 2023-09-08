import pandas as pd

def interpolate_data(df: pd.DataFrame, 
                    col_names: list, 
                    method_: str = 'linear',
                    logs: bool = True) -> pd.DataFrame:
    """
    Interpolation of the missing data with a linear method.

    Args:
        df (pd.DataFrame): Dataframe to interpolate.
        col_names (list): Name of the columns to interpolate.
        method_ (str, optional): Interpolation method (default is 'linear').
        logs (bool, optional): Enable logging (default is True).

    Returns:
        pd.DataFrame: Dataframe with missing data interpolated.

    For more: https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.interpolate.html
    """
    print('Interpolating:', " ".join(col_names), end=' ') if logs else None
    for col in col_names:
        df[col] = df[col].interpolate(method=method_, limit_direction='forward', axis=0)
    print('Done') if logs else None
    return df

# Define a sample DataFrame.
data = {'timestamp': pd.date_range(start='2023-01-01', periods=10, freq='D'),
        'value1': [1.0, None, 3.0, None, None, 6.0, 7.0, None, 9.0, 10.0],
        'value2': [None, 2.0, None, 4.0, 5.0, None, None, 8.0, None, 11.0]}

df = pd.DataFrame(data)


# Specify columns to interpolate.
columns_to_interpolate = ['value1', 'value2']

# Call the interpolate_data function.
interpolated_df = interpolate_data(df, columns_to_interpolate, method_='linear', logs=True)
print(interpolated_df)
