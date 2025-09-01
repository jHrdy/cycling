import pandas as pd
import matplotlib.pyplot as plt

def append_days_to_df(df: pd.DataFrame, datetime_col: str = "datetime") -> pd.DataFrame:
    """
    Append a column with day of the week to the DataFrame.

    Args:
        df (pd.DataFrame): Input DataFrame containing a datetime column.
        datetime_col (str): Name of the datetime column (default: "datetime").

    Returns:
        pd.DataFrame: Original DataFrame with an additional column 'day_of_week'.
    """
    if datetime_col not in df.columns:
        raise ValueError(f"Column '{datetime_col}' not found in DataFrame")

    df = df.copy()
    df[datetime_col] = pd.to_datetime(df[datetime_col])

    df["day_of_week"] = df[datetime_col].dt.day_name().str[:3]

    return df

def prep_data(path : str) -> pd.DataFrame:
    """Sorts data by datetime and appends day of the week column"""
    df = pd.read_parquet(path)
    df = df.sort_values(by='datetime')
    df = append_days_to_df(df)
    return df

def get_df(file_path) -> pd.DataFrame:
    """Returns preprocessed dataframe"""
    df = prep_data(file_path)
    return df

def main():
    df = get_df(file_path='viedenska2014-2020.parquet')
    print(df.head())

if __name__  == '__main__':
    main()