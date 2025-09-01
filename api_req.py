# ETL script

import requests as r
import pandas as pd
import json

API_URL = 'https://services8.arcgis.com/pRlN1m0su5BYaFAS/arcgis/rest/services/Cyklosčítač na Viedenskej v rokoch 2014 - 2020/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson'

def fetch_data(url : str, timeout : int = 10 ) -> list[dict]:
    """Fetches JSON data from API and return list of dicts"""
    try:
        response = r.get(url, timeout=timeout)
        response.raise_for_status()
        data = response.json()
        data = data['features']
        if not isinstance(data, list):
            raise ValueError("Expected a list of JSON objects")
        return data
    except (r.RequestException, ValueError) as e:
        raise RuntimeError(f"API fetch failed: {e}")

def extract_features(features: list[dict], use_geometry: bool = False) -> pd.DataFrame:
    """
    Extracts 'properties' (and optionally 'geometry') from a list of GeoJSON features
    and converts them into a clean pandas DataFrame.

    Args:
        features (list[dict]): List of GeoJSON Feature objects.
        use_geometry (bool): If True, include 'geometry' in the DataFrame.

    Returns:
        pd.DataFrame: A DataFrame with feature properties (and geometry if selected).
    """

    if not isinstance(features, list):
        raise TypeError(f"Expected list of features, got {type(features)}")

    records = []

    for feature in features:
        if "properties" not in feature:
            raise ValueError("Feature missing 'properties' field")
        
        record = feature["properties"].copy()

        if use_geometry:
            record["geometry"] = feature.get("geometry")
        
        records.append(record)

    return pd.DataFrame.from_records(records)

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Cleans DataFrame from Slovak labels, converts values to specific types returning pandas DataFrame."""
    
    df = df.rename(columns={
        "Dátum_a_čas": "datetime",
        "Viedenská": "viedenska",
        "K_Starému_mostu": "k_staremu_mostu",
        "K_Mostu_SNP": "k_mostu_snp",
        "ObjectId": "object_id"
    })
    
    df["datetime"] = pd.to_datetime(df["datetime"])
    numeric_cols = ["viedenska", "k_staremu_mostu", "k_mostu_snp"]
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce")
    
    return df

data = fetch_data(API_URL)
df = extract_features(data)
df = clean_dataframe(df)

if __name__ == '__main__':
    print(df.head())
    #df.to_parquet('viedenska.parquet')