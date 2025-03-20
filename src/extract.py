import pandas as pd
import json
import requests

class Extractor:
    """Handles data extraction from multiple sources."""

    def extract_from_csv(self, file_path: str) -> pd.DataFrame:
        """Extract data from a CSV file."""
        return pd.read_csv(file_path)

    def extract_from_api(self, url: str, params: dict = None) -> dict:
        """Extract data from an API."""
        response = requests.get(url, params=params)
        if response.status_code == 200:
            return response.json()
        raise Exception(f"API request failed with status {response.status_code}")

    def extract_from_database(self, conn, query: str) -> pd.DataFrame:
        """Extract data from a SQL database."""
        return pd.read_sql(query, conn)
