from sqlalchemy import create_engine
import pandas as pd

class Loader:
    
    def __init__(self, df):
        self.__df = df

    def load_to_csv(self, df: pd.DataFrame, file_path: str):
        """Saves DataFrame to a CSV file."""
        df.to_csv(file_path, index=False)

    def load_to_database(self, df: pd.DataFrame, conn_string: str, table_name: str):
        """Loads DataFrame into a SQL database."""
        engine = create_engine(conn_string)
        df.to_sql(table_name, engine, if_exists="replace", index=False)
    
