from sqlalchemy import create_engine
import pandas as pd
import yaml
class Extractor:
    def __init__(self, config: dict | str):
        self.config = self._load_config(config) if isinstance(config, str) else config
        self.pg_config = self.config["source_sqlserver"]
        self.engine = self.connect()



    def connect (self, path: str) -> dict:
        try: 
            with open(path, "r") as file:
                return yaml.safe_load(file)
        except Exception as e:
            print(f"Error while reading config file:{e}")
            raise 

    def execute_query(self, query):
        if self.engine:
            try:
                return pd.read_sql(query, self.engine)
            except Exception as e:
                print(f"Query execution failed: {e}")
                return None
        else:
            print("No active connection.")
            return None

    def close_connection(self):
        if self.engine:
            self.engine.dispose()
            print("Connection closed.")

    def fetch_tables_as_array(self):
        query_tables = [
            "MOVING.DestinyByAddress",
            "MOVING.DistributionCenterByAddress",
            "PEOPLE.Employee",
            "PRODUCTION.Machine",
            "GENERAL.ServiceProvisionType"
        ]

        tables = [] # all tables
        for query in query_tables:
            df = self.execute_query(query)
            if df is not None:
                tables.append(df)
            else:
                print(f"Failed to retrieve data from {query}")
        
        return tables

