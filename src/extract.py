from sqlalchemy import create_engine
import pandas as pd

class Extractor:
    def __init__(self, driver, server, database, uid, pwd):
        self.connection_url = (
            f"mssql+pyodbc://{uid}:{pwd}@{server}/{database}"
            "?driver=ODBC+Driver+17+for+SQL+Server"
        )
        self.engine = None

    def connect(self):
        try:
            self.engine = create_engine(self.connection_url)
            print("Connected!")
        except Exception as e:
            print(f"Connection failed: {e}")

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
        table_names = [
            "MOVING.DestinyByAddress",
            "MOVING.DistributionCenterByAddress",
            "PEOPLE.Employee",
            "PRODUCTION.Machine",
            "GENERAL.ServiceProvisionType"
        ]

        tables = [] # all tables
        for table in table_names:
            query = f"SELECT * FROM {table}"
            df = self.execute_query(query)
            if df is not None:
                tables.append(df)
            else:
                print(f"Failed to retrieve data from {table}")
        
        return tables


# Usage example
if __name__ == "__main__":
    extractor = Extractor(
        driver="ODBC Driver 17 for SQL Server",
        server="macotech-database-1.c9ccouqwg4es.us-east-2.rds.amazonaws.com,1433",
        database="MacoDatabase",
        uid="Estancia2",
        pwd="0n55KV/zuxh\\"
    )
    
    extractor.connect()
    tables = extractor.fetch_tables_as_array()
    for idx, table_df in enumerate(tables):
        print(f'TABLE {idx + 1}:')
        print(table_df) # the important one
        print("\n")
