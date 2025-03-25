from sqlalchemy import create_engine
import pandas as pd
import yaml
import urllib

class Extractor:
    
    def __init__(self, config: dict | str):
        self.config = self._load_config(config) if isinstance(config, str) else config
        self.sqlserver_config = self.config["source_sqlserver"]
        self.engine = self.connect()

    def _load_config(self, path: str) -> dict:
        try: 
            with open(path, "r") as file:
                return yaml.safe_load(file)
        except Exception as e:
            print(f"Error while reading config file: {e}")
            raise 

    def connect(self):
        try:
            conn_str = (
                f"DRIVER={self.sqlserver_config['driver']};"
                f"SERVER={self.sqlserver_config['server']},{self.sqlserver_config['port']};"
                f"DATABASE={self.sqlserver_config['database']};"
                f"UID={self.sqlserver_config['username']};"
                f"PWD={self.sqlserver_config['password']}"
            )
            quoted = urllib.parse.quote_plus(conn_str)
            engine = create_engine(f"mssql+pyodbc:///?odbc_connect={quoted}")
            print("Connected to SQL Server.")
            return engine
        except Exception as e:
            print(f"Error connecting to SQL Server: {e}")
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
            "SELECT * FROM MOVING.Freight f INNER JOIN MOVING.DistributionCenterByAddress dcba ON f.distributionCenterCodeId = dcba.distributionCenterByAddressId;",
            "SELECT f.FreightID, f.code AS FreightCode, f.freightDate, dba.destinyId, dba.addressId AS DestinationCode, dba.code AS DestinationAddress FROM MacoDatabase.MOVING.Freight f INNER JOIN Macodatabase.MOVING.DestinyByAddress dba ON f.destinyCodeId = dba.destinyId;",
            "SELECT * FROM MOVING.Freight f INNER JOIN PEOPLE.EmployeeStatus es ON f.status = es.employeeStatusId;",
            "SELECT * FROM MacoDatabase.MOVING.Freight f INNER JOIN MacoDatabase.PRODUCTION.Machine	m ON f.companyId = m.companyId;",
            "SELECT * FROM MacoDatabase.MOVING.Freight f INNER JOIN MacoDatabase.GENERAL.ServiceProvisionType sptON f.companyId = spt.companyId;"
        ]
        '''
        Key Queries:
        SELECT *
        FROM MOVING.Freight f
        INNER JOIN MOVING.DistributionCenterByAddress dcba
            ON f.distributionCenterCodeId = dcba.distributionCenterByAddressId;

        SELECT
        f.FreightID,
        f.code AS FreightCode,
        f.freightDate,
        dba.destinyId,
        dba.addressId AS DestinationCode,
        dba.code AS DestinationAddress
        FROM MacoDatabase.MOVING.Freight f
        INNER JOIN Macodatabase.MOVING.DestinyByAddress dba
            ON f.destinyCodeId = dba.destinyId;

        SELECT *
        FROM MOVING.Freight f
        INNER JOIN PEOPLE.EmployeeStatus es
        ON f.status = es.employeeStatusId;


        SELECT * 
        FROM MacoDatabase.MOVING.Freight f
        INNER JOIN MacoDatabase.PRODUCTION.Machine	m
            ON f.companyId = m.companyId;


        SELECT *
        FROM MacoDatabase.MOVING.Freight f
        INNER JOIN MacoDatabase.GENERAL.ServiceProvisionType spt
            ON f.companyId = spt.companyId;
        '''
        tables = [] # all tables
        for query in query_tables:
            df = self.execute_query(query)
            if df is not None:
                tables.append(df)
            else:
                print(f"Failed to retrieve data from {query}")
        
        return tables

