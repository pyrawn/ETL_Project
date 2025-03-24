import pandas as pd
import yaml
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError  

class Loader:
    
    def __init__(self, config: dict | str):
        self.config = self.load_config(config) if isinstance(config, str) else config
        self.pg_config = self.config["target_postgres"]
        self.engine = self._connect_postgres()

    def _load_config(self, path: str) -> dict:
        try: 
            with open(path, "r") as file:
                return yaml.safe_load(file)
        except Exception as e:
            print(f"Error while reading config file:{e}")
            raise

    def _connect_postgres(self):
        try: 
            conn_string = (
                f"postgresql://{self.pg_config['user']}:{self.pg_config['password']}"
                f"@{self.pg_config['host']}:{self.pg_config['port']}/{self.pg_config['database']}"
            )
            engine = create_engine(conn_string)
            print("Connected to PostgreSQL.")
            return engine
        except SQLAlchemyError as e:
            print(f"PostgreSQL connection error: {e}")
            raise

    def load_to_postgres(self, df: pd.DataFrame, if_exists="append"):
        try: 
            df.to_sql(
                name = self.pg_config["table"],
                con = self.engine,
                if_exists= if_exists,
                index=False,
                method="multi",
                chunksize=1000
            )
            print(f"{len(df)} records loaded into {self.pg_config['table']}.")
        except SQLAlchemyError as e:
            print(f"Error loading data into PostgreSQL: {e}")
            raise

    
