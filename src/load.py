import pandas as pd
import yaml
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError 
from sqlalchemy import Table, Column, MetaData, Integer, String, Float, Boolean, DateTime, Text


class Loader:

    def __init__(self, config: dict | str):
        self.config = self._load_config(config) if isinstance(config, str) else config
        self.pg_config = self.config["target_postgres"]
        self.engine = self._connect_postgres()

    def _load_config(self, path: str) -> dict:
        try: 
            with open(path, "r") as file:
                return yaml.safe_load(file)
        except Exception as e:
            print(f"Error while reading config file: {e}")
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

    def create_freightsummary_table_if_not_exists(self, engine):
        metadata = MetaData()

        freightsummary = Table("freightsummary", metadata,
            Column("freight_id", Integer),
            Column("freight_code", String),
            Column("freight_date", DateTime),
            Column("company_id", Integer),
            Column("customer_id", Integer),
            Column("operator_id", Integer),
            Column("distribution_center_code_id", Integer),
            Column("destiny_code_id", Integer),
            Column("destination_code", String),
            Column("destination_address", Text),
            Column("freight_type_id", Integer),
            Column("freight_status_id", Integer),
            Column("customer_order_code", String),
            Column("travelled_distance", Float),
            Column("vehicle_id", Integer),
            Column("pre_invoice", Text),
            Column("started_date", DateTime),
            Column("created_at", DateTime),
            Column("status", Boolean),
            Column("duration_days", Integer),
            Column("is_long_distance", Boolean),
            Column("day_of_week", String)
        )

        metadata.create_all(engine, checkfirst=True)

    def load_to_postgres(self, df: pd.DataFrame, if_exists="append"):
        try:
            # Ensure table exists before loading
            self.create_freightsummary_table_if_not_exists(self.engine)

            df.to_sql(
                name=self.pg_config["table"],
                con=self.engine,
                if_exists=if_exists,
                index=False,
                method="multi",
                chunksize=1000
            )
            print(f"{len(df)} records loaded into {self.pg_config['table']}.")
        except SQLAlchemyError as e:
            print(f"Error loading data into PostgreSQL: {e}")
            raise

    def export_insert_scripts(self, df: pd.DataFrame):
        print("Generating INSERT statements:")
        table = self.pg_config["table"]
        for _, row in df.iterrows():
            columns = ', '.join([f'"{col}"' for col in row.index])
            values = ', '.join(
                [self._format_sql_value(val) for val in row.values]
            )
            insert_statement = f'INSERT INTO "{table}" ({columns}) VALUES ({values});'
            print(insert_statement)

    def _format_sql_value(self, value):
        if pd.isna(value):
            return "NULL"
        if isinstance(value, str):
            escaped = value.replace("'", "''")
            return f"'{escaped}'"
        if isinstance(value, bool):
            return 'TRUE' if value else 'FALSE'
        return str(value)
