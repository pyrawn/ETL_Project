from src.load import Loader
from src.extract import Extractor
from src.transform import Transformer
import pandas as pd
import yaml

class ETL:
    def __init__(self, config_path="config/database_config.yml", verbose=True):
        self.verbose = verbose
        self.config = self._load_config(config_path)
        self.extractor = Extractor(self.config)
        self.transformer = Transformer(self.get_extractor())
        self.loader = Loader(self.config)

    def _load_config(self, path):
        with open(path, "r") as f:
            return yaml.safe_load(f)

    def run(self):
        if self.verbose:
            print("Starting ETL pipeline...")

        df = self.run_transformer()
        df.columns = df.columns.str.lower()

        if df.empty:
            print("The DataFrame is empty. No data to load.")
        else:
            if self.verbose:
                print(f"DataFrame ready with {len(df)} rows. Loading into PostgreSQL...")
            self.loader.load_to_postgres(df)
            self.loader.export_insert_scripts(df)

    def run_extractor(self):
        data = self.extractor.fetch_tables_as_array()
        print("Extracted tables:")
        for i, df in enumerate(data, start=1):
            print(f"  Table {i}: {len(df)} rows, {len(df.columns)} columns")
        return data

    def get_extractor(self):
        return self.extractor.fetch_tables_as_array()

    def run_transformer(self):
        return self.transformer.transform()

    def run_load(self):
        print("Starting manual load process with dummy data...")

        df = self._generate_dummy_data()
        df.columns = df.columns.str.lower()

        if df.empty:
            print("Dummy DataFrame is empty. Nothing to load.")
        else:
            self.loader.load_to_postgres(df)
            self.loader.export_insert_scripts(df)

    def _generate_dummy_data(self):
        data = [{
            "freight_id": 1,
            "freight_code": "TEST_001",
            "freight_date": pd.Timestamp("2025-03-27"),
            "company_id": 10,
            "customer_id": 100,
            "operator_id": 7,
            "distribution_center_code_id": 2,
            "destiny_code_id": 5,
            "destination_code": "CDMX",
            "destination_address": "Somewhere in Mexico",
            "freight_type_id": 1,
            "freight_status_id": 3,
            "customer_order_code": "ORD123",
            "travelled_distance": 450.5,
            "vehicle_id": 4,
            "pre_invoice": "/files/company1/preinvoice/20250327T080000.pdf",
            "started_date": pd.Timestamp("2025-03-27 08:00:00"),
            "created_at": pd.Timestamp("2025-03-27 07:45:00"),
            "status": True,
            "duration_days": 0,
            "cost_per_km": None,
            "is_long_distance": False,
            "day_of_week": "Thursday"
        }]

        return pd.DataFrame(data)
