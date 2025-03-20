import logging
from extract import Extractor
from transform import Transformer
from load import Loader

# Configure logging
logging.basicConfig(filename="logs/etl.log", level=logging.INFO, format="%(asctime)s - %(message)s")

class ETL:
    """Orchestrates the ETL pipeline."""

    def __init__(self):
        self.extractor = Extractor()
        self.transformer = Transformer()
        self.load = Loader()

    def run_pipeline(self):
        try:
            # Extract
            data = self.extractor.extract_from_csv("data/raw_sales.csv")
            logging.info("Data extracted successfully.")

            # Transform
            data = self.transformer.clean_column_names(data)
            data = self.transformer.drop_null_values(data)
            logging.info("Data transformed successfully.")

            # Load
            self.loader.load_to_csv(data, "data/processed_sales.csv")
            logging.info("Data loaded successfully.")

        except Exception as e:
            logging.error(f"ETL process failed: {str(e)}")

# Run ETL
if __name__ == "__main__":
    pipeline = ETL()
    pipeline.run_pipeline()
