from src.load import Loader
from src.extract import Extractor
import pandas as pd
import yaml

'''Orchestator'''

class ETL:
    def __init__(self, config_path="config/database_config.yml"):
        self.config = self._load_config(config_path)
        self.extractor = Extractor(self.config)
        self.loader = Loader(self.config)

    def _extract_config(self, path):
        with open(path, "r") as f:
            return yaml.safe_load(f)
        
    def _load_config(self, path):
        with open(path, "r") as f:
            return yaml.safe_load(f)

    def run(self):
        pass

    def run_extractor(self):
        print(self.extractor.fetch_tables_as_array())

    def run_transform(self):
        print("Starting ETL process")

        df = self._generate_dummy_data()

        df.columns = df.columns.str.lower()

        if df.empty:
            print("The DataFrame is empty. No data to load.")
        else: 
            self.loader.load_to_postgres(df)
            self.loader.export_insert_scripts(df)

    '''This method is only for testing Load.'''
    def _generate_dummy_data(self):
        data = [{
            "freightId": 101,
            "companyId": 1,
            "distributionCenterId": 10,
            "distributionCenterName": "Yucatan Center",
            "destinationId": 501,
            "destinationCode": "DEST-501",
            "destinationAddress": "50th St x 60, Merida",
            "employeeId": 9001,
            "employeeStatus": "Active",
            "totalWeight": 2500.50,
            "distanceTraveled": 120.75,
            "freightDate": "2024-10-10 08:00:00",
            "freightStatus": "Delivered",
            "onTimeDelivery": True,
            "deliveryDelay": 0.0,
            "efficiencyScore": 89.5,
            "deliverySuccessRate": 100.0,
            "revenueGenerated": 50000.00,
            "discountApplied": 1200.00,
            "machineCount": 3,
            "serviceType": "Express",
            "orderCodeStandardized": "ORD-2024-0001",
            "orderType": "OC",
            "createdAt": "2024-10-10 07:45:00",
            "updatedAt": "2024-10-10 08:30:00",
            "invoiceGenerated": True
        }]

        df = pd.DataFrame(data)
        return df