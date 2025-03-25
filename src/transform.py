import pandas as pd

class Transform:
    def __init__(self, extractor):
        self.extractor = extractor
    
    def clean_and_join(self):
        tables = self.extractor.fetch_tables_as_array()

        if not tables:
            print("No tables.")
            return None 
        
        cleaned_tables = [table.dropna(axis = 1) for table in tables]

        Freight_Summary = cleaned_tables[0]
        for table in cleaned_tables[1:]:
            Freight_Summary = pd.merge(Freight_Summary, table, how='inner', on='key_column')

            return Freight_Summary