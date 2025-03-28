import pandas as pd
from extract import Extractor
class DataCleaner:
    
    def __init__(self, dataframes: list):
        self.dataframes = dataframes
        
    def clean_data(self):
        cleaned_dataframes = []
        for df in self.dataframes:
            df_cleaned = self._clean_dataframe(df)
            cleaned_dataframes.append(df_cleaned)
        return cleaned_dataframes

    def _clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        # Drop columns that are not useful
        df = df.drop(columns=['createdAt', 'createdIn', 'updatedAt', 'updatedBy', 'updatedIn'], errors='ignore')
        
        # null values
        df = df.dropna(axis = 1)
        df = df.fillna('Unknown')  
        
        # Remove duplicates
        df = df.drop_duplicates()

        # Convert relevant columns to correct data types
        df['freightDate'] = pd.to_datetime(df['freightDate'], errors='coerce')


        df.rename(columns={'freightId': 'FreightID', 'code': 'FreightCode'}, inplace=True)

        return df


class DataTransformer:
    
    def __init__(self, cleaned_dataframes: list):
        self.cleaned_dataframes = cleaned_dataframes
    
    def merge_dataframes(self) -> pd.DataFrame:
        # Assuming we're merging on 'FreightID'
        merged_df = self.cleaned_dataframes[0]
        
        for df in self.cleaned_dataframes[1:]:
            merged_df = pd.merge(merged_df, df, on='FreightID', how='outer')
        
        return merged_df

    def transform(self) -> pd.DataFrame:
        # Step 1: Clean data
        cleaner = DataCleaner(self.cleaned_dataframes)
        cleaned_dataframes = cleaner.clean_data()

        # Step 2: Merge dataframes
        transformer = DataTransformer(cleaned_dataframes)
        final_dataframe = transformer.merge_dataframes()

        return final_dataframe

extractor = Extractor(config="config\database_config.yml")
tables = extractor.fetch_tables_as_array()

data_transformer = DataTransformer(tables)
final_dataframe = data_transformer.transform()

