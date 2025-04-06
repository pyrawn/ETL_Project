import pandas as pd
import numpy as np
from src.extract import Extractor

class Transformer:
    def _init_(self, tables):
        self.tables = tables  # List of 5 Dataframes

    def transform(self):
        cleaned_tables = []

        for i, df in enumerate(self.tables):
            duplicated_cols = df.columns[df.columns.duplicated()].tolist()
            if duplicated_cols:
                print(f"DataFrame {i+1} Has the following duplicated columns: {duplicated_cols}")
            df = df.loc[:, ~df.columns.duplicated()]
            cleaned_tables.append(df)

        combined = pd.concat(cleaned_tables, ignore_index=True)

        rename_map = {
            'freightid': 'freight_id',
            'freightcode': 'freight_code',
            'freightdate': 'freight_date',
            'companyid': 'company_id',
            'customerid': 'customer_id',
            'operatorid': 'operator_id',
            'distributioncentercodeid': 'distribution_center_code_id',
            'destinycodeid': 'destiny_code_id',
            'destinationcode': 'destination_code',
            'destinationcc': 'destination_code',
            'destinationaddress': 'destination_address',
            'destinationad': 'destination_address',
            'freighttypeid': 'freight_type_id',
            'freightstatusid': 'freight_status_id',
            'customerordercode': 'customer_order_code',
            'travelleddistance': 'travelled_distance',
            'vehicleid': 'vehicle_id',
            'preinvoice': 'pre_invoice',
            'starteddate': 'started_date',
            'createdat': 'created_at',
            'status': 'status'
        }

        combined.columns = [col.strip() for col in combined.columns]
        col_map = {col: rename_map[col.lower()] for col in combined.columns if col.lower() in rename_map}
        combined = combined.rename(columns=col_map)

        for col in rename_map.values():
            if col not in combined.columns:
                combined[col] = np.nan

        combined = combined.loc[:, ~combined.columns.duplicated()]

        # Get destination info and preserve all destination rows per freight_id
        destino_df = pd.DataFrame()
        for df in self.tables:
            if set(['FreightID', 'DestinationCode', 'DestinationAddress']).issubset(df.columns):
                destino_df = df[['FreightID', 'DestinationCode', 'DestinationAddress']].copy()
                destino_df.columns = ['freight_id', 'destination_code', 'destination_address']
                break

        if not destino_df.empty:
            destino_df['freight_id'] = pd.to_numeric(destino_df['freight_id'], errors='coerce').astype('Int64')
            destino_df = destino_df.dropna(subset=['freight_id'])
            combined['freight_id'] = pd.to_numeric(combined['freight_id'], errors='coerce').astype('Int64')
            combined = combined.drop(columns=['destination_code', 'destination_address'], errors='ignore')
            combined = pd.merge(combined, destino_df, on='freight_id', how='inner')

        # Drop rows without a freight_date
        combined = combined[combined['freight_date'].notna()]

        # Clean and convert travelled_distance
        combined['travelled_distance'] = (
            combined['travelled_distance']
            .astype(str)
            .str.replace(",", "", regex=False)
            .replace("", np.nan)
        )
        combined['travelled_distance'] = pd.to_numeric(combined['travelled_distance'], errors='coerce')

        combined['destination_code'] = combined['destination_code'].fillna(-1)
        combined['destination_address'] = combined['destination_address'].fillna("Unknown")

        print(f"destination_code nulls: {combined['destination_code'].isna().sum()}")
        print(f"destination_address nulls: {combined['destination_address'].isna().sum()}")

        combined['pre_invoice'] = combined['pre_invoice'].astype(str)

        def generate_freight_code(row):
            try:
                if pd.isna(row['freight_code']) and pd.notna(row['freight_id']):
                    return f"FREIGHT_{int(row['freight_id'])}"
                return row['freight_code']
            except Exception as e:
                print(f"Error at row {row.name}: {e}")
                return np.nan

        combined['freight_code'] = combined.apply(generate_freight_code, axis=1)

        # Remove rows without freight_id
        if 'freight_id' in combined.columns:
            combined = combined.loc[:, ~combined.columns.duplicated()]
            combined = combined[combined['freight_id'].notna()]

        # Derived columns
        combined['duration_days'] = (
            pd.to_datetime(combined['started_date'], errors='coerce') -
            pd.to_datetime(combined['created_at'], errors='coerce')
        ).dt.days

        combined['is_long_distance'] = combined['travelled_distance'].apply(
            lambda x: x > 500 if pd.notnull(x) else False
        )

        combined['day_of_week'] = pd.to_datetime(combined['freight_date'], errors='coerce').dt.day_name()

        # Normalize strings
        for col in combined.columns:
            if combined[col].dtype == 'object':
                combined[col] = combined[col].astype(str).str.strip().str.lower()
            elif pd.api.types.is_numeric_dtype(combined[col]):
                combined[col] = pd.to_numeric(combined[col], errors='coerce')
            elif pd.api.types.is_datetime64_any_dtype(combined[col]):
                combined[col] = pd.to_datetime(combined[col], errors='coerce')

        # Drop duplicates based on business logic
        combined = combined.drop_duplicates(subset=[
            'freight_id', 'freight_code', 'freight_date', 'company_id', 'customer_id',
            'operator_id', 'distribution_center_code_id', 'destiny_code_id',
            'destination_code', 'destination_address'
        ])

        final_columns = [
            'freight_id', 'freight_code', 'freight_date', 'company_id', 'customer_id', 'operator_id',
            'distribution_center_code_id', 'destiny_code_id', 'destination_code', 'destination_address',
            'freight_type_id', 'freight_status_id', 'customer_order_code', 'travelled_distance',
            'vehicle_id', 'pre_invoice', 'started_date', 'created_at', 'status',
            'duration_days', 'is_long_distance', 'day_of_week'
        ]

        print("Sample of destination columns:")
        print(combined[['destination_code', 'destination_address']].dropna(how='all').head())

        if combined.empty:
            print("The DataFrame is empty. No data to load.")

        return combined[final_columns]
