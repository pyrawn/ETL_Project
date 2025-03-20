class Transformer:
    """Applies transformations to the extracted data."""

    def clean_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardizes column names (lowercase, no spaces)."""
        df.columns = df.columns.str.lower().str.replace(' ', '_')
        return df

    def drop_null_values(self, df: pd.DataFrame, threshold: float = 0.7) -> pd.DataFrame:
        """Drops columns with too many null values (default: >70%)."""
        return df.dropna(thresh=int(threshold * len(df)), axis=1)

    def convert_data_types(self, df: pd.DataFrame, column_types: dict) -> pd.DataFrame:
        """Converts column data types based on a given dictionary."""
        return df.astype(column_types)