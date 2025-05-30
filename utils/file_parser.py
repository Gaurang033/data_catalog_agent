import pandas as pd


def get_schema_and_sample_date(file_path: str, sample_size=10):
    try:
        df = pd.read_csv(file_path, nrows=sample_size)
        schema = dict(df.dtypes.apply(lambda dt: dt.name))
        return df, schema

    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return {}
