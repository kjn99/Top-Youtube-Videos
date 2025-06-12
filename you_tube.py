import pandas as pd
import os
from sqlalchemy import create_engine
import urllib.parse


# PostgreSQL config
DB_CONFIG = {
    'user': 'youtube',
    'password': 'w0nder@1313',
    'host': 'localhost',
    'port': '5432',
    'database': 'youtube'
}

def get_engine():
    password = urllib.parse.quote_plus(DB_CONFIG['password'])  
    return create_engine(
        f"postgresql+psycopg2://{DB_CONFIG['user']}:{password}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )

def clean_column_names(df):
    df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]
    return df

def process_csv(file_path, country_code):
    df = pd.read_csv(file_path, encoding="latin1")
    df = clean_column_names(df)
    df['country'] = country_code
    df['publish_time'] = pd.to_datetime(df['publish_time'], errors='coerce')
    df = df[df['views'] > 100000]
    return df

def load_to_postgres(df, table_name='youtube_trending_videos'):
    engine = get_engine()
    df.to_sql(table_name, engine, if_exists='append', index=False)

def etl_pipeline(file_country_map):
    for file_path, country_code in file_country_map.items():
        df = process_csv(file_path, country_code)
        load_to_postgres(df)

if __name__ == "__main__":
    file_country_map = {
        'D:/pythone_code/you/USvideos.csv': 'US',
        'D:/pythone_code/you/CAvideos.csv': 'CA',
        'D:/pythone_code/you/DEvideos.csv': 'DE',
        'D:/pythone_code/you/FRvideos.csv': 'FR',
        'D:/pythone_code/you/GBvideos.csv': 'GB',
        'D:/pythone_code/you/INvideos.csv': 'IN',
        'D:/pythone_code/you/JPvideos.csv': 'JP',
        'D:/pythone_code/you/KRvideos.csv': 'KR',
        'D:/pythone_code/you/MXvideos.csv': 'MX',
        'D:/pythone_code/you/RUvideos.csv': 'RU',
    }
    etl_pipeline(file_country_map)