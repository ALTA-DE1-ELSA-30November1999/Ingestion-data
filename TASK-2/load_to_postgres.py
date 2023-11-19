import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import BigInteger, String, JSON, Boolean, Float, Integer, DateTime

def get_data_frame () :
    df  = pd.read_parquet('../dataset/yellow_tripdata_2023-01.parquet', engine="pyarrow")
    return df

def get_manipulate_data(df) :
    df.dropna(inplace=True)

    df['passenger_count'] = df['passenger_count'].astype('int8')
    
    df['store_and_fwd_flag'] = df['store_and_fwd_flag'].replace(['N', 'Y'], [True, False])
    df['store_and_fwd_flag'] = df['store_and_fwd_flag'].astype('boolean')
    return df

def get_postgres_conn():
    user = 'user'
    password = 'pass'
    host = 'localhost'
    database = 'postgres'
    port = 5440
    conn_string = f'postgresql://{user}:{password}@{host}:{port}/{database}'
    engine = create_engine(conn_string)
    return engine

def load_to_postgres(engine) :
    df_schema = {
        'VendorID':BigInteger,
        'tpep_pickup_datetime':DateTime,
        'tpep_dropoff_datetime':DateTime,
        'passenger_count':BigInteger,
        'trip_distance':Float,
        'RatecodeID':Float,
        'store_and_fwd_flag':Boolean,
        'PULocationID':BigInteger,
        'DOLocationID':BigInteger,
        'payment_type':BigInteger,
        'fare_amount':Float,
        'extra':Float,
        'mta_tax':Float,
        'tip_amount':Float,
        'tolls_amount':Float,
        'improvement_surcharge':Float,
        'total_amount':Float,
        'congestion_surcharge':Float,
        'airport_fee':Float
        }

    df.to_sql(name='Tugas-elsa', 
              con=engine, if_exists='replace',
              index=False, schema='public', 
              dtype=df_schema, method=None, 
              chunksize=5000)


df = get_data_frame()
# print(df.dtypes)
# print('-----------------------------')
clean_data = get_manipulate_data(df)
# print(clean_data)

postgres_conn = get_postgres_conn()
# print(postgres_conn)

load_to_postgres(postgres_conn)