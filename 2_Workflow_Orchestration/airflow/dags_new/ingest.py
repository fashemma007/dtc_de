import os,glob

from time import time

import pandas as pd
from sqlalchemy import create_engine

import pyarrow.csv as pv
import pyarrow.parquet as pq

def format_to_csv(src_file,output_csv):
    df = pd.read_parquet(src_file)
    df.to_csv(output_csv)

def ingest_callable(user, password, host, port, db, table_name, csv_file, execution_date):
    print(table_name, csv_file, execution_date)

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()

    print('Connection established successfully. \n Inserting data...')

    t_start = time()
    df_iter = pd.read_csv(csv_file, iterator=True, chunksize=100000)

    df = next(df_iter)
    
    try:
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    except Exception:
        pass
    
    try:
        df.pickup_datetime = pd.to_datetime(df.pickup_datetime)
        df.dropOff_datetime = pd.to_datetime(df.dropOff_datetime)
    except Exception:
        pass
    
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    df.to_sql(name=table_name, con=engine, if_exists='append')

    t_end = time()
    print('Inserted the first chunk, took %.3f second' % (t_end - t_start))

    while True: 
        t_start = time()

        try:
            df = next(df_iter)
        except StopIteration:
            print("completed")
            break

        try:
            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        except Exception:
            pass
        
        try:
            df.pickup_datetime = pd.to_datetime(df.pickup_datetime)
            df.dropOff_datetime = pd.to_datetime(df.dropOff_datetime)
        except Exception:
            pass

        df.to_sql(name=table_name, con=engine, if_exists='append')

        t_end = time()

        print('Inserted another chunk, took %.3f second' % (t_end - t_start))

def clean_directory(parquet,csv):
    """ Delete files """
    sourcefiles = glob.glob(parquet, recursive=True)
    for file in sourcefiles:
        os.remove(file)
    
    csv_sourcefiles = glob.glob(csv, recursive=True)
    for file in csv_sourcefiles:
        os.remove(file)