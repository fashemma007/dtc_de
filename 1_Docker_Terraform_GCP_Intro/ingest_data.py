from sqlalchemy import create_engine
from time import time
import pandas as pd
import argparse as argparse #used to pass in command line arguments
import os
from pathlib import Path


#=========PARAMS==========
# user,
# password,
# host,
# port,
# database name,
# table name,
# url of CSV

"""
URL="https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-01.csv"
python ingest_data.py \
  --user=root \
  --password=password \
  --host=localhost \
  --port=5431 \
  --db=ny_taxi \
  --table_name=yellow_taxi_trips \
  --url=${URL}
"""

def main(params):
   user = params.user
   password = params.password
   host = params.host
   port = params.port
   db = params.db
   table_name = params.table_name
   url = params.url
   
   # download the csv file
   
   csv_name = 'output.csv'
   file_name = Path("output.csv")
   
   if file_name.exists():
      pass
   else:
      os.system(f"wget {url} -O {csv_name}")
   
   # Initializing our connection to the server
   # engine = create_engine('postgresql://root:password@localhost:5431/ny_taxi')
   # engine.connect()
   
   engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")
  
   #==============================================================
   #           UPLOADING ZONES LOOK_UP FILE
   #==============================================================
   zone_file_name = "zone_lookup.csv"
   zone_file = Path("zone_lookup.csv")
   zone_url = "https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv"
   if zone_file.exists():
      pass
   else:
      os.system(f"wget {zone_url} -O {zone_file_name}")
   
   df_zones = pd.read_csv(zone_file_name)
   df_zones.to_sql(name='zones',con=engine,if_exists='replace')
  #============================================================== 
   
   # to split our full data into chunks
   dtf_iter = pd.read_csv(csv_name,iterator=True, chunksize =100000)
   
   dtf = next(dtf_iter)
   # to convert the data in the drop off and pickup columns to date time
   dtf.tpep_dropoff_datetime = pd.to_datetime(dtf.tpep_dropoff_datetime)   
   dtf.tpep_pickup_datetime = pd.to_datetime(dtf.tpep_pickup_datetime)   
   
   dtf.head(n=0).to_sql(name=table_name,con=engine,if_exists='replace')
   
   #Importing our first 100k rows before iterator
   dtf.to_sql(name=table_name,con=engine,if_exists='append')
   
   while True:
      try:
         t_start = time()
         dtf = next(dtf_iter)
         
         # to convert the data in the drop off and pickup columns to date time
         dtf.tpep_dropoff_datetime = pd.to_datetime(dtf.tpep_dropoff_datetime)
         
         dtf.tpep_pickup_datetime = pd.to_datetime(dtf.tpep_pickup_datetime)   
   
      
         dtf.to_sql(name=table_name,con=engine,if_exists='append')
         t_end = time()
         diff = t_end - t_start 
         print (f'Another 100k rows of data uploaded in %.3f seconds' %(diff))
      except StopIteration:
         print ('Done uploading')
         break

# cursor = engine.connect() # creating a cursor to run sql queries with
# cursor.execute("""CREATE TABLE yellow_taxi_data (
# 	"VendorID" BIGINT, 
# 	tpep_pickup_datetime TIMESTAMP WITHOUT TIME ZONE, 
# 	tpep_dropoff_datetime TIMESTAMP WITHOUT TIME ZONE, 
# 	passenger_count BIGINT, 
# 	trip_distance FLOAT(53), 
# 	"RatecodeID" BIGINT, 
# 	store_and_fwd_flag TEXT, 
# 	"PULocationID" BIGINT, 
# 	"DOLocationID" BIGINT, 
# 	payment_type BIGINT, 
# 	fare_amount FLOAT(53), 
# 	extra FLOAT(53), 
# 	mta_tax FLOAT(53), 
# 	tip_amount FLOAT(53), 
# 	tolls_amount FLOAT(53), 
# 	improvement_surcharge FLOAT(53), 
# 	total_amount FLOAT(53), 
# 	congestion_surcharge FLOAT(53)
# )""")

if __name__ == '__main__':
   parser = argparse.ArgumentParser(description='Ingest CSV data to postgresql database.')
   
   parser.add_argument('--user',required=True, help='username for postgreSql account')
   parser.add_argument('--password',required=True,help='password for postgreSql account')
   parser.add_argument('--host',required=True,help='host for postgreSql account')
   parser.add_argument('--port',required=True,help='port for postgreSql account')
   parser.add_argument('--db',required=True,help='db_name for postgreSql connection')
   parser.add_argument('--table_name',required=True,help='table-name where results will be written to')
   parser.add_argument('--url',required=True,help='url of our csv to uploaded')
   
   args = parser.parse_args()

   main(args)
   