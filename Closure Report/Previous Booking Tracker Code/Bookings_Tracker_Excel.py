import datetime
import time
import pyodbc
import pandas as pd

LOC_QUERY = r'\\goaspen\Department_Shares\PricingRevMan\Analytics\02 - Infrastructure\Bookings Tracker\SQL\Bookings_Tracker.txt'
LOC_OUTPUT = r'\\goaspen\Department_Shares\PricingRevMan\Analytics\02 - Infrastructure\Bookings Tracker\Data\Bookings_Tracker_Data.csv'
LOC_HISTORICAL = r'\\goaspen\Department_Shares\PricingRevMan\Analytics\02 - Infrastructure\Bookings Tracker\Data\Historical\Bookings_Tracker_Data_'+datetime.datetime.now().strftime('%Y%m%d%H%M')+'.csv'
SERVER_NAME = '149.122.13.215,52900'
DATABASE_NAME = 'REZF9OD01'

def execute_sql_query(server_name, database_name,query_path):
    print('Executing query to '+database_name+'...')
    start_time = time.time()
    conn = pyodbc.connect(
    r'DRIVER={SQL Server Native Client 11.0};'
    r'SERVER='+server_name+';'
    r'UID=F9ODSUSER;'
    r'PWD=kUhuhAK3th;'
    r'DATABASE='+database_name+';'
    )
    with open(query_path,'r') as myfile:
        sql = myfile.read()
    df = pd.read_sql(sql,con=conn)
    print('Query completed in '+str(time.time()-start_time)+'s')
    df.columns = df.columns.str.upper()
    return df

bookings_tracker_df = execute_sql_query(SERVER_NAME, DATABASE_NAME, LOC_QUERY)

with pd.ExcelWriter(r'\\goaspen\Department_Shares\PricingRevMan\Analytics\02 - Infrastructure\Bookings Tracker\Data\Bookings_Tracker_Data.xlsx') as writer:
    bookings_tracker_df.to_excel(writer,index=False)