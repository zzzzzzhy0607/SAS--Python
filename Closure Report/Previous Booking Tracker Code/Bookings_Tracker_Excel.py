import datetime
import time
import pyodbc
import pandas as pd

LOC_QUERY = r'\\xxxx\SQL\Bookings_Tracker.txt'
LOC_OUTPUT = r'\\xxxx\Data\Bookings_Tracker_Data.csv'
LOC_HISTORICAL = r'\\xxxx\Data\Historical\Bookings_Tracker_Data_'+datetime.datetime.now().strftime('%Y%m%d%H%M')+'.csv'
SERVER_NAME = 'xxx.xxx.xx.xxx,xxxxx'
DATABASE_NAME = 'xxxxxxxxx'

def execute_sql_query(server_name, database_name,query_path):
    print('Executing query to '+database_name+'...')
    start_time = time.time()
    conn = pyodbc.connect(
    r'DRIVER={SQL Server Native Client 11.0};'
    r'SERVER='+server_name+';'
    r'UID=xxxxxxxxx;'
    r'PWD=xxxxxxxxxx;'
    r'DATABASE='+database_name+';'
    )
    with open(query_path,'r') as myfile:
        sql = myfile.read()
    df = pd.read_sql(sql,con=conn)
    print('Query completed in '+str(time.time()-start_time)+'s')
    df.columns = df.columns.str.upper()
    return df

bookings_tracker_df = execute_sql_query(SERVER_NAME, DATABASE_NAME, LOC_QUERY)

with pd.ExcelWriter(r'\\xxxx\Data\Bookings_Tracker_Data.xlsx') as writer:
    bookings_tracker_df.to_excel(writer,index=False)
