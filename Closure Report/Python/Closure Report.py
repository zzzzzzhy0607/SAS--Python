# -*- coding: utf-8 -*-
"""
Created on Wed Jul 17 15:19:57 2019
@ author: Hongyang Zheng
"""


# Import libraries
import datetime
import pyodbc
import pandas as pd


"""
# Optional
# Function to get the process date
def get_process_date(n):
    today=datetime.datetime.now()
    process_date=today-datetime.timedelta(days=n)
    
    # Format the process date : YYYY-MM-DD and return
    return(process_date.strftime("%Y-%m-%d"))
    
# Get the process date    
process_date=get_process_date(1)
process_date_1=get_process_date(2)
process_date_3=get_process_date(4)
process_date_7=get_process_date(8)
process_date_14=get_process_date(15)
process_date_364=get_process_date(366)
process_date_371=get_process_date(370)
"""


# Function to execute query and return dataframe
def execute_sql_query(query_path):  
    print('Executing query of '+ query_path +'...')
    
    conn = pyodbc.connect(
    'DRIVER={SQL Server};'
    'SERVER=FDCSQLCE04P;'
    'DATABASE=RM_Reports;'
    'Trusted_Connection=yes;'
    )
    
    with open(query_path,'r') as myfile:
        sql = myfile.read()
        
    return pd.read_sql(sql,con=conn)

# Generate dataframe
Current = execute_sql_query('\\\\goaspen\\Department_Shares\\PricingRevMan\\Analytics\\09 - Team\\2-Interns\\Hongyang\\Closure Report\\SQL\\Current.txt')
Current_LY = execute_sql_query('\\\\goaspen\\Department_Shares\\PricingRevMan\\Analytics\\09 - Team\\2-Interns\\Hongyang\\Closure Report\\SQL\\CurrentLY.txt')
Current_1 = execute_sql_query('\\\\goaspen\\Department_Shares\\PricingRevMan\\Analytics\\09 - Team\\2-Interns\\Hongyang\\Closure Report\\SQL\\Current_1.txt')


# Function to create N-cls based on the condition of N-avl
def create_cls(df):
    
    list1=["Yavl", "Bavl", "Havl", "Vavl", "Lavl", "Uavl", "Eavl", "Davl",
           "Mavl", "Qavl", "Tavl", "Gavl", "Wavl", "Ravl", "Zavl", "Kavl"]
    list2=["Ycls", "Bcls", "Hcls", "Vcls", "Lcls", "Ucls", "Ecls", "Dcls",
       "Mcls", "Qcls", "Tcls", "Gcls", "Wcls", "Rcls", "Zcls", "Kcls"]   
   
    for old, new in zip(list1, list2):
        df[new]=df[old].apply(lambda x: 1 if x <=0 else 0)
    
    # Drop useless columns
    df.drop(df.iloc[:, 5:21], inplace=True, axis=1)
    
    # Sort the dataframe by org dest departuredate
    df.sort_values(['org', 'dest', 'departuredate'])
    
    return(df)
    
# Add N-cls to current dataframe
Current_a=create_cls(Current) 
Current_LY_a=create_cls(Current_LY) 
Current_1_a=create_cls(Current_1)


# Function to simulate proc summary in SAS
def proc_summary(df, column_name):
    
    # Sum and Count
    df_new=df.groupby(['org','dest', 'departuredate']).agg(['count', 'sum'])
    # Drop useless column
    df_new.drop(df_new.columns[[2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32, 34]], inplace=True, axis=1)
    # Rename columns
    df_new.columns = column_name
    # Add columns
    df_final=df_new.reset_index()
    
    # Sort the dataframe by org dest departuredate
    df_final.sort_values(['org', 'dest', 'departuredate'])

    return(df_final)

# Generate dataframe with summary stats  
name_C=["Count", "RPMS", "ASMS", "Ycls", "Bcls", "Hcls", "Vcls", "Lcls", "Ucls", "Ecls", "Dcls",
        "Mcls", "Qcls", "Tcls", "Gcls", "Wcls", "Rcls", "Zcls", "Kcls"]
name_LY=["LY_Count", "LY_RPMS", "LY_ASMS", "LY_Ycls", "LY_Bcls", "LY_Hcls", "LY_Vcls", "LY_Lcls", "LY_Ucls", "LY_Ecls", 
         "LY_Dcls", "LY_Mcls", "LY_Qcls", "LY_Tcls", "LY_Gcls", "LY_Wcls", "LY_Rcls", "LY_Zcls", "LY_Kcls"]    
name_Pre=["Pre_Count", "Pre_RPMS", "Pre_ASMS", "Pre_Ycls", "Pre_Bcls", "Pre_Hcls", "Pre_Vcls", "Pre_Lcls", "Pre_Ucls", 
          "Pre_Ecls", "Pre_Dcls", "Pre_Mcls", "Pre_Qcls", "Pre_Tcls", "Pre_Gcls", "Pre_Wcls", "Pre_Rcls", "Pre_Zcls", "Pre_Kcls"]

Current_aa = proc_summary(Current_a, name_C)
Current_LY_aa = proc_summary(Current_LY_a, name_Pre)
Current_1_aa = proc_summary(Current_1_a, name_Pre)

   
# Merge the dataframe 
# Outer merge
s1 = pd.merge(Current_aa, Current_LY_aa, how='outer', on=['org', 'dest', 'departuredate'])
s2 = pd.merge(s1, Current_1_aa, how='outer', on=['org', 'dest', 'departuredate'])
# Fill NAs
merged_df = s2.fillna(0)

# Function to add Month, DOW, Week, AlphaMkt and DirMkt
def add_info (df):
    df["Month"]=df["departuredate"].apply(lambda x: x.month)
    df["DOW"]=df["departuredate"].apply(lambda x: x.weekday())+1
    df['Week'] = df.apply(lambda row: (row['departuredate']-datetime.timedelta(days=row["DOW"])).strftime("%m/%d/%Y"), axis=1)
    df["AlphaMkt"] = df.apply(lambda row: row["org"]+row["dest"] if row["org"]<row["dest"] 
                              else row["dest"]+row["org"], axis=1)
    df["DirMkt"] = df.apply(lambda row: row["org"]+row["dest"], axis=1)
    
    return(df)
    
final_df=add_info(merged_df)

"""
# Optional
# Clean the data
final_df=final_df[final_df["RPMS"]>1]
"""


# Write output to csv
date=str(datetime.datetime.today().strftime("%Y%m%d"))[2:]
path = '\\\\goaspen\\Department_Shares\\PricingRevMan\\Analytics\\09 - Team\\2-Interns\\Hongyang\\Closure Report\\Output\\' + date + '_Closure_Report.csv'
final_df.to_csv(path, index=False)
    
    
 