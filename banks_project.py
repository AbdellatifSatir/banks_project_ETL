import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import sqlite3
from datetime import datetime


# Task 1: Logging function
def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    timestamp = datetime.now().strftime('%Y-%h-%d-%H:%M:%S')
    with open('code_log.txt','a') as f:
        f.write(timestamp + ':' + message + '\n')


# Task 2 : Extraction of data
def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    html = requests.get(url).text
    soup = BeautifulSoup(html,'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = soup.find_all('tbody')
    rows = tables[0].find_all('tr')
    for row in rows:
        col = row.find_all('td')
        if len(col)!=0:
            data_dict = {
                'Name' : col[1].text.strip(),
                'MC_USD_Billion' : col[2].text.strip()
            }
            df1 = pd.DataFrame(data=data_dict, index=[0])
            df = pd.concat([df,df1], ignore_index=True)
    return df


# Task 3 : Transformation of data
def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    exchange_rate = pd.read_csv(csv_path)
    exchange_rate = exchange_rate.set_index('Currency').to_dict()['Rate']
    df1 = df.copy()
    df1['MC_USD_Billion'] = pd.to_numeric(df1['MC_USD_Billion'])
    df1['MC_GBP_Billion'] = round(df1['MC_USD_Billion'] * exchange_rate.get('GBP'), 2)
    df1['MC_EUR_Billion'] = round(df1['MC_USD_Billion'] * exchange_rate.get('EUR'), 2)
    df1['MC_INR_Billion'] = round(df1['MC_USD_Billion'] * exchange_rate.get('INR'), 2)
    return df1


# Task 4: Loading to CSV
def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path)

# Task 5: Loading to Database
def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists='replace')


# Task 6: Function to Run queries on Database
def run_queries(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)



url = 'https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'
csv_path = './exchange_rate.csv'
output_path = './Largest_banks_data.csv'
table_attribs = ['Name', 'MC_USD_Billion']
table_attribs_final = ['Name', 'MC_USD_Billion', 'MC_GBP_Billion', 'MC_EUR_Billion', 'MC_INR_Billion']
db_name = 'Banks.db'
table_name = 'Largest_banks'


log_progress('Preliminaries complete. Initiating ETL process')
df = extract(url, table_attribs)
# print(df)

log_progress('Data extraction complete. Initiating Transformation process')
df1 = transform(df, csv_path)
print(df1)

log_progress('Data transformation complete. Initiating Loading process')
load_to_csv(df1, output_path)
log_progress('Data saved to CSV file')

sql_connection = sqlite3.connect('Banks.db')
log_progress('SQL Connection initiated')
load_to_db(df1, sql_connection, table_name='Largest_banks')
log_progress('Data loaded to Database as a table, Executing queries')

query_statement = f"SELECT * FROM Largest_banks"
run_queries(query_statement, sql_connection)
print('\n')
query_statement = f"SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
run_queries(query_statement, sql_connection)
print('\n')
query_statement = f"SELECT Name from Largest_banks LIMIT 5"
run_queries(query_statement, sql_connection)

log_progress('Process Complete')
sql_connection.close()
log_progress('Server Connection closed')
