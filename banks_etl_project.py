# Code for ETL operations on List_of_largest_banks data

# Importing the required libraries
from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime 

url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
csv_path = "exchange_rate.csv"
table_attribs = ["Name", "MC_USD_Billion"]
final_table_attributes = ["Name", "MC_USD_Billion", "MC_GBP_Billion", "MC_EUR_Billion", "MC_INR_Billion"]
output_path = "./Largest_banks_data.csv"
database_name = "Banks.db"
table_name = "Largest_banks"
log_file = "code_log.txt"

def log_progress(message): 
    timestamp_format = "%Y-%h-%d-%H:%M:%S"
    now = datetime.now()
    timestamp = now.strftime(timestamp_format) 
    with open("./code_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')

def extract(url, table_attribs):
    page = requests.get(url).text
    data = BeautifulSoup(page,'html.parser')
    header = data.find('span', {'id': 'By_market_capitalization'})
    table = header.find_next('table', {'class': 'wikitable'})

    row_data = []
    rows = table.find_all('tr')[1:]
    for row in rows:
        cols = row.find_all('td')
        if len(cols) >= 3:
            bank_name = cols[1].get_text(strip=True)
            market_cap = float(cols[2].get_text(strip=True))
            row_data.append((bank_name, market_cap))
    df = pd.DataFrame(row_data, columns=table_attribs)

    return df

def transform(df, csv_path):
    exchange_rates_df = pd.read_csv(csv_path)

    exchange_rates = exchange_rates_df.set_index('Currency').to_dict()['Rate']

    df['MC_GBP_Billion'] = np.round(df['MC_USD_Billion'] * exchange_rates['GBP'], 2)
    df['MC_EUR_Billion'] = np.round(df['MC_USD_Billion'] * exchange_rates['EUR'], 2)
    df['MC_INR_Billion'] = np.round(df['MC_USD_Billion'] * exchange_rates['INR'], 2)

    return df

def load_to_csv(df, output_path):
    df.to_csv(output_path)

def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

log_progress('Preliminaries complete. Initiating ETL process')
df = extract(url, table_attribs)
log_progress('Data extraction complete. Initiating Transformation process')
df = transform(df, csv_path)
log_progress('Data transformation complete. Initiating loading process')
load_to_csv(df, output_path)
log_progress('Data saved to CSV file')
sql_connection = sqlite3.connect(database_name)
log_progress('SQL Connection initiated.')
load_to_db(df, sql_connection, table_name)
log_progress('Data loaded to Database as a table, Executing queries')
query_statement = f"SELECT * FROM Largest_banks"
run_query(query_statement, sql_connection)
query_statement = f"SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
run_query(query_statement, sql_connection)
query_statement = f"SELECT Name from Largest_banks LIMIT 5"
run_query(query_statement, sql_connection)
log_progress('Process Complete.')
sql_connection.close()
log_progress('Server Connection closed')


