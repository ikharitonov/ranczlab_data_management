import mysql.connector as sql
import pandas as pd
from getpass import getpass

def get_input_sql_credentials():
    input_hostname = input("Hostname: ")
    input_user = input("Username: ")
    # input_pwd = input("Password: ")
    input_pwd = getpass()
    return input_hostname, input_user, input_pwd

def get_npx_data_table(hostname, user, pwd):
    db = sql.connect(host=hostname, user=user, password=pwd, port=3306, database='RPM_NPX_DB')

    query = "SELECT * FROM NPX_Data;"
    df = pd.read_sql(query,db)
    db.close()
    return df

def add_recordings_to_database(hostname, user, pwd, query_values):
    query = "INSERT INTO NPX_Data (recording_id, animal_id, date, time, condition_id, condition_info, transferred_to_NAS) VALUES (%s, %s, %s, %s, %s, %s, %s)"

    if len(query_values)==0:
        print('No information extracted from unreferenced recordings, skipping connection to the database.')
    else:
        db = sql.connect(host=hostname, user=user, password=pwd, port=3306, database='RPM_NPX_DB')

        cursor = db.cursor()
        cursor.executemany(query, query_values)
        db.commit()

        print(cursor.rowcount, " rows inserted into SQL table.")