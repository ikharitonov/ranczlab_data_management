import mysql.connector as sql
import os
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

        print(cursor.rowcount, " row(s) inserted into the SQL database.")



def ask_yes_no(input_str):
    correctly_answered = False
    while not correctly_answered:
        user_input = input(f'{input_str} [answer y/n]: ')
        if user_input not in ['y', 'Y', 'yes', 'YES', 'n', 'N', 'no', 'NO']:
            print('Invalid character entered. Try again.')
        else:
            if user_input in ['y', 'Y', 'yes', 'YES']:
                return True
            else:
                return False

def scan_directory(data_directory):
    local_unreferenced_folders = []
    local_referenced_folders = []
    for folder in os.listdir(data_directory):
        if 'db_conf.csv' not in os.listdir(data_directory/folder):
            local_unreferenced_folders.append(data_directory/folder)
        else:
            local_referenced_folders.append(data_directory/folder)

    print(f"Scanned '{data_directory}' data directory. Found {len(local_referenced_folders)} SQL database referenced and {len(local_unreferenced_folders)} non-referenced folders.")
    if ask_yes_no('List them?'):
        print('Referenced folders:')
        print(*local_referenced_folders,sep='\n')
        print('Unreferenced folders:')
        print(*local_unreferenced_folders,sep='\n')
    
    return local_referenced_folders, local_unreferenced_folders

def read_local_reference_csv(path):
    return pd.read_csv(path/'db_conf.csv')

def check_local_remote_consistency(hostname, user, pwd, local_referenced_folders):
    if ask_yes_no('\nCheck if all locally referenced folders exist in the database?'):
        db_df = get_npx_data_table(hostname, user, pwd)
        db_referenced_folders = []
        db_unreferenced_folders = []
        db_problem_folders = []

        # Check consistency by recording_id
        for folder_path in local_referenced_folders:
            csv_df = read_local_reference_csv(folder_path)
            local_recording_id = csv_df['recording_id'].item()
            if len(db_df[db_df['recording_id']==local_recording_id]) == 1:
                db_referenced_folders.append(folder_path)
            elif len(db_df[db_df['recording_id']==local_recording_id]) == 0:
                db_unreferenced_folders.append(folder_path)
            else:
                db_problem_folders.append(folder_path)

        print(f"Checked consistency of locally referenced data folders (recordings) with the SQL database. Found: \n{len(db_referenced_folders)} recordings referenced both locally and in the database; \n{len(db_unreferenced_folders)} referenced locally but not in the database; \n{len(db_problem_folders)} recordings with potential duplicate references in the database.")

        if ask_yes_no('List them?'):
            print('Recordings with consistent reference:')
            print(*db_referenced_folders,sep='\n')
            print('Recordings with inconsistent reference:')
            print(*db_unreferenced_folders,sep='\n')
            print('Recordings with possible duplicate reference:')
            print(*db_problem_folders,sep='\n')

def update_recordings_in_database(hostname, user, pwd, update_column_name, where_column_name, query_values):
    # update_column_name -> the field to be update in the database
    # where_column_name -> the field used to select specific rows from the database which a value is provided for

    query = "UPDATE NPX_Data SET " + update_column_name + " = %s WHERE " + where_column_name + " = %s"

    if len(query_values)==0:
        print('No information passed for database update request. Skipping connection to the database.')
    else:
        db = sql.connect(host=hostname, user=user, password=pwd, port=3306, database='RPM_NPX_DB')

        cursor = db.cursor()
        cursor.executemany(query, query_values)
        db.commit()

        print(cursor.rowcount, "row(s) updated in the SQL database.")