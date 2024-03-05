# Adds new datafiles to the database
import os
import numpy as np
import pandas as pd
from pathlib import Path
from mysql_functions import get_input_sql_credentials, get_npx_data_table, ask_yes_no, scan_directory, check_local_remote_consistency, add_recordings_to_database

def parse_recording_folder_name(path):
    animal_id = path.parts[-1].split('_')[0]
    date, time = path.parts[-1].split('_')[-1].split('T')
    time = time[:6]
    time = f'{time[0:2]}:{time[2:4]}:{time[4:]}'
    return animal_id, date, time

data_directory = Path.home() / 'RANCZLAB-NAS/data/ONIX'
print("<<<DATA REGISTRATION SCRIPT>>>")

# Get credentials and validate them
hostname, user, pwd = get_input_sql_credentials()
correct_login = False
while not correct_login:
    try:
        _ = get_npx_data_table(hostname, user, pwd)
        correct_login = True
    except Exception as e:
        print(f'Login failed with error: {str(e)}\nPlease try again:')

# Scan the directory
local_referenced_folders, local_unreferenced_folders = scan_directory(data_directory)

# Checking if all locally referenced folders exist in the database
check_local_remote_consistency(hostname, user, pwd, local_referenced_folders)

if ask_yes_no('\nAdd local unreferenced recordings to the SQL database?'):
    # Get the last recording_id from the database
    db_df = get_npx_data_table(hostname, user, pwd)
    all_recording_ids = np.sort(db_df['recording_id'].unique())
    if len(all_recording_ids)==0:
        last_recording_id = -1
    else:
        last_recording_id = all_recording_ids[-1]

    query_values = []

    # Extract metadata from the recording folder and save it to the database
    for folder_path in local_unreferenced_folders:
        animal_id, date, time = parse_recording_folder_name(folder_path)
        last_recording_id += 1
        animal_id = last_recording_id # REMOVE AFTER ADOPTING FOLDER NAMING CONVENTION
        condition_id = last_recording_id # DEFINE AFTER ADOPTING FOLDER NAMING CONVENTION
        condition_info = 'test data' # DEFINE AFTER ADOPTING FOLDER NAMING CONVENTION
        transferred_to_NAS = 0
        query_values.append((last_recording_id, animal_id, date, time, condition_id, condition_info, transferred_to_NAS))
        local_recording_df = pd.DataFrame([{'recording_id':last_recording_id, 'animal_id':animal_id, 'date':date, 'time':time, 'condition_id':condition_id, 'condition_info':condition_info, 'transferred_to_NAS':transferred_to_NAS}])
        local_recording_df.to_csv(folder_path/'db_conf.csv')
    
    # Make the query to SQL
    add_recordings_to_database(hostname, user, pwd, query_values)