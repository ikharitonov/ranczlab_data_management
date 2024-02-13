# Adds new datafiles to the database
import os
import numpy as np
import pandas as pd
from pathlib import Path
from mysql_functions import get_input_sql_credentials, get_npx_data_table, add_recordings_to_database

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

def read_local_reference_csv(path):
    return pd.read_csv(path/'db_conf.csv')

def parse_recording_folder_name(path):
    animal_id = path.split('/')[-1].split('_')[0]
    date, time = path.split('/')[-1].split('_')[-1].split('T')
    time = time[:6]
    time = f'{time[0:2]}-{time[2:4]}-{time[4:]}'
    return animal_id, date, time

data_directory = Path.home() / 'RANCZLAB-NAS/data/onix'

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
local_unreferenced_folders = []
local_referenced_folders = []
for folder in os.listdir(data_directory):
    if 'db_conf.csv' not in os.listdir(data_directory/folder):
        local_unreferenced_folders.append(data_directory/folder)
    else:
        local_referenced_folders.append(data_directory/folder)

print(f"Scanned {data_directory} data directory. Found {len(local_referenced_folders)} SQL database referenced and {len(local_unreferenced_folders)} non-referenced folders.")
if ask_yes_no('List them?'):
    print('Referenced folders:')
    print(*local_referenced_folders,sep='\n')
    print('Unreferenced folders:')
    print(*local_unreferenced_folders,sep='\n')

if ask_yes_no('Check if all locally referenced folders exist in the database?'):
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

    print(f"Checked consistency of locally referenced data folders (recordings) with the SQL database. Found {len(db_referenced_folders)} recordings referenced both locally and in the database; {len(db_unreferenced_folders)} referenced locally but not in the database; {len(db_problem_folders)} recordings with potential duplicate references in the database.")

    if ask_yes_no('List them?'):
        print('Recordings with consistent reference:')
        print(*db_referenced_folders,sep='\n')
        print('Recordings with inconsistent reference:')
        print(*db_unreferenced_folders,sep='\n')
        print('Recordings with possible duplicate reference:')
        print(*db_problem_folders,sep='\n')

if ask_yes_no('Add local unreferenced recordings to the SQL database?'):
    # Get the last recording_id from the database
    db_df = get_npx_data_table(hostname, user, pwd)
    last_recording_id = np.sort(db_df['recording_id'].unique())[-1]

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
    
    # Make the query to SQL
    add_recordings_to_database(hostname, user, pwd, query_values)