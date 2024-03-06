from mysql_functions import get_input_sql_credentials, get_npx_data_table, ask_yes_no, scan_directory, check_local_remote_consistency, read_local_reference_csv, update_recordings_in_database
import pandas as pd
import os
from pathlib import Path
from distutils.dir_util import copy_tree

def update_local_information(path):
    csv_df = read_local_reference_csv(path)
    csv_df["transferred_to_NAS"] = 1
    csv_df.to_csv(path/"db_conf.csv")

data_directory = Path.home() / 'RANCZLAB-NAS/data/ONIX'
NAS_path = Path.home() / 'RANCZLAB-NAS/data/ONIX'
print("<<<DATA TRANSFER SCRIPT>>>")

# Get credentials and validate them
hostname, user, pwd = get_input_sql_credentials()
correct_login = False
while not correct_login:
    try:
        _ = get_npx_data_table(hostname, user, pwd)
        correct_login = True
    except Exception as e:
        print(f'Login failed with error: {str(e)}\nPlease try again:')

print("This script works only on data which has already been registered in the database. Please first run 'register_new_data_script.py' if there are any unregistered local directories.")

# Scan the directory
local_referenced_folders, local_unreferenced_folders = scan_directory(data_directory)

# Checking if all locally referenced folders exist in the database
check_local_remote_consistency(hostname, user, pwd, local_referenced_folders)

# Check which locally referenced folders have been transfered to the NAS
print("\nChecking which locally referenced folders have been transfered to NAS storage:")
local_referenced_transferred_folders = []
local_referenced_untransferred_folders = []
for folder_path in local_referenced_folders:
    csv_df = read_local_reference_csv(folder_path)
    if (csv_df["transferred_to_NAS"] == 0).item():
        local_referenced_untransferred_folders.append(folder_path)
    elif (csv_df["transferred_to_NAS"] == 1).item():
        local_referenced_transferred_folders.append(folder_path)
print(f"Found {len(local_referenced_transferred_folders)} transferred directories and {len(local_referenced_untransferred_folders)} untransferred directories.")
if ask_yes_no('List them?'):
    print('Transferred directories:')
    print(*local_referenced_transferred_folders,sep='\n')
    print('Untransferred directories:')
    print(*local_referenced_untransferred_folders,sep='\n')


if len(local_referenced_untransferred_folders) != 0:
    if ask_yes_no("\nProceed with the transfer of found directories?"):
        actually_transferred_folders = []
        if ask_yes_no("Transfer all together [answer 'y'] or one by one [answer 'n']?"):
            try:
                for folder_path in local_referenced_untransferred_folders:
                    copy_tree(str(folder_path), str(Path(NAS_path) / Path(folder_path).name))
                    actually_transferred_folders.append(folder_path)
                    print(f"Finished transfer of: {folder_path}")
            except Exception as e:
                print(f"Transfer failed with error: {str(e)}")
        else:
            for folder_path in local_referenced_untransferred_folders:
                if ask_yes_no(f"Transfer {folder_path} ?"):
                    try:
                        copy_tree(str(folder_path), str(Path(NAS_path) / Path(folder_path).name))
                        actually_transferred_folders.append(folder_path)
                    except Exception as e:
                        print(f"Transfer failed with error: {str(e)}")
        print("All directories transferred successfully.")


        # Update the "transferred_to_NAS" field for local references
        for folder_path in actually_transferred_folders:
            update_local_information(folder_path)
            update_local_information(Path(NAS_path) / Path(folder_path).name)
        print("Local information updated for transferred directories.")
                
        # Update the "transferred_to_NAS" field for database references
        query_values = []
        for folder_path in actually_transferred_folders:
            csv_df = read_local_reference_csv(folder_path)
            query_values.append((csv_df["transferred_to_NAS"].item(), csv_df["recording_id"].item()))
        try:
            update_recordings_in_database(hostname, user, pwd, update_column_name="transferred_to_NAS", where_column_name="recording_id", query_values=query_values)
            print("Database information updated for transferred directories.")
        except Exception as e:
            print(f"Database information update failed with error: {str(e)}")