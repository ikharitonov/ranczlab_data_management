from mysql_functions import get_npx_data_table
import pandas as pd
import os

# Looks through the local data directory, checks all entries against the database, transfers those 

# df = get_npx_data_table()
df = pd.DataFrame([{'recording_id':1, 'animal_id':1, 'date':'2024-02-13', 'time':'14-23', 'condition_id':0, 'condition_info':'test', 'transferred_to_NAS':0}])



# print(df)

