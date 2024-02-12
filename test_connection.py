import mysql.connector as sql
import pandas as pd

db = sql.connect(host='10.51.106.97', user='nora', password='nora', port=8080, database='RPM_NPX_DB')

query = "SELECT * FROM NPX_Data;"
df = pd.read_sql(query,db)
db.close()

print(df)