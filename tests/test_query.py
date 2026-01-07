
import sqlite3
import pandas as pd

#df = pd.DataFrame()

with sqlite3.connect(f'fetch_result.sqlite3') as sqlite_db:
    df = pd.read_sql_query('SELECT * FROM wo_attachments',sqlite_db)
    
    
    wo_id = 81192596
    query = df.query('wo_id == @wo_id')
    
    print(query)