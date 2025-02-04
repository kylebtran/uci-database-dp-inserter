import pandas as pd
import aioodbc
import asyncio
import os
from dotenv import load_dotenv

load_dotenv()

server = os.getenv("SERVER")
database = os.getenv("DATABASE")
uid = os.getenv("UID")  
password = os.getenv("PWD")
driver = os.getenv("DRIVER", "ODBC Driver 17 for SQL Server")

table_name = '[CA].[stg_DegreePlan_Sample_BSAerospaceEngineering]'
csv_file_path = './sample_dp_imports/UCI_DegreePlan_Sample_BSAerospaceEngineering.csv'

dsn = f"DRIVER={{{driver}}};SERVER={server};DATABASE={database};UID={uid};PWD={password}"

async def insert_data():
    df = pd.read_csv(csv_file_path)
    
    df = df.where(pd.notna(df), None)

    columns = ', '.join(df.columns)
    placeholders = ', '.join(['?' for _ in df.columns])
    insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

    async with aioodbc.connect(dsn=dsn) as conn:
        async with conn.cursor() as cursor:
            tasks = [cursor.execute(insert_sql, tuple(row)) for row in df.itertuples(index=False, name=None)]
            await asyncio.gather(*tasks)
        await conn.commit()

asyncio.run(insert_data())

print("Data inserted successfully.")
