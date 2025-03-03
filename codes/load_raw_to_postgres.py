from dotenv import load_dotenv
import duckdb
import os
from tqdm import tqdm
from pathlib import Path


dataset_orders = Path("../datasets/raw_data/orders")
dataset_registers = Path("../datasets/raw_data/registers")

for env_variable in ["POSTGRES_DBHOST", "POSTGRES_DBUSER", "POSTGRES_DATABASE", "POSTGRES_DBPASSW", "POSTGRES_DBPORT"]:
    os.environ.pop(env_variable, None)


load_dotenv(override=True)

POSTGRES_DBHOST = os.getenv("POSTGRES_DBHOST")
POSTGRES_DBUSER = os.getenv("POSTGRES_DBUSER")
POSTGRES_DATABASE = os.getenv("POSTGRES_DATABASE")
POSTGRES_DBPASSW = os.getenv("POSTGRES_DBPASSW")
POSTGRES_DBPORT = os.getenv("POSTGRES_DBPORT")


conn_duckdb = duckdb.connect()
conn_duckdb.execute('INSTALL postgres_scanner;')
conn_duckdb.execute("LOAD postgres_scanner;")
conn_duckdb.execute(f"ATTACH  'dbname={POSTGRES_DATABASE} user={POSTGRES_DBUSER} host={POSTGRES_DBHOST} password={POSTGRES_DBPASSW} port={POSTGRES_DBPORT}' AS postgres (TYPE POSTGRES, SCHEMA 'public');")


def load_parquet_to_postgres(path_parquet_files, table):
    all_parquet_files = list(path_parquet_files.glob("*.parquet"))
    for file in tqdm(all_parquet_files, desc=f"Uploading data to {table}"):
        query = f"CREATE TABLE IF NOT EXISTS postgres.main.{table} AS SELECT * FROM read_parquet('{file}') LIMIT 0"
        conn_duckdb.execute(query)

        query_load = f"""
                INSERT INTO postgres.main.{table}
                SELECT * FROM read_parquet('{file}')
            """
        conn_duckdb.execute(query_load)

try:
    load_parquet_to_postgres(dataset_orders, 'orders')
    load_parquet_to_postgres(dataset_registers, 'registers')
except Exception as ex:
    print("Erro: ", ex)

finally:
    conn_duckdb.close()