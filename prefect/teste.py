import duckdb
import subprocess
from prefect import task, flow

@task
def save_csv_as_table():

    conn = duckdb.connect("data/dev.duckdb")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS songs_bronze AS
        SELECT *
        FROM read_csv("data/top_100_spotify_songs_2025.csv")
    """)
    
    return True

@task
def run_dbt_models():
    
    subprocess.run(
        ["dbt", "run", "--project-dir", "./dbt/learning/"]
    )
    
    return True

@task
def create_docs():
    
    subprocess.run(
        ["dbt", "docs", "generate","--project-dir", "./dbt/learning/"]
    )
    
    return True

@flow
def etl():
    result = save_csv_as_table()
    result2 = run_dbt_models()
    result3 = create_docs()
    print(result, result2, result3)
    
etl()