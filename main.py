import duckdb
conn = duckdb.connect("data/dev.duckdb")
print(conn.execute("SHOW TABLES").fetchall())
print(conn.execute("SELECT * FROM stg_songs LIMIT 3").fetchall())