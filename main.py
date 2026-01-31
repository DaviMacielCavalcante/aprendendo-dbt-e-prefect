import duckdb
conn = duckdb.connect("data/dev.duckdb")
print(conn.execute("SHOW TABLES").fetchall())