import duckdb

state_conn = duckdb.connect('data/duckdb/state_duckdb.db')
storm_conn = duckdb.connect('data/duckdb/warehouse.duckdb')


results = storm_conn.execute("""
SELECT catalog_name, schema_name, schema_owner
FROM information_schema.schemata
ORDER BY schema_name, catalog_name;
""").fetchall()

for row in results:
    print(row)