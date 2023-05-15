<h1 align="center"><b>DeltaDuck</b></h1>

A delta lake full of ducks and polar bears....

---

<h2 align="center"><b>API Reference</b></h2>

To be updated...

````python
import delta_duck as dd, duckdb

### Delta

# Get table
table = dd.getDeltaTable(table_path="data/data.delta")
print(table.version())

# Clean old versions
print(table.vacuum(1, dry_run=False, enforce_retention_duration=False))
print(table.history())

# Update table
dd.postDeltaTable(table=table, path="data/data.delta", mode="overwrite", overwrite_schema=True)

table = dd.getDeltaTable(table_path="data/data.delta")
print(table.version())


### Polars

# Export to polars frame
polar = dd.polarFromDelta(table)
print(polar.describe())

# Import from polars frame
unpolar = dd.polarToDelta(polar, mode="overwrite", overwrite_schema=True)
print(unpolar.version())


### DuckDB

# Export to DuckDB
DB = duckdb.connect("data/database.duckdb")

duck = dd.duckFromDelta(table="tst", source=table, DB=DB)
print(DB.execute("DESCRIBE;"))

# Import from DuckDB
unduck = dd.duckToDelta(
    table="tst", 
    source=DB, 
    path="data/data.delta",
    mode="overwrite", 
    overwrite_schema=True)
print(unduck.version())


````
