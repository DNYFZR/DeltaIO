# DeltaDuck

import duckdb, polars as pl, deltalake as delta
from typing import Union

def getDeltaTable(table_path, **kwargs):
    return delta.DeltaTable(table_uri=table_path, **kwargs)

def postDeltaTable(table: delta.table.DeltaTable, path: str, **kwargs):
    '''Update a delta table on path'''
    delta.write_deltalake(
        table_or_uri=path,
        data=table.to_pyarrow_table(),
        **kwargs
    )
    return True

def duckFromDelta(table: str, source: delta.table.DeltaTable, DB:Union[str, duckdb.DuckDBPyConnection]):
    '''Load delta to duck'''

    if isinstance(DB, str):
        DB = duckdb.connect(DB)
    
    tmp = source.to_pyarrow_table()

    DB.execute(f'''
        CREATE OR REPLACE TABLE {table} AS 
        SELECT * FROM tmp ; 
    ''')
    
    return True

def duckToDelta(table: str, source: duckdb.DuckDBPyConnection, path="data/data.delta", **kwargs):
    '''Load polars DataFrame to Delta'''
    
    delta.write_deltalake(
        table_or_uri=path,
        data=source.execute(f"SELECT * FROM {table};").arrow(),
        **kwargs
        )
    
    return delta.DeltaTable(path)

def polarFromDelta(source: delta.table.DeltaTable):
    '''Load delta to polars'''
    return pl.from_arrow(source.to_pyarrow_table())

def polarToDelta(source: pl.DataFrame, path="data/data.delta", **kwargs):
    '''Load polars DataFrame to Delta'''
    
    delta.write_deltalake(
        table_or_uri=path,
        data=source.to_arrow(),
        **kwargs
        )
    
    return delta.DeltaTable(path)

if __name__ == "__main__":

    ### Delta
    table = getDeltaTable(table_path="data/data.delta")
    print(table.version())
    
    print(table.vacuum(1, dry_run=False, enforce_retention_duration=False))
    print(table.history())
    
    postDeltaTable(table=table, path="data/data.delta", mode="overwrite", overwrite_schema=True)

    table = getDeltaTable(table_path="data/data.delta")
    print(table.version())

    
    ### Polars
    polar = polarFromDelta(table)
    print(polar.describe())

    unpolar = polarToDelta(polar, mode="overwrite", overwrite_schema=True)
    print(unpolar.version())

    ### DuckDB
    DB = duckdb.connect("data/database.duckdb")
    
    duck = duckFromDelta(table="tst", source=table, DB=DB)
    print(DB.execute("DESCRIBE;"))

    unduck = duckToDelta(
        table="tst", 
        source=DB, 
        path="data/data.delta",
        mode="overwrite", 
        overwrite_schema=True)
    print(unduck.version())
