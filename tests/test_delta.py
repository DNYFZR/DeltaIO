import os, sys, pytest, deltalake as delta
sys.path.append('../')
from src.delta_duck import *

DELTA_PATH = "data/data.delta"
DB_PATH = "data/database.duckdb"

# Clean DB for each test run
if os.path.exists(DB_PATH):
    os.remove("data/database.duckdb")

DB = duckdb.connect(DB_PATH)

### Delta
@pytest.mark.parametrize(argnames='test_case', argvalues=[DELTA_PATH, ])
def test_getDeltaTable(test_case):
    table = getDeltaTable(table_path=test_case)

    if not isinstance(table, delta.table.DeltaTable):
        raise AssertionError('getDeltaTable did not return a delta table')
    
    # clear unrequired versions
    table.vacuum()

@pytest.mark.parametrize(argnames='test_case', argvalues=[{"table": getDeltaTable(DELTA_PATH), "path": DELTA_PATH}, ])
def test_postDeltaTable(test_case):
    init_version = test_case["table"].version()
    
    postDeltaTable(table=test_case["table"], path=test_case["path"], mode="overwrite", overwrite_schema=True)
    new_version = getDeltaTable(table_path=test_case["path"]).version()
    
    if new_version <= init_version:
        raise AssertionError(f"postDeltaTable did not update test case version - init ({init_version}) & new ({new_version})")

### Polars
@pytest.mark.parametrize(argnames='test_case', argvalues=[{"table": getDeltaTable(DELTA_PATH)}, ])
def test_polarFromDelta(test_case):
    polar = polarFromDelta(test_case["table"])
    
    if not isinstance(polar, pl.DataFrame):
        raise AssertionError("polarFromDelta did not return a polars data frame")

@pytest.mark.parametrize(argnames='test_case', argvalues=[{"table": polarFromDelta(getDeltaTable(DELTA_PATH)), }, ])
def test_polarToDelta(test_case):
    unpolar = polarToDelta(test_case["table"], mode="overwrite", overwrite_schema=True)

    if not isinstance(unpolar, delta.table.DeltaTable):
        raise AssertionError('polarToDelta did not return a delta table')

### DuckDB
@pytest.mark.parametrize(argnames="test_case", argvalues=[{"DB": DB, "table": getDeltaTable(DELTA_PATH)}, ])
def test_duckFromDelta(test_case):
    duckFromDelta(table="tst", source=test_case["table"], DB=test_case["DB"])

    if "tst" not in test_case["DB"].execute("DESCRIBE;").pl().select("table_name").to_series().to_list():
        raise AssertionError("duckFromDelta did not create a table")

@pytest.mark.parametrize(argnames="test_case", argvalues=[{"DB": DB, }, ])
def test_duckToDelta(test_case):
    unduck = duckToDelta(
        table="tst", 
        source=DB, 
        path=DELTA_PATH,
        mode="overwrite", 
        overwrite_schema=True)

    if not isinstance(unduck, delta.table.DeltaTable):
        raise AssertionError('duckToDelta did not return a delta table')
