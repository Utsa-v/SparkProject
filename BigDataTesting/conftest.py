import pytest
import duckdb

@pytest.fixture(scope="module")
def duckdb_connection():
    con = duckdb.connect()

    # Load CSV File as a Table

    con.execute(r""" CREATE TABLE source AS 
    SELECT * FROM 
    read_csv_auto("C:\Users\Utsav Sharma\PycharmProjects\BigDataTesting\data\source_sales.csv")""")

    con.execute(r"""CREATE TABLE target AS 
    SELECT * FROM 
    read_csv_auto("C:\Users\Utsav Sharma\PycharmProjects\BigDataTesting\data\target_sales.csv")""")

    yield con  # Yield the connection for use in tests

    con.close()