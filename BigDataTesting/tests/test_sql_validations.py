from conftest import duckdb_connection

# View data
# print("📌 Source Table:")
# print(con.execute("SELECT * FROM source").fetchdf())
#
# print("\n📌 Target Table:")
# print(con.execute("SELECT * FROM target").fetchdf())
#
# print("\n✅ Row Count Validation:")

# Test case 1 Row count match and expected result source_count = target_count

def test_row_count_match(duckdb_connection):
    df = duckdb_connection.execute("""SELECT 
    (SELECT count(*) from source) as source_count,
    (SELECT count(*) from target) as target_count
    """).fetchdf()
    assert (df['source_count'][0] == df['target_count'][0]), \
        f"Row count mismatch: {df['source_count'][0]} != {df['target_count'][0]}"

def test_null_order_id_in_source(duckdb_connection):
    df = duckdb_connection.execute("""SELECT
    * from source where order_id is null
    """).fetchdf()

    assert df.empty, "Found NULL order_id in source"



# Transformation validation total_amount = quantity * price
def test_transformation_validity(duckdb_connection):
    df = duckdb_connection.execute("""
        SELECT 
            s.order_id, s.product, s.quantity, s.price,
            (s.quantity * s.price) AS expected_total,
            t.total_amount
        FROM source s
        JOIN target t ON s.order_id = t.order_id
        WHERE (s.quantity * s.price) != t.total_amount
    """).fetchdf()

    assert df.empty, f"Transformation mismatch found:\n{df}"

def test_no_duplicate_order_ids(duckdb_connection):
    df = duckdb_connection.execute("""
        SELECT order_id, COUNT(*) AS cnt
        FROM source
        GROUP BY order_id
        HAVING COUNT(*) > 1
    """).fetchdf()

    assert df.empty, f"Duplicate order_ids found:\n{df}"