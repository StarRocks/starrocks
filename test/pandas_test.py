import pandas as pd
import adbc_driver_manager
import adbc_driver_flightsql.dbapi as flight_sql

# ✅ Connect to StarRocks Flight SQL
conn = flight_sql.connect(
    uri="grpc://127.0.0.1:9408",
    db_kwargs={
        adbc_driver_manager.DatabaseOptions.USERNAME.value: "root",
        adbc_driver_manager.DatabaseOptions.PASSWORD.value: "",
    }
)

# ✅ Create test table
create_sql = """
CREATE TABLE IF NOT EXISTS test_flight_sql (
    id INT,
    name STRING,
    score DOUBLE,
    created_at DATETIME
) ENGINE=OLAP
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES("replication_num" = "1");
"""

# ✅ Insert test data
insert_sql = """
INSERT INTO test_flight_sql VALUES
    (1, 'Alice', 88.5, '2023-10-01 10:00:00'),
    (2, 'Bob', 92.3, '2023-10-01 10:01:00'),
    (3, 'Charlie', 79.2, '2023-10-01 10:02:00');
"""

# ✅ SQL to query data
query_sql = "SELECT * FROM test_flight_sql ORDER BY id"

# ✅ Run test
with conn.cursor() as cur:
    cur.execute("USE pandas_test;")
    cur.execute(create_sql)
    cur.execute(insert_sql)
    cur.execute(query_sql)

    # ✅ Fetch Arrow Table
    arrow_table = cur.fetchallarrow()
    print("Arrow Table Schema:", arrow_table.schema)

    # ✅ Convert to Pandas
    df = arrow_table.to_pandas()
    # df.columns = ["id", "name", "score", "created_at"]

    print("Pandas DataFrame:")
    print(df)

    # ✅ Assertions
    assert df.shape[0] == 3
    assert "name" in df.columns
    assert df["score"].dtype in [float, "float64"]
    assert pd.to_datetime(df["created_at"]).dt.year[0] == 2023

    print("✅ Arrow Flight SQL + Pandas test passed!")
