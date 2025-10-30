import adbc_driver_manager
import adbc_driver_flightsql.dbapi as flight_sql

FE_HOST = "127.0.0.1"
FE_PORT = 9408

conn = flight_sql.connect(
    uri=f"grpc://{FE_HOST}:{FE_PORT}",
    db_kwargs={
        adbc_driver_manager.DatabaseOptions.USERNAME.value: "root",
        adbc_driver_manager.DatabaseOptions.PASSWORD.value: "",
    }
)
cursor = conn.cursor()

sql1 = "SELECT * FROM default_catalog.testdb.96float_table ORDER BY k0 LIMIT 10"

cursor.execute(sql1)
df = cursor.fetchallarrow().to_pandas()
# print(cursor.fetchallarrow().to_pandas())
# print(df.columns)

cursor.execute("SELECT * FROM default_catalog.testdb.test_flight_sql ORDER BY k0")
arrow_table = cursor.fetchallarrow()
print(arrow_table)
print("Schema column names:", arrow_table.schema.names)
df = arrow_table.to_pandas()
print(df)

sql = "SELECT * FROM default_catalog.testdb.96float_table LIMIT 1000"

# cursor.execute(sql)
# df = cursor.fetch_df()
# print(df)

# print(cursor.fetchallarrow().to_pandas())

# cursor.execute("DROP DATABASE IF EXISTS sr_arrow_flight_sql FORCE;")
# print(cursor.fetchallarrow().to_pandas())
#
# cursor.execute("create database sr_arrow_flight_sql;")
# print(cursor.fetchallarrow().to_pandas())
#
# cursor.execute("show databases;")
# print(cursor.fetchallarrow().to_pandas())
#
# cursor.execute("use sr_arrow_flight_sql;")
# print(cursor.fetchallarrow().to_pandas())
#
# cursor.execute("""CREATE TABLE sr_arrow_flight_sql
#     (
#          k0 INT,
#          k1 DOUBLE,
#          K2 varchar(32) NULL DEFAULT "" COMMENT "",
#          k3 DECIMAL(27,9) DEFAULT "0",
#          k4 BIGINT NULL DEFAULT '10',
#          k5 DATE
#     )
#     DISTRIBUTED BY HASH(k5) BUCKETS 5
#     PROPERTIES("replication_num" = "1");""")
# print(cursor.fetchallarrow().to_pandas())
#
# cursor.execute("show create table sr_arrow_flight_sql;")
# print(cursor.fetchallarrow().to_pandas())
#
# cursor.execute("""INSERT INTO sr_arrow_flight_sql VALUES
#         ('0', 0.1, "ID", 0.0001, 9999999999, '2023-10-21'),
#         ('1', 0.20, "ID_1", 1.00000001, 0, '2023-10-21'),
#         ('2', 3.4, "ID_1", 3.1, 123456, '2023-10-22'),
#         ('3', 4, "ID", 4, 4, '2023-10-22'),
#         ('4', 122345.54321, "ID", 122345.54321, 5, '2023-10-22');""")
# print(cursor.fetchallarrow().to_pandas())
#
# cursor.execute("select * from sr_arrow_flight_sql order by k0;")
# print(cursor.fetchallarrow().to_pandas())
#
# cursor.execute("SHOW VARIABLES LIKE '%query_mem_limit%';")
# print(cursor.fetchallarrow().to_pandas())
#
# cursor.execute("SET query_mem_limit = 2147483648;")
# print(cursor.fetchallarrow().to_pandas())
#
# cursor.execute("SHOW VARIABLES LIKE '%query_mem_limit%';")
# print(cursor.fetchallarrow().to_pandas())
#
# cursor.execute("select k5, sum(k1), count(1), avg(k3) from sr_arrow_flight_sql group by k5;")
# print(cursor.fetch_df())