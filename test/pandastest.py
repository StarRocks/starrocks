import adbc_driver_flightsql.dbapi as flight_sql
import adbc_driver_manager
import pandas as pd
import numpy as np
import time
from datetime import datetime

# 连接配置
conn = flight_sql.connect(
    uri="grpc://127.0.0.1:9408",
    db_kwargs={
        adbc_driver_manager.DatabaseOptions.USERNAME.value: "root",
        adbc_driver_manager.DatabaseOptions.PASSWORD.value: "",
    }
)
cursor = conn.cursor()

# 创建测试库
cursor.execute("DROP DATABASE IF EXISTS pandas_test FORCE;")
cursor.execute("CREATE DATABASE pandas_test;")
cursor.execute("USE pandas_test;")

# 创建测试表
cursor.execute("""
CREATE TABLE pandas_test_table (
    id INT,
    name STRING,
    score DOUBLE,
    created_at DATE,
    amount DECIMAL(18,6)
)
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");
""")

# 创建 Pandas DataFrame
df = pd.DataFrame({
    "id": [1, 2, 3, 4, 5],
    "name": ["Alice", "Bob", "Charlie", "中文", None],
    "score": [95.5, 88.0, 92.3, 100.0, np.nan],
    "created_at": pd.to_datetime(["2023-10-01", "2023-10-02", "2023-10-03", "2023-10-04", "2023-10-05"]),
    "amount": [100.12, 200.23, 300.34, 400.45, None]
})

# 打印 DataFrame
print("✅ Pandas DataFrame:")
print(df)
print(df.dtypes)

# 使用 INSERT INTO 方式写入 StarRocks
print("🚀 INSERT INTO StarRocks")
for _, row in df.iterrows():
    sql = f"""
    INSERT INTO pandas_test_table VALUES (
        {int(row['id']) if pd.notna(row['id']) else 'NULL'},
        {f"'{row['name']}'" if pd.notna(row['name']) else 'NULL'},
        {row['score'] if pd.notna(row['score']) else 'NULL'},
        {f"'{row['created_at'].date()}'" if pd.notna(row['created_at']) else 'NULL'},
        {row['amount'] if pd.notna(row['amount']) else 'NULL'}
    );
    """
    cursor.execute(sql)

# 查询并转为 Pandas
print("🔍 查询并转为 Pandas DataFrame")
cursor.execute("SELECT * FROM pandas_test_table ORDER BY id;")
df2 = cursor.fetch_df()

df2.columns = ["id", "name", "score", "created_at", "amount"]
print("✅ 查询结果:")
print(df2)
print(df2.dtypes)

# Pandas 场景测试
print("🔍 场景测试: 筛选 score > 90")
print(df2[df2['score'] > 90])

print("🔍 场景测试: 分组聚合")
print(df2.groupby("created_at").agg({
    "score": "mean",
    "amount": "sum"
}))

# 关闭连接
cursor.close()
conn.close()
print("✅ Pandas 场景测试完成")
