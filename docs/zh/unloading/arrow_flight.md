---
displayed_sidebar: docs
---

# 使用 Arrow Flight SQL 从 StarRocks 读取数据

从 v3.5.1 开始，StarRocks 支持通过 Apache Arrow Flight SQL 协议进行连接。

此方案建立了一个从 StarRocks 列式执行引擎到客户端的全列式数据传输管道，消除了传统 JDBC 和 ODBC 接口中常见的频繁行列转换和序列化开销。这使得 StarRocks 能够实现零拷贝、低延迟和高吞吐量的数据传输。

## 使用方法

按照以下步骤，通过 Arrow Flight SQL 协议使用 Python ADBC 驱动程序连接并与 StarRocks 进行交互。完整代码示例请参见[附录](#附录)。

:::note

需要 Python 3.9 或更高版本。

:::

### 步骤 1. 安装库

使用 `pip` 从 PyPI 安装 `adbc_driver_manager` 和 `adbc_driver_flightsql`：

```Bash
pip install adbc_driver_manager
pip install adbc_driver_flightsql
```

将以下模块或库导入到您的代码中：

- 必需的库：

```Python
import adbc_driver_manager
import adbc_driver_flightsql.dbapi as flight_sql
```

- 可选模块以提高可用性和调试：

```Python
import pandas as pd       # 可选：使用 DataFrame 更好地显示结果
import traceback          # 可选：在 SQL 执行期间提供详细的错误追踪
import time               # 可选：用于测量 SQL 执行时间
```

### 步骤 2. 连接到 StarRocks

:::note

如果您想在 IntelliJ IDEA 中运行服务，必须在 `Run/Debug Configurations` 的 `Build and run` 中添加以下选项：

```Bash
--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED
```

:::

#### 配置 StarRocks

在通过 Arrow Flight SQL 连接到 StarRocks 之前，您必须先配置 FE 和 BE 节点，以确保 Arrow Flight SQL 服务已启用并在指定端口上监听。

在 FE 配置文件 **fe.conf** 和 BE 配置文件 **be.conf** 中，将 `arrow_flight_port` 设置为可用端口。修改配置文件后，重启 FE 和 BE 服务以使修改生效。

示例：

```Properties
arrow_flight_port = 9408
```

#### 建立连接

在客户端上，使用以下信息创建一个 Arrow Flight SQL 客户端：

- StarRocks FE 的主机地址
- Arrow Flight 在 StarRocks FE 上监听的端口
- 拥有必要权限的 StarRocks 用户的用户名和密码

示例：

```Python
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
```

连接建立后，您可以通过返回的 Cursor 执行 SQL 语句与 StarRocks 进行交互。

### 步骤 3. （可选）预定义工具函数

这些函数用于格式化输出、标准化格式和简化调试。您可以在代码中可选地定义它们以进行测试。

```Python
# =============================================================================
# 工具函数，用于更好的输出格式化和 SQL 执行
# =============================================================================

# 打印部分标题
def print_header(title: str):
    """
    打印部分标题以提高可读性。
    """
    print("\n" + "=" * 80)
    print(f"🟢 {title}")
    print("=" * 80)

# 打印正在执行的 SQL 语句
def print_sql(sql: str):
    """
    在执行前打印 SQL 语句。
    """
    print(f"\n🟡 SQL:\n{sql.strip()}")

# 打印结果 DataFrame
def print_result(df: pd.DataFrame):
    """
    以可读格式打印结果 DataFrame。
    """
    if df.empty:
        print("\n🟢 Result: (no rows returned)\n")
    else:
        print("\n🟢 Result:\n")
        print(df.to_string(index=False))

# 打印错误追踪
def print_error(e: Exception):
    """
    如果 SQL 执行失败，打印错误追踪。
    """
    print("\n🔴 Error occurred:")
    traceback.print_exc()

# 执行 SQL 语句并打印结果
def execute(sql: str):
    """
    执行 SQL 语句并打印结果和执行时间。
    """
    print_sql(sql)
    try:
        start = time.time()  # 可选：开始时间用于测量执行时间
        cursor.execute(sql)
        result = cursor.fetchallarrow()  # Arrow Table
        df = result.to_pandas()  # 可选：转换为 DataFrame 以更好地显示
        print_result(df)
        print(f"\n⏱️  Execution time: {time.time() - start:.3f} seconds")
    except Exception as e:
        print_error(e)
```

### 步骤 4. 与 StarRocks 交互

本节将指导您完成一些基本操作，例如创建表、导入数据、检查表结构、设置变量和运行查询。

:::note

下面列出的输出示例是基于前述步骤中描述的可选模块和工具函数实现的。

:::

1. 创建一个数据库和一个将导入数据的表，并检查表结构。

   ```Python
   # Step 1: Drop and create database
   print_header("Step 1: Drop and Create Database")
   execute("DROP DATABASE IF EXISTS sr_arrow_flight_sql FORCE;")
   execute("SHOW DATABASES;")
   execute("CREATE DATABASE sr_arrow_flight_sql;")
   execute("SHOW DATABASES;")
   execute("USE sr_arrow_flight_sql;")
   
   # Step 2: Create table
   print_header("Step 2: Create Table")
   execute("""
   CREATE TABLE sr_arrow_flight_sql_test
   (
       k0 INT,
       k1 DOUBLE,
       k2 VARCHAR(32) NULL DEFAULT "" COMMENT "",
       k3 DECIMAL(27,9) DEFAULT "0",
       k4 BIGINT NULL DEFAULT '10',
       k5 DATE
   )
   DISTRIBUTED BY HASH(k5) BUCKETS 5
   PROPERTIES("replication_num" = "1");
   """)
   execute("SHOW CREATE TABLE sr_arrow_flight_sql_test;")
   ```

   示例输出：

   ```SQL
   ================================================================================
   🟢 Step 1: Drop and Create Database
   ================================================================================
   
   🟡 SQL:
   DROP DATABASE IF EXISTS sr_arrow_flight_sql FORCE;
   /Users/starrocks/test/venv/lib/python3.9/site-packages/adbc_driver_manager/dbapi.py:307: Warning: Cannot disable autocommit; conn will not be DB-API 2.0 compliant
     warnings.warn(
   
   🟢 Result:
   
   StatusResult
              0
   
   ⏱️  Execution time: 0.025 seconds
   
   🟡 SQL:
   SHOW DATABASES;
   
   🟢 Result:
   
             Database
         _statistics_
                 hits
   information_schema
                  sys
   
   ⏱️  Execution time: 0.014 seconds
   
   🟡 SQL:
   CREATE DATABASE sr_arrow_flight_sql;
   
   🟢 Result:
   
   StatusResult
              0
   
   ⏱️  Execution time: 0.012 seconds
   
   🟡 SQL:
   SHOW DATABASES;
   
   🟢 Result:
   
              Database
          _statistics_
                  hits
    information_schema
   sr_arrow_flight_sql
                   sys
   
   ⏱️  Execution time: 0.005 seconds
   
   🟡 SQL:
   USE sr_arrow_flight_sql;
   
   🟢 Result:
   
   StatusResult
              0
   
   ⏱️  Execution time: 0.006 seconds
   
   ================================================================================
   🟢 Step 2: Create Table
   ================================================================================
   
   🟡 SQL:
   CREATE TABLE sr_arrow_flight_sql_test
   (
       k0 INT,
       k1 DOUBLE,
       k2 VARCHAR(32) NULL DEFAULT "" COMMENT "",
       k3 DECIMAL(27,9) DEFAULT "0",
       k4 BIGINT NULL DEFAULT '10',
       k5 DATE
   )
   DISTRIBUTED BY HASH(k5) BUCKETS 5
   PROPERTIES("replication_num" = "1");
   
   🟢 Result:
   
   StatusResult
              0
   
   ⏱️  Execution time: 0.021 seconds
   
   🟡 SQL:
   SHOW CREATE TABLE sr_arrow_flight_sql_test;
   
   🟢 Result:
   
                      Table                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  Create Table
   sr_arrow_flight_sql_test CREATE TABLE `sr_arrow_flight_sql_test` (\n  `k0` int(11) NULL COMMENT "",\n  `k1` double NULL COMMENT "",\n  `k2` varchar(32) NULL DEFAULT "" COMMENT "",\n  `k3` decimal(27, 9) NULL DEFAULT "0" COMMENT "",\n  `k4` bigint(20) NULL DEFAULT "10" COMMENT "",\n  `k5` date NULL COMMENT ""\n) ENGINE=OLAP \nDUPLICATE KEY(`k0`)\nDISTRIBUTED BY HASH(`k5`) BUCKETS 5 \nPROPERTIES (\n"compression" = "LZ4",\n"fast_schema_evolution" = "true",\n"replicated_storage" = "true",\n"replication_num" = "1"\n);
   
   ⏱️  Execution time: 0.005 seconds
   ```
2. 插入数据，运行一些查询，并设置变量。

   ```Python
   # Step 3: Insert data
   print_header("Step 3: Insert Data")
   execute("""
   INSERT INTO sr_arrow_flight_sql_test VALUES
       (0, 0.1, "ID", 0.0001, 1111111111, '2025-04-21'),
       (1, 0.20, "ID_1", 1.00000001, 0, '2025-04-21'),
       (2, 3.4, "ID_1", 3.1, 123456, '2025-04-22'),
       (3, 4, "ID", 4, 4, '2025-04-22'),
       (4, 122345.54321, "ID", 122345.54321, 5, '2025-04-22');
   """)
   
   # Step 4: Query data
   print_header("Step 4: Query Data")
   execute("SELECT * FROM sr_arrow_flight_sql_test ORDER BY k0;")
   
   # Step 5: Session variables
   print_header("Step 5: Session Variables")
   execute("SHOW VARIABLES LIKE '%query_mem_limit%';")
   execute("SET query_mem_limit = 2147483648;")
   execute("SHOW VARIABLES LIKE '%query_mem_limit%';")
   
   # Step 6: Aggregation query
   print_header("Step 6: Aggregation Query")
   execute("""
   SELECT k5, SUM(k1) AS total_k1, COUNT(1) AS row_count, AVG(k3) AS avg_k3
   FROM sr_arrow_flight_sql_test
   GROUP BY k5
   ORDER BY k5;
   """)
   ```

   示例输出：

   ```SQL
   ================================================================================
   🟢 Step 3: Insert Data
   ================================================================================
   
   🟡 SQL:
   INSERT INTO sr_arrow_flight_sql_test VALUES
       (0, 0.1, "ID", 0.0001, 1111111111, '2025-04-21'),
       (1, 0.20, "ID_1", 1.00000001, 0, '2025-04-21'),
       (2, 3.4, "ID_1", 3.1, 123456, '2025-04-22'),
       (3, 4, "ID", 4, 4, '2025-04-22'),
       (4, 122345.54321, "ID", 122345.54321, 5, '2025-04-22');
   
   🟢 Result:
   
   StatusResult
              0
   
   ⏱️  Execution time: 0.149 seconds
   
   ================================================================================
   🟢 Step 4: Query Data
   ================================================================================
   
   🟡 SQL:
   SELECT * FROM sr_arrow_flight_sql_test ORDER BY k0;
   
   🟢 Result:
                                                             
   0      0.10000   ID      0.000100000 1111111111 2025-04-21
   1      0.20000 ID_1      1.000000010          0 2025-04-21
   2      3.40000 ID_1      3.100000000     123456 2025-04-22
   3      4.00000   ID      4.000000000          4 2025-04-22
   4 122345.54321   ID 122345.543210000          5 2025-04-22
   
   ⏱️  Execution time: 0.019 seconds
   
   ================================================================================
   🟢 Step 5: Session Variables
   ================================================================================
   
   🟡 SQL:
   SHOW VARIABLES LIKE '%query_mem_limit%';
   
   🟢 Result:
   
     Variable_name Value
   query_mem_limit     0
   
   ⏱️  Execution time: 0.005 seconds
   
   🟡 SQL:
   SET query_mem_limit = 2147483648;
   
   🟢 Result:
   
   StatusResult
              0
   
   ⏱️  Execution time: 0.007 seconds
   
   🟡 SQL:
   SHOW VARIABLES LIKE '%query_mem_limit%';
   
   🟢 Result:
   
     Variable_name      Value
   query_mem_limit 2147483648
   
   ⏱️  Execution time: 0.005 seconds
   
   ================================================================================
   🟢 Step 6: Aggregation Query
   ================================================================================
   
   🟡 SQL:
   SELECT k5, SUM(k1) AS total_k1, COUNT(1) AS row_count, AVG(k3) AS avg_k3
   FROM sr_arrow_flight_sql_test
   GROUP BY k5
   ORDER BY k5;
   
   🟢 Result:
                                               
   2025-04-21      0.30000 2     0.500050005000
   2025-04-22 122352.94321 3 40784.214403333333
   
   ⏱️  Execution time: 0.014 second
   ```

### 步骤 5. 关闭连接

在代码中包含以下部分以关闭连接。

```Python
# Step 7: Close
print_header("Step 7: Close Connection")
cursor.close()
conn.close()
print("✅ Test completed successfully.")
```

示例输出：

```Python
================================================================================
🟢 Step 7: Close Connection
================================================================================
✅ Test completed successfully.

Process finished with exit code 0
```

## 大规模数据传输的用例

### Python

通过 ADBC 驱动程序在 Python 中连接到支持 Arrow Flight SQL 的 StarRocks 后，您可以使用各种 ADBC API 将 Clickbench 数据集从 StarRocks 加载到 Python 中。

代码示例：

```Python
#!/usr/bin/env python
# -*- coding: utf-8 -*-

import adbc_driver_manager
import adbc_driver_flightsql.dbapi as flight_sql
from datetime import datetime

# ----------------------------------------
# StarRocks Flight SQL Connection Settings
# ----------------------------------------
# Replace the URI and credentials as needed
my_uri = "grpc://127.0.0.1:9408"  # Default Flight SQL port for StarRocks
my_db_kwargs = {
    adbc_driver_manager.DatabaseOptions.USERNAME.value: "root",
    adbc_driver_manager.DatabaseOptions.PASSWORD.value: "",
}

# ----------------------------------------
# SQL Query (ClickBench: hits table)
# ----------------------------------------
# Replace with the actual table and dataset as needed
sql = "SELECT * FROM clickbench.hits LIMIT 1000000;"  # Read 1 million rows

# ----------------------------------------
# Method 1: fetchallarrow + to_pandas
# ----------------------------------------
def test_fetchallarrow():
    conn = flight_sql.connect(uri=my_uri, db_kwargs=my_db_kwargs)
    cursor = conn.cursor()
    start = datetime.now()
    cursor.execute(sql)
    arrow_table = cursor.fetchallarrow()
    df = arrow_table.to_pandas()
    duration = datetime.now() - start

    print("\n[Method 1] fetchallarrow + to_pandas")
    print(f"Time taken: {duration}, Arrow table size: {arrow_table.nbytes / 1024 / 1024:.2f} MB, Rows: {len(df)}")
    print(df.info(memory_usage='deep'))

# ----------------------------------------
# Method 2: fetch_df (recommended)
# ----------------------------------------
def test_fetch_df():
    conn = flight_sql.connect(uri=my_uri, db_kwargs=my_db_kwargs)
    cursor = conn.cursor()
    start = datetime.now()
    cursor.execute(sql)
    df = cursor.fetch_df()
    duration = datetime.now() - start

    print("\n[Method 2] fetch_df (recommended)")
    print(f"Time taken: {duration}, Rows: {len(df)}")
    print(df.info(memory_usage='deep'))

# ----------------------------------------
# Method 3: adbc_execute_partitions (for parallel read)
# ----------------------------------------
def test_execute_partitions():
    conn = flight_sql.connect(uri=my_uri, db_kwargs=my_db_kwargs)
    cursor = conn.cursor()
    start = datetime.now()
    partitions, schema = cursor.adbc_execute_partitions(sql)

    # Read the first partition (for demo)
    cursor.adbc_read_partition(partitions[0])
    arrow_table = cursor.fetchallarrow()
    df = arrow_table.to_pandas()
    duration = datetime.now() - start

    print("\n[Method 3] adbc_execute_partitions (parallel read)")
    print(f"Time taken: {duration}, Partitions: {len(partitions)}, Rows: {len(df)}")
    print(df.info(memory_usage='deep'))

# ----------------------------------------
# Run All Tests
# ----------------------------------------
if __name__ == "__main__":
    test_fetchallarrow()
    test_fetch_df()
    test_execute_partitions()
```

结果表明，从 StarRocks 加载 100 万行 Clickbench 数据集（105 列，780 MB）仅用了 3 秒。

```Python
[Method 1] fetchallarrow + to_pandas
Time taken: 0:00:03.219575, Arrow table size: 717.42 MB, Rows: 1000000
<class 'pandas.core.frame.DataFrame'>
RangeIndex: 1000000 entries, 0 to 999999
Columns: 105 entries, CounterID to CLID
dtypes: int16(48), int32(19), int64(6), object(32)
memory usage: 2.4 GB

[Method 2] fetch_df (recommended)
Time taken: 0:00:02.358840, Rows: 1000000
<class 'pandas.core.frame.DataFrame'>
RangeIndex: 1000000 entries, 0 to 999999
Columns: 105 entries, CounterID to CLID
dtypes: int16(48), int32(19), int64(6), object(32)
memory usage: 2.4 GB

[Method 3] adbc_execute_partitions (parallel read)
Time taken: 0:00:02.231144, Partitions: 1, Rows: 1000000
<class 'pandas.core.frame.DataFrame'>
RangeIndex: 1000000 entries, 0 to 999999
Columns: 105 entries, CounterID to CLID
dtypes: int16(48), int32(19), int64(6), object(32)
memory usage: 2.4 GB
```

### Arrow Flight SQL JDBC 驱动程序

Arrow Flight SQL 协议提供了一个开源的 JDBC 驱动程序，与标准 JDBC 接口兼容。您可以轻松地将其集成到各种 BI 工具（如 Tableau、Power BI、DBeaver 等）中，以访问 StarRocks 数据库，就像使用传统 JDBC 驱动程序一样。此驱动程序的一个显著优势是其基于 Apache Arrow 的高速数据传输支持，大大提高了查询和数据传输的效率。使用方法几乎与传统的 MySQL JDBC 驱动程序相同。您只需在连接 URL 中将 `jdbc:mysql` 替换为 `jdbc:arrow-flight-sql` 即可无缝切换。查询结果仍然以标准 `ResultSet` 格式返回，确保与现有 JDBC 处理逻辑的兼容性。

:::note

如果您想在 IntelliJ IDEA 中进行调试，必须在 `Run/Debug Configurations` 的 `Build and run` 中添加以下选项：

```Bash
--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED
```

:::

<details>

  <summary>POM 依赖</summary>

```XML
<properties>
    <adbc.version>0.15.0</adbc.version>
</properties>

<dependencies>
    <dependency>
        <groupId>org.apache.arrow.adbc</groupId>
        <artifactId>adbc-driver-jdbc</artifactId>
        <version>${adbc.version}</version>
    </dependency>
    <dependency>
        <groupId>org.apache.arrow.adbc</groupId>
        <artifactId>adbc-core</artifactId>
        <version>${adbc.version}</version>
    </dependency>
    <dependency>
        <groupId>org.apache.arrow.adbc</groupId>
        <artifactId>adbc-driver-manager</artifactId>
        <version>${adbc.version}</version>
    </dependency>
    <dependency>
        <groupId>org.apache.arrow.adbc</groupId>
        <artifactId>adbc-sql</artifactId>
        <version>${adbc.version}</version>
    </dependency>
    <dependency>
        <groupId>org.apache.arrow.adbc</groupId>
        <artifactId>adbc-driver-flight-sql</artifactId>
        <version>${adbc.version}</version>
    </dependency>
</dependencies>
```

</details>

代码示例：

```Java
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Integration test for Arrow Flight SQL JDBC driver with StarRocks.
 *
 * This test covers:
 *  - Basic DDL and DML operations
 *  - Query execution and result validation
 *  - Error handling for invalid SQL
 *  - Query cancellation (simulated with a long-running query)
 */
public class ArrowFlightSqlIntegrationTest {

    private static final String JDBC_URL = "jdbc:arrow-flight-sql://127.0.0.1:9408"
            + "?useEncryption=false"
            + "&useServerPrepStmts=false"
            + "&useSSL=false"
            + "&useArrowFlightSql=true";

    private static final String USER = "root";
    private static final String PASSWORD = "";

    private static int testCaseNum = 1;

    public static void main(String[] args) {
        try {
            // Load Arrow Flight SQL JDBC driver
            Class.forName("org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver");

            try (Connection conn = DriverManager.getConnection(JDBC_URL, USER, PASSWORD);
                    Statement stmt = conn.createStatement()) {

                // Basic DDL and DML operations
                testUpdate(stmt, "DROP DATABASE IF EXISTS arrow_demo FORCE;");
                testQuery(stmt, "SHOW PROCESSLIST;");
                testUpdate(stmt, "CREATE DATABASE arrow_demo;");
                testQuery(stmt, "SHOW DATABASES;");
                testUpdate(stmt, "USE arrow_demo;");
                testUpdate(stmt, "CREATE TABLE test (id INT, name STRING) ENGINE=OLAP PRIMARY KEY (id) " +
                        "DISTRIBUTED BY HASH(id) BUCKETS 1 " +
                        "PROPERTIES ('replication_num' = '1');");
                testUpdate(stmt, "INSERT INTO test VALUES (1, 'Alice'), (2, 'Bob');");
                testUpdate(stmt, "INSERT INTO test VALUES (1, 'Alice'), (2, 'Bob');");
                testUpdate(stmt, "INSERT INTO test VALUES (3, 'Zac'), (4, 'Tom');");
                testQuery(stmt, "SELECT * FROM test;");
                testUpdate(stmt, "UPDATE test SET name = 'Charlie' WHERE id = 1;");
                testQuery(stmt, "SELECT * FROM arrow_demo.test;");
                testUpdate(stmt, "DELETE FROM test WHERE id = 2;");
                testUpdate(stmt, "ALTER TABLE test ADD COLUMN age INT;");
                testUpdate(stmt, "ALTER TABLE test MODIFY COLUMN name STRING;");
                testQuery(stmt, "SHOW CREATE TABLE test;");
                testUpdate(stmt, "INSERT INTO test (id, name, age) VALUES (5, 'Eve', 30);");
                testQuery(stmt, "SELECT * FROM test WHERE id = 5;");
                testQuery(stmt, "SELECT * FROM test;");
                testQuery(stmt, "SHOW CREATE TABLE test;");

                testUpdate(stmt, "CREATE TABLE test2 (id INT, age INT) ENGINE=OLAP PRIMARY KEY (id) " +
                        "DISTRIBUTED BY HASH(id) BUCKETS 1 " +
                        "PROPERTIES ('replication_num' = '1');");
                testUpdate(stmt, "INSERT INTO test2 VALUES (1, 18), (2, 20);");
                testQuery(stmt, "SELECT arrow_demo.test.id, arrow_demo.test.name, arrow_demo.test2.age FROM arrow_demo.test " +
                        "LEFT JOIN arrow_demo.test2 ON arrow_demo.test.id = arrow_demo.test2.id;");

                testQuery(stmt, "SELECT * FROM (SELECT id, name FROM test) AS sub WHERE id = 1;");
                testUpdate(stmt, "SET time_zone = '+08:00';");

                // Error handling: query on non-existent table
                try {
                    testQuery(stmt, "SELECT * FROM not_exist_table;");
                } catch (Exception e) {
                    System.out.println("✅ Expected error (table not exist): " + e.getMessage());
                }

                // Error handling: SQL syntax error
                try {
                    testQuery(stmt, "SELECT * FROM arrow_demo.test WHERE id = ;");
                } catch (Exception e) {
                    System.out.println("✅ Expected error (syntax error): " + e.getMessage());
                }

                // Query cancellation test
                try {
                    System.out.println("Test Case: " + testCaseNum);
                    System.out.println("▶ Executing long-running query (SELECT SLEEP(10)) and canceling after 1s");

                    try (Statement longStmt = conn.createStatement()) {
                        Thread cancelThread = new Thread(() -> {
                            try {
                                Thread.sleep(1);
                                try {
                                    longStmt.cancel();
                                    System.out.println("✅ Query cancel() called.");
                                } catch (SQLException e) {
                                    System.out.println("⚠️  Statement cancel() failed: " + e.getMessage());
                                }
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        });
                        cancelThread.start();

                        testQuery(longStmt, "SELECT * FROM information_schema.columns;");
                    }
                } catch (Exception e) {
                    System.out.println("✅ Expected error (query cancelled): " + e.getMessage());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("✅ SQL syntax coverage testing completed");
    }

    /**
     * Executes a query and prints the result to the console.
     */
    private static void testQuery(Statement stmt, String sql) throws Exception {
        System.out.println("Test Case: " + testCaseNum);
        System.out.println("▶ Executing query: " + sql);
        try (ResultSet rs = stmt.executeQuery(sql)) {
            System.out.println("Result:");
            int columnCount = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    System.out.print(rs.getString(i) + "\t");
                }
                System.out.println();
            }
        }
        testCaseNum++;
        System.out.println();
    }

    /**
     * Executes an update (DDL or DML) and prints the result to the console.
     */
    private static void testUpdate(Statement stmt, String sql) throws Exception {
        System.out.println("Test Case: " + testCaseNum);
        System.out.println("▶ Executing update: " + sql);
        stmt.executeUpdate(sql);
        System.out.println("Result: ✅ Success");
        testCaseNum++;
        System.out.println();
    }
}
```

执行结果：

```Bash
Test Case: 1
▶ Executing update: DROP DATABASE IF EXISTS arrow_demo FORCE;
Result: ✅ Success

Test Case: 2
▶ Executing query: SHOW PROCESSLIST;
Result:
192.168.124.17_9010_1745287990251        16777217        root        127.0.0.1:58950        hits        Sleep        2025-04-22 10:26:43        4745        EOF        select count(*) from hits        false        
192.168.124.17_9010_1745287990251        16777218        root                sr_arrow_flight_sql        Sleep        2025-04-22 10:45:21        11221        ERR        show create table arrow_flight_sql_test;        false        
192.168.124.17_9010_1745287990251        16777219        root                sr_arrow_flight_sql        Sleep        2025-04-22 10:45:56        11186        EOF        show create table sr_arrow_flight_sql_test;        false        
192.168.124.17_9010_1745287990251        16777220        root                sr_arrow_flight_sql        Sleep        2025-04-22 10:50:06        10935        ERR        
SELECT k5, SUM(k1) AS total_k1, COUNT(1) AS row_count, AVG(k3) AS avg_k3
FROM sr_arrow_flight_sql_t        false        
192.168.124.17_9010_1745287990251        16777221        root                sr_arrow_flight_sql        Sleep        2025-04-22 11:23:11        8951        EOF        SHOW CREATE TABLE sr_arrow_flight_sql_test;        false        
192.168.124.17_9010_1745287990251        16777222        root                sr_arrow_flight_sql        Sleep        2025-04-22 11:24:08        8894        EOF        SHOW CREATE TABLE sr_arrow_flight_sql_test;        false        
192.168.124.17_9010_1745287990251        16777223        root                sr_arrow_flight_sql        Sleep        2025-04-22 11:31:06        8476        OK        
SELECT k5, SUM(k1) AS total_k1, COUNT(1) AS row_count, AVG(k3) AS avg_k3
FROM sr_arrow_flight_sql_t        false        
192.168.124.17_9010_1745287990251        16777224        root                sr_arrow_flight_sql        Sleep        2025-04-22 11:31:20        8462        ERR        
SELECT k5, SUM(k1) AS total_k1, COUNT(1) AS row_count, AVG(k3) AS avg_k3
FROM sr_arrow_flight_sql_t        false        
192.168.124.17_9010_1745287990251        16777225        root                sr_arrow_flight_sql        Sleep        2025-04-22 11:37:47        8075        ERR        INSERT INTO arrow_flight_sql_test VALUES
        (0, 0.1, "ID", 0.0001, 9999999999, '2023-10-21'),
         false        
192.168.124.17_9010_1745287990251        16777226        root                sr_arrow_flight_sql        Sleep        2025-04-22 11:38:10        8052        ERR        select k5, sum(k1), count(1), avg(k3) from arrow_flight_sql_test group by k5;        false        
192.168.124.17_9010_1745287990251        16777227        root                sr_arrow_flight_sql        Sleep        2025-04-22 11:42:43        7779        ERR        
SELECT k5, SUM(k1) AS total_k1, COUNT(1) AS row_count, AVG(k3) AS avg_k3
FROM sr_arrow_flight_sql_t        false        
192.168.124.17_9010_1745287990251        16777228        root                sr_arrow_flight_sql        Sleep        2025-04-22 11:46:47        7535        ERR        
SELECT k5, SUM(k1) AS total_k1, COUNT(1) AS row_count, AVG(k3) AS avg_k3
FROM sr_arrow_flight_sql_t        false        
192.168.124.17_9010_1745287990251        16777229        root                        Sleep        2025-04-22 12:34:46        4656        ERR        SELECT * FROM hits.hits LIMIT 1000000;        false        
192.168.124.17_9010_1745287990251        16777230        root                        Sleep        2025-04-22 12:34:54        4648        ERR        SELECT * FROM hits.hits LIMIT 1000000;        false        
192.168.124.17_9010_1745287990251        16777231        root                        Sleep        2025-04-22 12:34:59        4643        ERR        SELECT * FROM hits.hits LIMIT 1000000;        false        
192.168.124.17_9010_1745287990251        16777232        root                        Sleep        2025-04-22 12:37:11        4511        ERR        SELECT * FROM hits.hits LIMIT 1000000;        false        
192.168.124.17_9010_1745287990251        16777233        root                        Sleep        2025-04-22 12:37:18        4505        ERR        SELECT * FROM hits.hits LIMIT 1000000;        false        
192.168.124.17_9010_1745287990251        16777234        root                        Sleep        2025-04-22 12:37:23        4499        ERR        SELECT * FROM hits.hits LIMIT 1000000;        false        
192.168.124.17_9010_1745287990251        16777235        root                        Sleep        2025-04-22 12:37:58        4464        ERR        SELECT * FROM hits.hits LIMIT 1000000;        false        
192.168.124.17_9010_1745287990251        16777236        root                        Sleep        2025-04-22 12:38:05        4457        ERR        SELECT * FROM hits.hits LIMIT 1000000;        false        
192.168.124.17_9010_1745287990251        16777237        root                        Sleep        2025-04-22 12:38:11        4451        ERR        SELECT * FROM hits.hits LIMIT 1000000;        false        
192.168.124.17_9010_1745287990251        16777238        root                        Sleep        2025-04-22 12:40:38        4304        ERR        SELECT * FROM hits.hits LIMIT 1000000;        false        
192.168.124.17_9010_1745287990251        16777239        root                        Sleep        2025-04-22 12:40:44        4298        ERR        SELECT * FROM hits.hits LIMIT 1000000;        false        
192.168.124.17_9010_1745287990251        16777240        root                        Sleep        2025-04-22 12:40:50        4292        ERR        SELECT * FROM hits.hits LIMIT 1000000;        false        
192.168.124.17_9010_1745287990251        16777241        root                        Sleep        2025-04-22 12:41:23        4259        ERR        SELECT * FROM hits.hits LIMIT 1000000;        false        
192.168.124.17_9010_1745287990251        16777242        root                        Sleep        2025-04-22 12:41:30        4252        ERR        SELECT * FROM hits.hits LIMIT 1000000;        false        
192.168.124.17_9010_1745287990251        16777243        root                        Sleep        2025-04-22 12:41:36        4246        ERR        SELECT * FROM hits.hits LIMIT 1000000;        false        
192.168.124.17_9010_1745287990251        16777244        root                        Query        2025-04-22 13:52:22        0        OK        SHOW PROCESSLIST;        false        

Test Case: 3
▶ Executing update: CREATE DATABASE arrow_demo;
Result: ✅ Success

Test Case: 4
▶ Executing query: SHOW DATABASES;
Result:
_statistics_        
arrow_demo        
arrow_flight_sql        
hits        
information_schema        
sr_arrow_flight_sql        
sys        

Test Case: 5
▶ Executing update: USE arrow_demo;
Result: ✅ Success

Test Case: 6
▶ Executing update: CREATE TABLE test (id INT, name STRING) ENGINE=OLAP PRIMARY KEY (id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num' = '1');
Result: ✅ Success

Test Case: 7
▶ Executing update: INSERT INTO test VALUES (1, 'Alice'), (2, 'Bob');
Result: ✅ Success

Test Case: 8
▶ Executing update: INSERT INTO test VALUES (1, 'Alice'), (2, 'Bob');
Result: ✅ Success

Test Case: 9
▶ Executing update: INSERT INTO test VALUES (3, 'Zac'), (4, 'Tom');
Result: ✅ Success

Test Case: 10
▶ Executing query: SELECT * FROM test;
Result:
1        Alice        
2        Bob        
3        Zac        
4        Tom        

Test Case: 11
▶ Executing update: UPDATE test SET name = 'Charlie' WHERE id = 1;
Result: ✅ Success

Test Case: 12
▶ Executing query: SELECT * FROM arrow_demo.test;
Result:
2        Bob        
3        Zac        
4        Tom        
1        Charlie        

Test Case: 13
▶ Executing update: DELETE FROM test WHERE id = 2;
Result: ✅ Success

Test Case: 14
▶ Executing update: ALTER TABLE test ADD COLUMN age INT;
Result: ✅ Success

Test Case: 15
▶ Executing update: ALTER TABLE test MODIFY COLUMN name STRING;
Result: ✅ Success

Test Case: 16
▶ Executing query: SHOW CREATE TABLE test;
Result:
test        CREATE TABLE `test` (
  `id` int(11) NOT NULL COMMENT "",
  `name` varchar(65533) NULL COMMENT "",
  `age` int(11) NULL COMMENT ""
) ENGINE=OLAP 
PRIMARY KEY(`id`)
DISTRIBUTED BY HASH(`id`) BUCKETS 1 
PROPERTIES (
"compression" = "LZ4",
"enable_persistent_index" = "true",
"fast_schema_evolution" = "true",
"replicated_storage" = "true",
"replication_num" = "1"
);        

Test Case: 17
▶ Executing update: INSERT INTO test (id, name, age) VALUES (5, 'Eve', 30);
Result: ✅ Success

Test Case: 18
▶ Executing query: SELECT * FROM test WHERE id = 5;
Result:
5        Eve        30        

Test Case: 19
▶ Executing query: SELECT * FROM test;
Result:
3        Zac        null        
4        Tom        null        
1        Charlie        null        
5        Eve        30        

Test Case: 20
▶ Executing query: SHOW CREATE TABLE test;
Result:
test        CREATE TABLE `test` (
  `id` int(11) NOT NULL COMMENT "",
  `name` varchar(65533) NULL COMMENT "",
  `age` int(11) NULL COMMENT ""
) ENGINE=OLAP 
PRIMARY KEY(`id`)
DISTRIBUTED BY HASH(`id`) BUCKETS 1 
PROPERTIES (
"compression" = "LZ4",
"enable_persistent_index" = "true",
"fast_schema_evolution" = "true",
"replicated_storage" = "true",
"replication_num" = "1"
);        

Test Case: 21
▶ Executing update: CREATE TABLE test2 (id INT, age INT) ENGINE=OLAP PRIMARY KEY (id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num' = '1');
Result: ✅ Success

Test Case: 22
▶ Executing update: INSERT INTO test2 VALUES (1, 18), (2, 20);
Result: ✅ Success

Test Case: 23
▶ Executing query: SELECT arrow_demo.test.id, arrow_demo.test.name, arrow_demo.test2.age FROM arrow_demo.test LEFT JOIN arrow_demo.test2 ON arrow_demo.test.id = arrow_demo.test2.id;
Result:
4        Tom        null        
3        Zac        null        
5        Eve        null        
1        Charlie        18        

Test Case: 24
▶ Executing query: SELECT * FROM (SELECT id, name FROM test) AS sub WHERE id = 1;
Result:
1        Charlie        

Test Case: 25
▶ Executing update: SET time_zone = '+08:00';
Result: ✅ Success

Test Case: 26
▶ Executing query: SELECT * FROM not_exist_table;
✅ Expected error (table not exist): Error while executing SQL "SELECT * FROM not_exist_table;": failed to process query [queryID=f70a03a5-1f3d-11f0-92e7-f29d1152bb04] [error=Getting analyzing error. Detail message: Unknown table 'arrow_demo.not_exist_table'.]
Test Case: 26
▶ Executing query: SELECT * FROM arrow_demo.test WHERE id = ;
✅ Expected error (syntax error): Error while executing SQL "SELECT * FROM arrow_demo.test WHERE id = ;": com.starrocks.sql.parser.ParsingException: Getting syntax error at line 1, column 39. Detail message: Unexpected input '=', the most similar input is {<EOF>, ';'}.
Test Case: 26
▶ Executing long-running query (SELECT SLEEP(10)) and canceling after 1s
Test Case: 26
▶ Executing query: SELECT * FROM information_schema.columns;
✅ Query cancel() called.
Result:
✅ Expected error (query cancelled): Statement canceled
✅ SQL syntax coverage testing completed
```

### Java ADBC 驱动程序

Arrow Flight SQL 协议提供了一个开源的 JDBC 驱动程序，与标准 JDBC 接口兼容。您可以轻松地将其集成到各种 BI 工具（如 Tableau、Power BI、DBeaver 等）中，以访问 StarRocks 数据库，就像使用传统 JDBC 驱动程序一样。此驱动程序的一个显著优势是其基于 Apache Arrow 的高速数据传输支持，大大提高了查询和数据传输的效率。使用方法几乎与传统的 MySQL JDBC 驱动程序相同。您只需在连接 URL 中将 `jdbc:mysql` 替换为 `jdbc:arrow-flight-sql` 即可无缝切换。查询结果仍然以标准 `ResultSet` 格式返回，确保与现有 JDBC 处理逻辑的兼容性。

:::note

如果您想在 IntelliJ IDEA 中进行调试，必须在 `Run/Debug Configurations` 的 `Build and run` 中添加以下选项：

```Bash
--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED
```

:::

<details>

  <summary>POM 依赖</summary>

```XML
<properties>
    <adbc.version>0.15.0</adbc.version>
</properties>

<dependencies>
    <dependency>
        <groupId>org.apache.arrow.adbc</groupId>
        <artifactId>adbc-driver-jdbc</artifactId>
        <version>${adbc.version}</version>
    </dependency>
    <dependency>
        <groupId>org.apache.arrow.adbc</groupId>
        <artifactId>adbc-core</artifactId>
        <version>${adbc.version}</version>
    </dependency>
    <dependency>
        <groupId>org.apache.arrow.adbc</groupId>
        <artifactId>adbc-driver-manager</artifactId>
        <version>${adbc.version}</version>
    </dependency>
    <dependency>
        <groupId>org.apache.arrow.adbc</groupId>
        <artifactId>adbc-sql</artifactId>
        <version>${adbc.version}</version>
    </dependency>
    <dependency>
        <groupId>org.apache.arrow.adbc</groupId>
        <artifactId>adbc-driver-flight-sql</artifactId>
        <version>${adbc.version}</version>
    </dependency>
</dependencies>
```

</details>

与 Python 中的用法类似，您也可以直接在 Java 中创建 ADBC 客户端以从 StarRocks 读取数据。

在此过程中，您首先需要获取 FlightInfo，然后连接到每个 Endpoint 以获取数据。

代码示例：

```Java
public static void main(String[] args) throws Exception {
    try (BufferAllocator allocator = new RootAllocator()) {
        FlightSqlDriver driver = new FlightSqlDriver(allocator);

        Map<String, Object> parameters = new HashMap<>();
        String host = "localhost";
        int port = 9408;
        String uri = Location.forGrpcInsecure(host, port).getUri().toString();

        AdbcDriver.PARAM_URI.set(parameters, uri);
        AdbcDriver.PARAM_USERNAME.set(parameters, "root");
        AdbcDriver.PARAM_PASSWORD.set(parameters, "");

        try (AdbcDatabase database = driver.open(parameters);
                AdbcConnection connection = database.connect();
                AdbcStatement statement = connection.createStatement()) {

            statement.setSqlQuery("SHOW DATABASES;");

            try (AdbcStatement.QueryResult result = statement.executeQuery();
                    ArrowReader reader = result.getReader()) {

                int batchCount = 0;
                while (reader.loadNextBatch()) {
                    batchCount++;
                    VectorSchemaRoot root = reader.getVectorSchemaRoot();
                    System.out.println("Batch " + batchCount + ":");
                    System.out.println(root.contentToTSVString());
                }

                System.out.println("Total batches: " + batchCount);
            }
        }
    }
}
```

### Spark

目前，官方的 Arrow Flight 项目尚无计划支持 Spark 或 Flink。未来将逐步增加支持，以便 [starrocks-spark-connector](https://github.com/qwshen/spark-flight-connector) 能够通过 Arrow Flight SQL 访问 StarRocks，预计读性能将提升数倍。

在使用 Spark 访问 StarRocks 时，除了传统的 JDBC 或 Java 客户端方法外，您还可以使用开源的 Spark-Flight-Connector 组件，直接从 StarRocks Flight SQL Server 读取和写入数据作为 Spark DataSource。这种基于 Apache Arrow Flight 协议的方法具有以下显著优势：

- **高性能数据传输** Spark-Flight-Connector 使用 Apache Arrow 作为数据传输格式，实现零拷贝、高效的数据交换。StarRocks 的 `internal Block` 数据格式与 Arrow 之间的转换非常高效，相较于传统的 `CSV` 或 `JDBC` 方法，性能提升可达 10 倍，大幅降低数据传输开销。
- **对复杂数据类型的原生支持** Arrow 数据格式原生支持复杂类型（如 `Map`、`Array`、`Struct` 等），与传统 JDBC 方法相比，能够更好地适应 StarRocks 的复杂数据模型，增强数据的表达能力和兼容性。
- **支持读、写和流式写入** 该组件支持 Spark 作为 Flight SQL 客户端进行高效的读写操作，包括 `insert`、`merge`、`update` 和 `delete` DML 语句，甚至支持流式写入，适用于实时数据处理场景。
- **支持谓词下推和列裁剪** 在读取数据时，Spark-Flight-Connector 支持谓词下推和列裁剪，能够在 StarRocks 端进行数据过滤和列选择，大幅减少传输数据量，提高查询性能。
- **支持聚合下推和并行读取** 聚合操作（如 `sum`、`count`、`max`、`min` 等）可以下推到 StarRocks 执行，减少 Spark 的计算负担。同时支持基于分区的并行读取，提高大数据场景下的读取效率。
- **更适合大数据场景** 相较于传统 JDBC 方法，Flight SQL 协议更适合大规模、高并发访问场景，使 StarRocks 能够充分发挥其高性能分析能力。

## 附录

以下是使用教程中的完整代码示例。

```Python
# =============================================================================
# StarRocks Arrow Flight SQL Test Script
# =============================================================================
# pip install adbc_driver_manager adbc_driver_flightsql pandas
# =============================================================================

# =============================================================================
# Required core modules for connecting to StarRocks via Arrow Flight SQL
# =============================================================================
import adbc_driver_manager
import adbc_driver_flightsql.dbapi as flight_sql

# =============================================================================
# Optional modules for better usability and debugging
# =============================================================================
import pandas as pd       # 可选：使用 DataFrame 更好地显示结果
import traceback          # 可选：在 SQL 执行期间提供详细的错误追踪
import time               # 可选：用于测量 SQL 执行时间

# =============================================================================
# StarRocks Flight SQL Configuration
# =============================================================================
FE_HOST = "127.0.0.1"
FE_PORT = 9408

# =============================================================================
# Connect to StarRocks
# =============================================================================
conn = flight_sql.connect(
    uri=f"grpc://{FE_HOST}:{FE_PORT}",
    db_kwargs={
        adbc_driver_manager.DatabaseOptions.USERNAME.value: "root",
        adbc_driver_manager.DatabaseOptions.PASSWORD.value: "",
    }
)

cursor = conn.cursor()

# =============================================================================
# Utility functions for better output formatting and SQL execution
# =============================================================================

def print_header(title: str):
    """
    打印部分标题以提高可读性。
    """
    print("\n" + "=" * 80)
    print(f"🟢 {title}")
    print("=" * 80)


def print_sql(sql: str):
    """
    在执行前打印 SQL 语句。
    """
    print(f"\n🟡 SQL:\n{sql.strip()}")


def print_result(df: pd.DataFrame):
    """
    以可读格式打印结果 DataFrame。
    """
    if df.empty:
        print("\n🟢 Result: (no rows returned)\n")
    else:
        print("\n🟢 Result:\n")
        print(df.to_string(index=False))


def print_error(e: Exception):
    """
    如果 SQL 执行失败，打印错误追踪。
    """
    print("\n🔴 Error occurred:")
    traceback.print_exc()


def execute(sql: str):
    """
    执行 SQL 语句并打印结果和执行时间。
    """
    print_sql(sql)
    try:
        start = time.time()  # 开始时间用于测量执行时间
        cursor.execute(sql)
        result = cursor.fetchallarrow()  # Arrow Table
        df = result.to_pandas()          # 转换为 DataFrame 以更好地显示
        print_result(df)
        print(f"\n⏱️  Execution time: {time.time() - start:.3f} seconds")
    except Exception as e:
        print_error(e)

# =============================================================================
# Step 1: Drop and Create Database
# =============================================================================
print_header("Step 1: Drop and Create Database")
execute("DROP DATABASE IF EXISTS sr_arrow_flight_sql FORCE;")
execute("SHOW DATABASES;")
execute("CREATE DATABASE sr_arrow_flight_sql;")
execute("SHOW DATABASES;")
execute("USE sr_arrow_flight_sql;")

# =============================================================================
# Step 2: Create Table
# =============================================================================
print_header("Step 2: Create Table")
execute("""
CREATE TABLE sr_arrow_flight_sql_test
(
    k0 INT,
    k1 DOUBLE,
    k2 VARCHAR(32) NULL DEFAULT "" COMMENT "",
    k3 DECIMAL(27,9) DEFAULT "0",
    k4 BIGINT NULL DEFAULT '10',
    k5 DATE
)
DISTRIBUTED BY HASH(k5) BUCKETS 5
PROPERTIES("replication_num" = "1");
""")

execute("SHOW CREATE TABLE sr_arrow_flight_sql_test;")

# =============================================================================
# Step 3: Insert Data
# =============================================================================
print_header("Step 3: Insert Data")
execute("""
INSERT INTO sr_arrow_flight_sql_test VALUES
    (0, 0.1, "ID", 0.0001, 1111111111, '2025-04-21'),
    (1, 0.20, "ID_1", 1.00000001, 0, '2025-04-21'),
    (2, 3.4, "ID_1", 3.1, 123456, '2025-04-22'),
    (3, 4, "ID", 4, 4, '2025-04-22'),
    (4, 122345.54321, "ID", 122345.54321, 5, '2025-04-22');
""")

# =============================================================================
# Step 4: Query Data
# =============================================================================
print_header("Step 4: Query Data")
execute("SELECT * FROM sr_arrow_flight_sql_test ORDER BY k0;")

# =============================================================================
# Step 5: Session Variables
# =============================================================================
print_header("Step 5: Session Variables")
execute("SHOW VARIABLES LIKE '%query_mem_limit%';")
execute("SET query_mem_limit = 2147483648;")
execute("SHOW VARIABLES LIKE '%query_mem_limit%';")

# =============================================================================
# Step 6: Aggregation Query
# =============================================================================
print_header("Step 6: Aggregation Query")
execute("""
SELECT k5, SUM(k1) AS total_k1, COUNT(1) AS row_count, AVG(k3) AS avg_k3
FROM sr_arrow_flight_sql_test
GROUP BY k5
ORDER BY k5;
""")

# =============================================================================
# Step 7: Close Connection
# =============================================================================
print_header("Step 7: Close Connection")
cursor.close()
conn.close()
print("✅ Test completed successfully.")
```