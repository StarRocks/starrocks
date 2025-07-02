---
displayed_sidebar: docs
---

# Read Data from StarRocks using Arrow Flight SQL

From v3.5.1 onwards, StarRocks supports connections via Apache Arrow Flight SQL protocol.

This solution establishes a fully columnar data transfer pipeline from the StarRocks columnar execution engine to the client, eliminating the frequent row-column conversions and serialization overhead typically seen in traditional JDBC and ODBC interfaces. This enables StarRocks to transfer data with zero-copy, low latency, and high throughput.

## Usage

Follow these steps to connect to and interact with StarRocks using Python ADBC Driver via Arrow Flight SQL protocol. Refer to [Appendix](#appendix) for the complete code example.

:::note

Python 3.9 or later is a prerequisite.

:::

### Step 1. Install libraries

Use `pip` to install `adbc_driver_manager` and `adbc_driver_flightsql` from PyPI:

```Bash
pip install adbc_driver_manager
pip install adbc_driver_flightsql
```

Import the following modules or libraries into your code:

- Required libraries:

```Python
import adbc_driver_manager
import adbc_driver_flightsql.dbapi as flight_sql
```

- Optional modules for better usability and debugging:

```Python
import pandas as pd       # Optional: for better result display using DataFrame
import traceback          # Optional: for detailed error traceback during SQL execution
import time               # Optional: for measuring SQL execution time
```

### Step 2. Connect to StarRocks

:::note

If you want to run the service in IntelliJ IDEA, you must add the following option to `Build and run` in `Run/Debug Configurations`:

```Bash
--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED
```

:::

#### Configure StarRocks

Before connecting to StarRocks via Arrow Flight SQL, you must first configure the FE and BE nodes to ensure that the Arrow Flight SQL service is enabled and listening on the specified ports.

In both FE configuration file **fe.conf** and BE configuration file **be.conf**, set `arrow_flight_port` to an available port. After modifying the configuration files, restart FE and BE services to allow the modification to take effect.

Example:

```Properties
arrow_flight_port = 9408
```

#### Establish connection

On the client side, create an Arrow Flight SQL client using the following information:

- Host address of the StarRocks FE
- Port that Arrow Flight used for listening on the StarRocks FE
- Username and password of the StarRocks user that has the necessary privileges

Example:

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

After the connection is established, you can interact with StarRocks by executing SQL statements through the returned Cursor.

### Step 3. (Optional) Predefine utility functions

These functions are used to format the output, standardize the format, and simplify debugging. You can optionally define them in your code for testing.

```Python
# =============================================================================
# Utility functions for better output formatting and SQL execution
# =============================================================================

# Print a section header
def print_header(title: str):
    """
    Print a section header for better readability.
    """
    print("\n" + "=" * 80)
    print(f"🟢 {title}")
    print("=" * 80)

# Print the SQL statement being executed
def print_sql(sql: str):
    """
    Print the SQL statement before execution.
    """
    print(f"\n🟡 SQL:\n{sql.strip()}")

# Print the result DataFrame
def print_result(df: pd.DataFrame):
    """
    Print the result DataFrame in a readable format.
    """
    if df.empty:
        print("\n🟢 Result: (no rows returned)\n")
    else:
        print("\n🟢 Result:\n")
        print(df.to_string(index=False))

# Print the error traceback
def print_error(e: Exception):
    """
    Print the error traceback if SQL execution fails.
    """
    print("\n🔴 Error occurred:")
    traceback.print_exc()

# Execute a SQL statement and print the result
def execute(sql: str):
    """
    Execute a SQL statement and print the result and execution time.
    """
    print_sql(sql)
    try:
        start = time.time()  # Optional: start time for execution time measurement
        cursor.execute(sql)
        result = cursor.fetchallarrow()  # Arrow Table
        df = result.to_pandas()  # Optional: convert to DataFrame for better display
        print_result(df)
        print(f"\n⏱️  Execution time: {time.time() - start:.3f} seconds")
    except Exception as e:
        print_error(e)
```

### Step 4. Interact with StarRocks

This section will guide you through some basic operations, such as creating a table, loading data, checking table metadata, setting variables, and running queries.

:::note

The examples of output listed below are implemented based on the optional modules and utility functions described in the preceding steps.

:::

1. Create a database and a table where the data will be loaded, and check the table schema.

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

   Example output:

   ```SQL
   ================================================================================
   🟢 Step 1: Drop and Create Database
   ================================================================================
   
   🟡 SQL:
   DROP DATABASE IF EXISTS sr_arrow_flight_sql FORCE;
   /Users/zuopufan/starrocks/test/venv/lib/python3.9/site-packages/adbc_driver_manager/dbapi.py:307: Warning: Cannot disable autocommit; conn will not be DB-API 2.0 compliant
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

2. Insert data, run some queries, and set variables.

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

   Example output:

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
   
     Variable_name        Value
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

### Step 5. Close connection

Include the following section in your code to close the connection.

```Python
# Step 7: Close
print_header("Step 7: Close Connection")
cursor.close()
conn.close()
print("✅ Test completed successfully.")
```

Example output:

```Python
================================================================================
🟢 Step 7: Close Connection
================================================================================
✅ Test completed successfully.

Process finished with exit code 0
```

## Use cases of large-scale data transfer

### Python

After connecting to StarRocks (with Arrow Flight SQL support) via the ADBC Driver in Python, you can use various ADBC APIs to load the Clickbench dataset from StarRocks into Python.

Code example:

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

The results indicate that loading 1 million rows of Clickbench dataset (105 columns, 780 MB) from StarRocks took only 3 seconds.

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

### Arrow Flight SQL JDBC Driver

The Arrow Flight SQL protocol provides an open-source JDBC driver that is compatible with the standard JDBC interface. You can easily integrate it into various BI tools (such as Tableau, Power BI, DBeaver, etc.) to access the StarRocks database, just as you would with a traditional JDBC driver. A significant advantage of this driver is its support for high-speed data transfer based on Apache Arrow, which greatly improves the efficiency of query and data transmission. The usage is almost identical to that of a traditional MySQL JDBC driver. You only need to replace `jdbc:mysql` with `jdbc:arrow-flight-sql` in the connection URL to seamlessly switch. The query results are still returned in the standard `ResultSet` format, ensuring compatibility with existing JDBC processing logic.

:::note

If you want to debug in IntelliJ IDEA, you must add the following option to `Build and run` in `Run/Debug Configurations`:

```Bash
--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED
```

:::

<details>

  <summary>POM dependencies</summary>

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

Code example:

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

Execution results:

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

### Java ADBC Driver

The Arrow Flight SQL protocol provides an open-source JDBC driver that is compatible with the standard JDBC interface. You can easily integrate it into various BI tools (such as Tableau, Power BI, DBeaver, etc.) to access the StarRocks database, just as you would with a traditional JDBC driver. A significant advantage of this driver is its support for high-speed data transfer based on Apache Arrow, which greatly improves the efficiency of query and data transmission. The usage is almost identical to that of a traditional MySQL JDBC driver. You only need to replace `jdbc:mysql` with `jdbc:arrow-flight-sql` in the connection URL to seamlessly switch. The query results are still returned in the standard `ResultSet` format, ensuring compatibility with existing JDBC processing logic.

:::note

If you want to debug in IntelliJ IDEA, you must add the following option to `Build and run` in `Run/Debug Configurations`:

```Bash
--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED
```

:::

<details>

  <summary>POM dependencies</summary>

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

Similar to that in Python, you can also directly create an ADBC client in Java to read data from StarRocks.

In this process, you first need to obtain FlightInfo, and then connect to each Endpoint to fetch the data.

Code example:

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

Currently, the official Arrow Flight project has no plans to support Spark or Flink. In the future, support will be gradually added to allow [starrocks-spark-connector](https://github.com/qwshen/spark-flight-connector) to access StarRocks via Arrow Flight SQL, with expected read performance improvements several times.

When accessing StarRocks with Spark, in addition to the traditional JDBC or Java client methods, you can also use the open-source Spark-Flight-Connector component to directly read from and write to the StarRocks Flight SQL Server as a Spark DataSource. This approach, based on the Apache Arrow Flight protocol, offers the following significant advantages:

- **High-performance data transfer** Spark-Flight-Connector uses Apache Arrow as the data transfer format, enabling zero-copy, highly efficient data exchange. The conversion between StarRocks' `internal Block` data format and Arrow is highly efficient, achieving performance improvements of up to 10 times compared to traditional `CSV` or `JDBC` methods, and significantly reducing data transfer overhead.
- **Native support for complex data types** The Arrow data format natively supports complex types (such as `Map`, `Array`, `Struct`, etc.), enabling better adaptation to StarRocks' complex data models compared to traditional JDBC methods, and enhancing data expressiveness and compatibility.
- **Support for read, write, and streaming write** The component supports Spark as a Flight SQL client for efficient read and write operations, including `insert`, `merge`, `update`, and `delete` DML statements, and even supports streaming write, making it suitable for real-time data processing scenarios.
- **Support for predicate pushdown and column pruning** When reading data, Spark-Flight-Connector supports predicate pushdown and column pruning, enabling data filtering and column selection on the StarRocks side, significantly reducing the amount of data transferred and improving query performance.
- **Support for aggregation pushdown and parallel read** Aggregation operations (such as `sum`, `count`, `max`, `min`, etc.) can be pushed down to StarRocks for execution, reducing the computational load on Spark. Parallel reading based on partitioning is also supported, improving read efficiency in large data scenarios.
- **Better for big data scenarios** Compared to traditional JDBC methods, the Flight SQL protocol is better suited for large-scale, high-concurrency access scenarios, enabling StarRocks to fully leverage its high-performance analytical capabilities.

## Appendix

The following is the complete code example in the usage tutorial.

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
import pandas as pd       # Optional: for better result display using DataFrame
import traceback          # Optional: for detailed error traceback during SQL execution
import time               # Optional: for measuring SQL execution time

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
    Print a section header for better readability.
    """
    print("\n" + "=" * 80)
    print(f"🟢 {title}")
    print("=" * 80)


def print_sql(sql: str):
    """
    Print the SQL statement before execution.
    """
    print(f"\n🟡 SQL:\n{sql.strip()}")


def print_result(df: pd.DataFrame):
    """
    Print the result DataFrame in a readable format.
    """
    if df.empty:
        print("\n🟢 Result: (no rows returned)\n")
    else:
        print("\n🟢 Result:\n")
        print(df.to_string(index=False))


def print_error(e: Exception):
    """
    Print the error traceback if SQL execution fails.
    """
    print("\n🔴 Error occurred:")
    traceback.print_exc()


def execute(sql: str):
    """
    Execute a SQL statement and print the result and execution time.
    """
    print_sql(sql)
    try:
        start = time.time()  # Start time for execution time measurement
        cursor.execute(sql)
        result = cursor.fetchallarrow()  # Arrow Table
        df = result.to_pandas()          # Convert to DataFrame for better display
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
