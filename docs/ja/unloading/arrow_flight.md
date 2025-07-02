---
displayed_sidebar: docs
---

# Arrow Flight SQL を使用して StarRocks からデータを読み取る

v3.5.1 以降、StarRocks は Apache Arrow Flight SQL プロトコルを介した接続をサポートしています。

このソリューションは、StarRocks のカラム型実行エンジンからクライアントへの完全なカラム型データ転送パイプラインを確立し、従来の JDBC および ODBC インターフェースで一般的に見られる頻繁な行-カラム変換とシリアル化のオーバーヘッドを排除します。これにより、StarRocks はゼロコピー、低レイテンシ、高スループットでデータを転送できます。

## 使用方法

Arrow Flight SQL プロトコルを介して Python ADBC Driver を使用して StarRocks に接続し、操作する手順は次のとおりです。完全なコード例については [Appendix](#appendix) を参照してください。

:::note

Python 3.9 以降が前提条件です。

:::

### ステップ 1. ライブラリのインストール

`pip` を使用して PyPI から `adbc_driver_manager` と `adbc_driver_flightsql` をインストールします。

```Bash
pip install adbc_driver_manager
pip install adbc_driver_flightsql
```

次のモジュールまたはライブラリをコードにインポートします。

- 必要なライブラリ:

```Python
import adbc_driver_manager
import adbc_driver_flightsql.dbapi as flight_sql
```

- 使いやすさとデバッグのためのオプションモジュール:

```Python
import pandas as pd       # オプション: DataFrame を使用した結果表示の改善
import traceback          # オプション: SQL 実行中の詳細なエラートレースバック
import time               # オプション: SQL 実行時間の測定
```

### ステップ 2. StarRocks に接続する

:::note

IntelliJ IDEA でサービスを実行する場合、`Run/Debug Configurations` の `Build and run` に次のオプションを追加する必要があります。

```Bash
--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED
```

:::

#### StarRocks の設定

Arrow Flight SQL を介して StarRocks に接続する前に、Arrow Flight SQL サービスが有効になり、指定されたポートでリッスンしていることを確認するために、FE および BE ノードを設定する必要があります。

FE 設定ファイル **fe.conf** と BE 設定ファイル **be.conf** の両方で、`arrow_flight_port` を使用可能なポートに設定します。設定ファイルを変更した後、FE および BE サービスを再起動して、変更を有効にします。

例:

```Properties
arrow_flight_port = 9408
```

#### 接続の確立

クライアント側で、次の情報を使用して Arrow Flight SQL クライアントを作成します。

- StarRocks FE のホストアドレス
- StarRocks FE で Arrow Flight がリッスンしているポート
- 必要な権限を持つ StarRocks ユーザーのユーザー名とパスワード

例:

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

接続が確立された後、返されたカーソルを介して SQL ステートメントを実行することで StarRocks と対話できます。

### ステップ 3. (オプション) ユーティリティ関数を事前定義する

これらの関数は、出力をフォーマットし、フォーマットを標準化し、デバッグを簡素化するために使用されます。テストのためにコード内でオプションで定義できます。

```Python
# =============================================================================
# 出力フォーマットの改善と SQL 実行のためのユーティリティ関数
# =============================================================================

# セクションヘッダーを印刷
def print_header(title: str):
    """
    読みやすさを向上させるためにセクションヘッダーを印刷します。
    """
    print("\n" + "=" * 80)
    print(f"🟢 {title}")
    print("=" * 80)

# 実行される SQL ステートメントを印刷
def print_sql(sql: str):
    """
    実行前に SQL ステートメントを印刷します。
    """
    print(f"\n🟡 SQL:\n{sql.strip()}")

# 結果の DataFrame を印刷
def print_result(df: pd.DataFrame):
    """
    結果の DataFrame を読みやすい形式で印刷します。
    """
    if df.empty:
        print("\n🟢 Result: (no rows returned)\n")
    else:
        print("\n🟢 Result:\n")
        print(df.to_string(index=False))

# エラートレースバックを印刷
def print_error(e: Exception):
    """
    SQL 実行が失敗した場合にエラートレースバックを印刷します。
    """
    print("\n🔴 Error occurred:")
    traceback.print_exc()

# SQL ステートメントを実行し、結果を印刷
def execute(sql: str):
    """
    SQL ステートメントを実行し、結果と実行時間を印刷します。
    """
    print_sql(sql)
    try:
        start = time.time()  # 実行時間測定のための開始時間
        cursor.execute(sql)
        result = cursor.fetchallarrow()  # Arrow Table
        df = result.to_pandas()  # DataFrame に変換して表示を改善
        print_result(df)
        print(f"\n⏱️  Execution time: {time.time() - start:.3f} seconds")
    except Exception as e:
        print_error(e)
```

### ステップ 4. StarRocks と対話する

このセクションでは、テーブルの作成、データのロード、テーブルメタデータの確認、変数の設定、クエリの実行などの基本操作を案内します。

:::note

以下に示す出力例は、前述のステップで説明したオプションモジュールとユーティリティ関数に基づいて実装されています。

:::

1. データがロードされるデータベースとテーブルを作成し、テーブルスキーマを確認します。

   ```Python
   # ステップ 1: データベースの削除と作成
   print_header("Step 1: Drop and Create Database")
   execute("DROP DATABASE IF EXISTS sr_arrow_flight_sql FORCE;")
   execute("SHOW DATABASES;")
   execute("CREATE DATABASE sr_arrow_flight_sql;")
   execute("SHOW DATABASES;")
   execute("USE sr_arrow_flight_sql;")
   
   # ステップ 2: テーブルの作成
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

   出力例:

   ```SQL
   ================================================================================
   🟢 Step 1: Drop and Create Database
   ================================================================================
   
   🟡 SQL:
   DROP DATABASE IF EXISTS sr_arrow_flight_sql FORCE;
      /Users/zuopufan/starrocks/test/venv/lib/python3.9/site-packages/adbc_driver_manager/dbapi.py:307: Warning: Cannot disable autocommit; conn l not be DB-API 2.0 compliant
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
   
                         Table                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               Create Table
      sr_arrow_flight_sql_test CREATE TABLE `sr_arrow_flight_sql_test` (\n  `k0` int(11) NULL COMMENT "",\n  `k1` double NULL COMMENT "",\n  `k2` varchar(32) NULL DEFAULT "" COMMENT "",\n  `k3` decimal(27, 9) NULL DEFAULT "0" COMMENT "",\n  `k4` bigint(20) NULL DEFAULT "10" COMMENT "",\n  `k5` date NULL COMMENT ""\n) ENGINE=OLAP \nDUPLICATE KEY(`k0`)\nDISTRIBUTED BY HASH(`k5`) BUCKETS 5 \nPROPERTIES (\n"compression" = 4",\n"fast_schema_evolution" = "true",\n"replicated_storage" = "true",\n"replication_num" = "1"\n);
   
   ⏱️  Execution time: 0.005 seconds
   ```

2. データを挿入し、いくつかのクエリを実行し、変数を設定します。

   ```Python
   # ステップ 3: データの挿入
   print_header("Step 3: Insert Data")
   execute("""
   INSERT INTO sr_arrow_flight_sql_test VALUES
       (0, 0.1, "ID", 0.0001, 1111111111, '2025-04-21'),
       (1, 0.20, "ID_1", 1.00000001, 0, '2025-04-21'),
       (2, 3.4, "ID_1", 3.1, 123456, '2025-04-22'),
       (3, 4, "ID", 4, 4, '2025-04-22'),
       (4, 122345.54321, "ID", 122345.54321, 5, '2025-04-22');
   """)
   
   # ステップ 4: データのクエリ
   print_header("Step 4: Query Data")
   execute("SELECT * FROM sr_arrow_flight_sql_test ORDER BY k0;")
   
   # ステップ 5: セッション変数
   print_header("Step 5: Session Variables")
   execute("SHOW VARIABLES LIKE '%query_mem_limit%';")
   execute("SET query_mem_limit = 2147483648;")
   execute("SHOW VARIABLES LIKE '%query_mem_limit%';")
   
   # ステップ 6: 集計クエリ
   print_header("Step 6: Aggregation Query")
   execute("""
   SELECT k5, SUM(k1) AS total_k1, COUNT(1) AS row_count, AVG(k3) AS avg_k3
   FROM sr_arrow_flight_sql_test
   GROUP BY k5
   ORDER BY k5;
   """)
   ```

   出力例:

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

### ステップ 5. 接続を閉じる

接続を閉じるために、次のセクションをコードに含めます。

```Python
# ステップ 7: 接続を閉じる
print_header("Step 7: Close Connection")
cursor.close()
conn.close()
print("✅ Test completed successfully.")
```

出力例:

```Python
================================================================================
🟢 Step 7: Close Connection
================================================================================
✅ Test completed successfully.

Process finished with exit code 0
```

## 大規模データ転送のユースケース

### Python

Python で ADBC Driver を介して StarRocks (Arrow Flight SQL サポート付き) に接続した後、さまざまな ADBC API を使用して StarRocks から Clickbench データセットをロードできます。

コード例:

```Python
#!/usr/bin/env python
# -*- coding: utf-8 -*-

import adbc_driver_manager
import adbc_driver_flightsql.dbapi as flight_sql
from datetime import datetime

# ----------------------------------------
# StarRocks Flight SQL 接続設定
# ----------------------------------------
# 必要に応じて URI と資格情報を置き換えます
my_uri = "grpc://127.0.0.1:9408"  # StarRocks のデフォルト Flight SQL ポート
my_db_kwargs = {
    adbc_driver_manager.DatabaseOptions.USERNAME.value: "root",
    adbc_driver_manager.DatabaseOptions.PASSWORD.value: "",
}

# SQL クエリ (ClickBench: hits テーブル)
# ----------------------------------------
# 必要に応じて実際のテーブルとデータセットに置き換えます
sql = "SELECT * FROM clickbench.hits LIMIT 1000000;"  # 100 万行を読み取る

# ----------------------------------------
# メソッド 1: fetchallarrow + to_pandas
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
# メソッド 2: fetch_df (推奨)
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
# メソッド 3: adbc_execute_partitions (並列読み取り用)
# ----------------------------------------
def test_execute_partitions():
    conn = flight_sql.connect(uri=my_uri, db_kwargs=my_db_kwargs)
    cursor = conn.cursor()
    start = datetime.now()
    partitions, schema = cursor.adbc_execute_partitions(sql)

    # 最初のパーティションを読み取る (デモ用)
    cursor.adbc_read_partition(partitions[0])
    arrow_table = cursor.fetchallarrow()
    df = arrow_table.to_pandas()
    duration = datetime.now() - start

    print("\n[Method 3] adbc_execute_partitions (parallel read)")
    print(f"Time taken: {duration}, Partitions: {len(partitions)}, Rows: {len(df)}")
    print(df.info(memory_usage='deep'))

# ----------------------------------------
# すべてのテストを実行
# ----------------------------------------
if __name__ == "__main__":
    test_fetchallarrow()
    test_fetch_df()
    test_execute_partitions()
```

結果は、StarRocks から Clickbench データセット (105 列、780 MB) の 100 万行をロードするのにわずか 3 秒しかかからなかったことを示しています。

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

### Arrow Flight SQL JDBC ドライバー

Arrow Flight SQL プロトコルは、標準 JDBC インターフェースと互換性のあるオープンソースの JDBC ドライバーを提供します。これを使用して、Tableau、Power BI、DBeaver などのさまざまな BI ツールに簡単に統合し、StarRocks データベースにアクセスできます。従来の JDBC ドライバーと同様に、接続 URL で `jdbc:mysql` を `jdbc:arrow-flight-sql` に置き換えるだけでシームレスに切り替えることができます。このドライバーの大きな利点は、Apache Arrow に基づく高速データ転送をサポートしており、クエリとデータ転送の効率を大幅に向上させることです。クエリ結果は標準の `ResultSet` 形式で返されるため、既存の JDBC 処理ロジックとの互換性が確保されます。

:::note

IntelliJ IDEA でデバッグする場合、`Run/Debug Configurations` の `Build and run` に次のオプションを追加する必要があります。

```Bash
--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED
```

:::

<details>

  <summary>POM 依存関係</summary>

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

コード例:

```Java
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Arrow Flight SQL JDBC ドライバーと StarRocks の統合テスト。
 *
 * このテストは以下をカバーします:
 *  - 基本的な DDL および DML 操作
 *  - クエリの実行と結果の検証
 *  - 無効な SQL のエラーハンドリング
 *  - クエリのキャンセル (長時間実行されるクエリでシミュレート)
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
            // Arrow Flight SQL JDBC ドライバーをロード
            Class.forName("org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver");

            try (Connection conn = DriverManager.getConnection(JDBC_URL, USER, PASSWORD);
                    Statement stmt = conn.createStatement()) {

                // 基本的な DDL および DML 操作
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

                // エラーハンドリング: 存在しないテーブルへのクエリ
                try {
                    testQuery(stmt, "SELECT * FROM not_exist_table;");
                } catch (Exception e) {
                    System.out.println("✅ Expected error (table not exist): " + e.getMessage());
                }

                // エラーハンドリング: SQL 構文エラー
                try {
                    testQuery(stmt, "SELECT * FROM arrow_demo.test WHERE id = ;");
                } catch (Exception e) {
                    System.out.println("✅ Expected error (syntax error): " + e.getMessage());
                }

                // クエリキャンセルテスト
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
     * クエリを実行し、結果をコンソールに出力します。
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
     * 更新 (DDL または DML) を実行し、結果をコンソールに出力します。
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

実行結果:

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

### Java ADBC ドライバー

Arrow Flight SQL プロトコルは、標準 JDBC インターフェースと互換性のあるオープンソースの JDBC ドライバーを提供します。これを使用して、Tableau、Power BI、DBeaver などのさまざまな BI ツールに簡単に統合し、StarRocks データベースにアクセスできます。従来の JDBC ドライバーと同様に、接続 URL で `jdbc:mysql` を `jdbc:arrow-flight-sql` に置き換えるだけでシームレスに切り替えることができます。このドライバーの大きな利点は、Apache Arrow に基づく高速データ転送をサポートしており、クエリとデータ転送の効率を大幅に向上させることです。クエリ結果は標準の `ResultSet` 形式で返されるため、既存の JDBC 処理ロジックとの互換性が確保されます。

:::note

IntelliJ IDEA でデバッグする場合、`Run/Debug Configurations` の `Build and run` に次のオプションを追加する必要があります。

```Bash
--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED
```

:::

<details>

  <summary>POM 依存関係</summary>

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

Python と同様に、Java でも直接 ADBC クライアントを作成して StarRocks からデータを読み取ることができます。

このプロセスでは、まず FlightInfo を取得し、次に各 Endpoint に接続してデータを取得します。

コード例:

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

現在、公式の Arrow Flight プロジェクトでは Spark や Flink のサポートを計画していません。将来的には、[starrocks-spark-connector](https://github.com/qwshen/spark-flight-connector) を介して Arrow Flight SQL を使用して StarRocks にアクセスできるようにサポートが徐々に追加され、読み取りパフォーマンスの向上が期待されます。

Spark で StarRocks にアクセスする際には、従来の JDBC や Java クライアントの方法に加えて、オープンソースの Spark-Flight-Connector コンポーネントを使用して、StarRocks Flight SQL サーバーから直接読み書きすることができます。この方法は、Apache Arrow Flight プロトコルに基づいており、以下のような大きな利点があります。

- **高性能データ転送** Spark-Flight-Connector は Apache Arrow をデータ転送フォーマットとして使用し、ゼロコピーで高効率なデータ交換を実現します。StarRocks の `internal Block` データフォーマットと Arrow の間の変換は非常に効率的で、従来の `CSV` や `JDBC` メソッドと比較して最大 10 倍のパフォーマンス向上を達成し、データ転送のオーバーヘッドを大幅に削減します。
- **複雑なデータ型のネイティブサポート** Arrow データフォーマットは複雑な型 (例えば `Map`、`Array`、`Struct` など) をネイティブにサポートしており、従来の JDBC メソッドと比較して StarRocks の複雑なデータモデルにより適応し、データの表現力と互換性を向上させます。
- **読み取り、書き込み、ストリーミング書き込みのサポート** コンポーネントは Spark を Flight SQL クライアントとして使用して効率的な読み書き操作をサポートし、`insert`、`merge`、`update`、`delete` の DML ステートメントを含み、ストリーミング書き込みもサポートしているため、リアルタイムデータ処理シナリオに適しています。
- **述語プッシュダウンとカラムプルーニングのサポート** データを読み取る際に、Spark-Flight-Connector は述語プッシュダウンとカラムプルーニングをサポートし、StarRocks 側でデータフィルタリングとカラム選択を可能にし、転送されるデータ量を大幅に削減し、クエリパフォーマンスを向上させます。
- **集計プッシュダウンと並列読み取りのサポート** 集計操作 (例えば `sum`、`count`、`max`、`min` など) を StarRocks にプッシュダウンして実行し、Spark の計算負荷を軽減します。また、パーティショニングに基づく並列読み取りもサポートし、大規模データシナリオでの読み取り効率を向上させます。
- **ビッグデータシナリオに適している** 従来の JDBC メソッドと比較して、Flight SQL プロトコルは大規模で高コンカレンシーなアクセスシナリオにより適しており、StarRocks がその高性能な分析能力を最大限に活用できるようにします。

## Appendix

以下は、使用方法のチュートリアルにおける完全なコード例です。

```Python
# =============================================================================
# StarRocks Arrow Flight SQL テストスクリプト
# =============================================================================
# pip install adbc_driver_manager adbc_driver_flightsql pandas
# =============================================================================

# =============================================================================
# Arrow Flight SQL を介して StarRocks に接続するための必要なコアモジュール
# =============================================================================
import adbc_driver_manager
import adbc_driver_flightsql.dbapi as flight_sql

# =============================================================================
# 使いやすさとデバッグのためのオプションモジュール
# =============================================================================
import pandas as pd       # オプション: DataFrame を使用した結果表示の改善
import traceback          # オプション: SQL 実行中の詳細なエラートレースバック
import time               # オプション: SQL 実行時間の測定

# =============================================================================
# StarRocks Flight SQL 設定
# =============================================================================
FE_HOST = "127.0.0.1"
FE_PORT = 9408

# =============================================================================
# StarRocks に接続
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
# 出力フォーマットの改善と SQL 実行のためのユーティリティ関数
# =============================================================================

def print_header(title: str):
    """
    読みやすさを向上させるためにセクションヘッダーを印刷します。
    """
    print("\n" + "=" * 80)
    print(f"🟢 {title}")
    print("=" * 80)


def print_sql(sql: str):
    """
    実行前に SQL ステートメントを印刷します。
    """
    print(f"\n🟡 SQL:\n{sql.strip()}")


def print_result(df: pd.DataFrame):
    """
    結果の DataFrame を読みやすい形式で印刷します。
    """
    if df.empty:
        print("\n🟢 Result: (no rows returned)\n")
    else:
        print("\n🟢 Result:\n")
        print(df.to_string(index=False))


def print_error(e: Exception):
    """
    SQL 実行が失敗した場合にエラートレースバックを印刷します。
    """
    print("\n🔴 Error occurred:")
    traceback.print_exc()


def execute(sql: str):
    """
    SQL ステートメントを実行し、結果と実行時間を印刷します。
    """
    print_sql(sql)
    try:
        start = time.time()  # 実行時間測定のための開始時間
        cursor.execute(sql)
        result = cursor.fetchallarrow()  # Arrow Table
        df = result.to_pandas()          # DataFrame に変換して表示を改善
        print_result(df)
        print(f"\n⏱️  Execution time: {time.time() - start:.3f} seconds")
    except Exception as e:
        print_error(e)

# =============================================================================
# ステップ 1: データベースの削除と作成
# =============================================================================
print_header("Step 1: Drop and Create Database")
execute("DROP DATABASE IF EXISTS sr_arrow_flight_sql FORCE;")
execute("SHOW DATABASES;")
execute("CREATE DATABASE sr_arrow_flight_sql;")
execute("SHOW DATABASES;")
execute("USE sr_arrow_flight_sql;")

# =============================================================================
# ステップ 2: テーブルの作成
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
# ステップ 3: データの挿入
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
# ステップ 4: データのクエリ
# =============================================================================
print_header("Step 4: Query Data")
execute("SELECT * FROM sr_arrow_flight_sql_test ORDER BY k0;")

# =============================================================================
# ステップ 5: セッション変数
# =============================================================================
print_header("Step 5: Session Variables")
execute("SHOW VARIABLES LIKE '%query_mem_limit%';")
execute("SET query_mem_limit = 2147483648;")
execute("SHOW VARIABLES LIKE '%query_mem_limit%';")

# =============================================================================
# ステップ 6: 集計クエリ
# =============================================================================
print_header("Step 6: Aggregation Query")
execute("""
SELECT k5, SUM(k1) AS total_k1, COUNT(1) AS row_count, AVG(k3) AS avg_k3
FROM sr_arrow_flight_sql_test
GROUP BY k5
ORDER BY k5;
""")

# =============================================================================
# ステップ 7: 接続を閉じる
# =============================================================================
print_header("Step 7: Close Connection")
cursor.close()
conn.close()
print("✅ Test completed successfully.")
```