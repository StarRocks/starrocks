---
displayed_sidebar: docs
---

# Arrow Flight SQL を使用して StarRocks と対話する

v3.5.1 以降、StarRocks は Apache Arrow Flight SQL プロトコルによる接続をサポートしています。

## 概要

Arrow Flight SQL プロトコルを使用すると、通常の DDL、DML、DQL ステートメントを実行でき、Python コードまたは Java コードを使用して Arrow Flight SQL ADBC または JDBC ドライバー経由で大規模データを読み取ることができます。

このソリューションは、StarRocks の列指向実行エンジンからクライアントまで、完全な列指向データ転送パイプラインを確立し、従来の JDBC および ODBC インターフェースで一般的に見られる頻繁な行列変換とシリアライゼーションのオーバーヘッドを排除します。これにより、StarRocks はゼロコピー、低レイテンシ、高スループットでデータを転送できます。

### シナリオ

Arrow Flight SQL の統合により、StarRocks は特に以下のユースケースに適しています：

- データサイエンスワークフロー：Pandas や Apache Arrow などのツールが列指向データを必要とする場合。
- データレイク分析：大規模データセットへの高スループット・低レイテンシアクセスが必要な場合。
- 機械学習：高速なイテレーションと処理速度が重要な場合。
- リアルタイム分析プラットフォーム：最小限の遅延でデータを提供する必要がある場合。

Arrow Flight SQL を使用することで、以下のメリットが得られます：

- エンドツーエンドの列指向データ転送により、列形式と行形式の間のコストのかかる変換を排除。
- ゼロコピーのデータ移動により、CPU およびメモリのオーバーヘッドを削減。
- 低レイテンシと極めて高いスループットにより、分析と応答性を向上。

### 技術的アプローチ

従来、StarRocks はクエリ結果を内部的に列指向の Block 構造で管理しています。しかし、JDBC、ODBC、または MySQL プロトコルを使用する場合、データは以下の処理が必要です：

1. サーバー上で行ベースのバイト列にシリアライズされる。
2. ネットワーク経由で転送される。
3. ターゲット構造に逆シリアライズされる（多くの場合、列形式への再変換が必要）。

この3ステップのプロセスにより、以下の問題が生じます：

- 高いシリアライゼーション/デシリアライゼーションのオーバーヘッド。
- 複雑なデータ変換。
- データ量に比例して増大するレイテンシ。

Arrow Flight SQL との統合は、以下の方法でこれらの問題を解決します：

- StarRocks の実行エンジンからクライアントまで、エンドツーエンドで列指向フォーマットを維持する。
- 分析ワークロード向けに最適化された Apache Arrow のインメモリ列指向表現を活用する。
- Arrow Flight のプロトコルを高速転送に使用し、中間変換なしで効率的なストリーミングを実現する。

![Arrow Flight](../_assets/arrow_flight.png)

この設計により、真のゼロコピー転送が実現され、従来の方法よりも高速かつリソース効率に優れています。

さらに、StarRocks は Arrow Flight SQL 向けのユニバーサル JDBC ドライバーを提供しており、アプリケーションは JDBC 互換性や他の Arrow Flight 対応システムとの相互運用性を犠牲にすることなく、この高性能転送パスを採用できます。

BE ノードがクライアントから直接アクセスできないデプロイ環境（プライベートネットワークや Kubernetes クラスターなど）向けに、StarRocks は Arrow Flight プロキシ機能を提供しています。有効にすると、FE がプロキシとして機能し、BE ノードからクライアントへ Arrow データをルーティングすることで、ネットワークトポロジーの制約に対応しながら列指向転送のメリットを維持します。このプロキシモードはわずかなパフォーマンスオーバーヘッドが発生しますが、BE への直接接続が利用できない環境でも Arrow Flight SQL アクセスを可能にします。

### パフォーマンス比較

包括的なテストにより、データ取得速度の大幅な改善が実証されています。さまざまなデータ型（整数、浮動小数点、文字列、ブール値、混合カラム）において、Arrow Flight SQL は従来の PyMySQL および Pandas の read_sql インターフェースを一貫して上回りました。主な結果は以下のとおりです：

- 1,000 万行の整数データの読み取りでは、実行時間が約 35 秒から 0.4 秒に短縮（約 85 倍高速化）。
- 混合カラムテーブルでは、パフォーマンス改善が 160 倍の高速化に達した。
- 比較的単純なクエリ（例：単一の文字列カラム）でも、パフォーマンス向上は 12 倍を超えた。

平均して、Arrow Flight SQL は以下を達成しました：

- クエリの複雑さとデータ型に応じて、20 倍から 160 倍の転送時間の高速化。
- 冗長なシリアライゼーションステップの排除により、CPU およびメモリ使用量が明確に削減。

これらのパフォーマンス向上は、より高速なダッシュボード、より応答性の高いデータサイエンスワークフロー、そしてリアルタイムでより大規模なデータセットを分析する能力として直接反映されます。

クライアントコードでこれらの数値に到達する方法の詳細な内訳（JDBCアクセサーメソッド、生の`VectorSchemaRoot`消費、Parquetライター）、およびMySQL JDBCに対する各チューニングステップの測定済みスピードアップについては、[Arrow Flight SQL ベストプラクティス](./arrow_flight_best_practices.md).

## 使用方法

Arrow Flight SQLプロトコルを介してPython ADBAドライバーを使用してStarRocksに接続し、操作するには、次の手順に従ってください。完全なコード例については、[付録](#appendix)を参照してください。

:::note

Python 3.9以降が前提条件です。

:::

### ステップ1. ライブラリのインストール

`pip`を使用して、PyPIから`adbc_driver_manager`と`adbc_driver_flightsql`をインストールします：

```Bash
pip install adbc_driver_manager
pip install adbc_driver_flightsql
```

次のモジュールまたはライブラリをコードにインポートします：

- 必須ライブラリ：

```Python
import adbc_driver_manager
import adbc_driver_flightsql.dbapi as flight_sql
```

- 使いやすさとデバッグのためのオプションモジュール：

```Python
import pandas as pd       # Optional: for better result display using DataFrame
import traceback          # Optional: for detailed error traceback during SQL execution
import time               # Optional: for measuring SQL execution time
```

### ステップ2. StarRocksへの接続

:::note

- コマンドラインを使用してFEサービスを起動する場合は、次のいずれかの方法を使用できます：

  - 環境変数`JAVA_TOOL_OPTIONS`を指定します。

    ```Bash
    export JAVA_TOOL_OPTIONS="--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED"
    ```

  - FE設定項目`JAVA_OPTS`を**fe.conf**で指定します。この方法では、他の`JAVA_OPTS`値を追加できます。

    ```Bash
    JAVA_OPTS="--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED ..."
    ```

- IntelliJ IDEAでサービスを実行する場合は、`Run/Debug Configurations`の`Build and run`に次のオプションを追加する必要があります：

  ```Bash
  --add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED
  ```

:::

#### StarRocksの設定

Arrow Flight SQL経由でStarRocksに接続する前に、まずFEおよびBEノードを設定して、Arrow Flight SQLサービスが有効になり、指定されたポートでリッスンしていることを確認する必要があります。

FE設定ファイル**fe.conf**およびBE設定ファイル**be.conf**の両方で、`arrow_flight_port`を利用可能なポートに設定します。設定ファイルを変更した後、変更を有効にするためにFEおよびBEサービスを再起動してください。

:::note

FEとBEには異なる`arrow_flight_port`を設定する必要があります。

:::

例：

```Properties
// fe.conf
arrow_flight_port = 9408
// be.conf
arrow_flight_port = 9419
```

#### Arrow Flight Proxyの設定（オプション）

BEノードがクライアントアプリケーションから直接アクセスできない場合（例えば、プライベートネットワークやKubernetes環境にデプロイされている場合）、FE上でArrow Flightプロキシ機能を有効にして、BEノードからのデータをFEを通じてルーティングできます。

プロキシ機能は2つのグローバル変数によって制御されます：

- `arrow_flight_proxy_enabled`：プロキシモードを有効にするかどうかを制御します。デフォルトは`true`です。有効にすると、わずかなパフォーマンスオーバーヘッドが発生します。
- `arrow_flight_proxy`：プロキシのホスト名を指定します。空（デフォルト）の場合、現在のFEノードがプロキシとして機能します。別のプロキシエンドポイントを使用する場合は、特定のホスト名に設定できます。

これらの変数をすべてのセッションに対してグローバルに設定するには:

```sql
-- プロキシモードの有効化または無効化（デフォルトで有効）
SET GLOBAL arrow_flight_proxy_enabled = true;

-- 特定のプロキシホスト名を設定する（オプション、デフォルトは現在のFE）
SET GLOBAL arrow_flight_proxy = 'your-proxy-hostname:Port';
```

:::note

- プロキシ機能はデフォルトで有効になっており、BEへの直接接続と比較してスループットが8〜10%低下する場合があります。クライアントがBEノードへの直接ネットワークアクセスを持っている場合、またはFE側のメモリリソースが限られている場合は、プロキシを無効にして最適なパフォーマンスを実現できます: `SET GLOBAL arrow_flight_proxy_enabled = false;`。
- `arrow_flight_proxy` が空の場合、チケットはクライアントが最初に接続したFEノードを経由して自動的にルーティングされます。
- **重要**: `arrow_flight_proxy` および `arrow_flight_proxy_enabled` の設定は、`SET GLOBAL` を使用してグローバルに設定する必要があります。セッションレベルの設定はサポートされていません。
- **セッションの再起動が必要**: プロキシ設定の変更は新しいセッションにのみ影響します。既存のArrow Flight SQLセッションは、再接続するまで元の設定を使用し続けます。

:::

#### 接続を確立する

クライアント側で、以下の情報を使用してArrow Flight SQLクライアントを作成します:

- StarRocks FEのホストアドレス
- StarRocks FEでArrow Flightがリッスンに使用するポート
- 必要な権限を持つStarRocksユーザーのユーザー名とパスワード

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

接続が確立されると、返されたCursorを通じてSQL文を実行することでStarRocksと対話できます。

### ステップ3. （オプション）ユーティリティ関数を事前定義する

これらの関数は、出力のフォーマット、形式の標準化、およびデバッグの簡略化に使用されます。テスト用にコード内でオプションとして定義できます。

```Python
# =============================================================================
# 出力フォーマットとSQL実行のためのユーティリティ関数
# =============================================================================

# セクションヘッダーを出力する
def print_header(title: str):
    """
    Print a section header for better readability.
    """
    print("\n" + "=" * 80)
    print(f"🟢 {title}")
    print("=" * 80)

# 実行中のSQL文を出力する
def print_sql(sql: str):
    """
    Print the SQL statement before execution.
    """
    print(f"\n🟡 SQL:\n{sql.strip()}")

# 結果のDataFrameを出力する
def print_result(df: pd.DataFrame):
    """
    Print the result DataFrame in a readable format.
    """
    if df.empty:
        print("\n🟢 Result: (no rows returned)\n")
    else:
        print("\n🟢 Result:\n")
        print(df.to_string(index=False))

# エラーのトレースバックを出力する
def print_error(e: Exception):
    """
    Print the error traceback if SQL execution fails.
    """
    print("\n🔴 Error occurred:")
    traceback.print_exc()

# SQL文を実行して結果を出力する
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

### ステップ4. StarRocksと対話する

このセクションでは、テーブルの作成、データのロード、テーブルメタデータの確認、変数の設定、クエリの実行など、基本的な操作を説明します。

:::note

以下に示す出力例は、前述のステップで説明したオプションモジュールおよびユーティリティ関数に基づいて実装されています。

:::

1. データをロードするデータベースとテーブルを作成し、テーブルスキーマを確認します。

   ```Python
   # ステップ1: データベースの削除と作成
   print_header("Step 1: Drop and Create Database")
   execute("DROP DATABASE IF EXISTS sr_arrow_flight_sql FORCE;")
   execute("SHOW DATABASES;")
   execute("CREATE DATABASE sr_arrow_flight_sql;")
   execute("SHOW DATABASES;")
   execute("USE sr_arrow_flight_sql;")

   # ステップ2: テーブルの作成
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

2. データを挿入し、いくつかのクエリを実行して、変数を設定します。

   ```Python
   # ステップ3: データの挿入
   print_header("Step 3: Insert Data")
   execute("""
   INSERT INTO sr_arrow_flight_sql_test VALUES
       (0, 0.1, "ID", 0.0001, 1111111111, '2025-04-21'),
       (1, 0.20, "ID_1", 1.00000001, 0, '2025-04-21'),
       (2, 3.4, "ID_1", 3.1, 123456, '2025-04-22'),
       (3, 4, "ID", 4, 4, '2025-04-22'),
       (4, 122345.54321, "ID", 122345.54321, 5, '2025-04-22');
   """)

   # ステップ4: データのクエリ
   print_header("Step 4: Query Data")
   execute("SELECT * FROM sr_arrow_flight_sql_test ORDER BY k0;")

   # ステップ5: セッション変数
   print_header("Step 5: Session Variables")
   execute("SHOW VARIABLES LIKE '%query_mem_limit%';")
   execute("SET query_mem_limit = 2147483648;")
   execute("SHOW VARIABLES LIKE '%query_mem_limit%';")

   # ステップ6: 集計クエリ
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

### ステップ5. 接続を閉じる

接続を閉じるために、以下のセクションをコードに含めてください。

```Python
# ステップ7: クローズ
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

PythonのADBCドライバーを使用してStarRocks（Arrow Flight SQLサポート付き）に接続した後、さまざまなADBC APIを使用してStarRocksからClickbenchデータセットをPythonにロードできます。

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
# 必要に応じてURIと認証情報を置き換えてください
my_uri = "grpc://127.0.0.1:9408"  # Default Flight SQL port for StarRocks
my_db_kwargs = {
    adbc_driver_manager.DatabaseOptions.USERNAME.value: "root",
    adbc_driver_manager.DatabaseOptions.PASSWORD.value: "",
}

# ----------------------------------------
# SQLクエリ（ClickBench: hitsテーブル）
# ----------------------------------------
# 必要に応じて実際のテーブルとデータセットに置き換えてください
sql = "SELECT * FROM clickbench.hits LIMIT 1000000;"  # Read 1 million rows

# ----------------------------------------
# 方法1: fetchallarrow + to_pandas
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
# 方法2: fetch_df（推奨）
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
# 方法3: adbc_execute_partitions（並列読み取り用）
# ----------------------------------------
def test_execute_partitions():
    conn = flight_sql.connect(uri=my_uri, db_kwargs=my_db_kwargs)
    cursor = conn.cursor()
    start = datetime.now()
    partitions, schema = cursor.adbc_execute_partitions(sql)

    # 最初のパーティションを読み取る（デモ用）
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

結果は、StarRocksから100万行のClickbenchデータセット（105列、780 MB）のロードにわずか3秒しかかからなかったことを示しています。

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

### Arrow Flight SQL JDBCドライバー

Arrow Flight SQLプロトコルは、標準JDBCインターフェースと互換性のあるオープンソースのJDBCドライバーを提供します。Tableau、Power BI、DBeaver などのさまざまなBIツールに簡単に統合して、従来のJDBCドライバーと同様にStarRocksデータベースにアクセスできます。このドライバーの大きな利点は、Apache Arrowに基づく高速データ転送をサポートしており、クエリとデータ転送の効率を大幅に向上させることです。使用方法は従来のMySQL JDBCドライバーとほぼ同じです。接続URLの `jdbc:mysql` を `jdbc:arrow-flight-sql` に置き換えるだけでシームレスに切り替えられます。クエリ結果は引き続き標準の `ResultSet` 形式で返され、既存のJDBC処理ロジックとの互換性が確保されます。

:::note

Java 9以降を使用している場合は、JDKの内部構造を公開するために `--add-opens=java.base/java.nio=ALL-UNNAMED` をJavaコードに追加する必要があります。そうしないと、特定のエラーが発生する場合があります。

- コマンドラインを使用してFEサービスを起動する場合は、次のいずれかの方法を使用できます:

  - 環境変数 `JAVA_TOOL_OPTIONS` を指定します。

    ```Bash
    export JAVA_TOOL_OPTIONS="--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED"
    ```

  - FE 設定項目 `JAVA_OPTS` を指定します（**fe.conf**）。これにより、他の `JAVA_OPTS` の値を追加できます。

    ```Bash
    JAVA_OPTS="--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED ..."
    ```

- IntelliJ IDEA でデバッグする場合は、`Run/Debug Configurations` の `Build and run` に以下のオプションを追加する必要があります：

  ```Bash
  --add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED
  ```

![Arrow Flight の例](../_assets/arrow_flight_example.png)

:::

<details>
  <summary><b>POM 依存関係を表示するにはここをクリック</b></summary>

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

コード例：

```Java
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

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
            // Arrow Flight SQL JDBCドライバーを読み込む
            Class.forName("org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver");

            try (Connection conn = DriverManager.getConnection(JDBC_URL, USER, PASSWORD);
                    Statement stmt = conn.createStatement()) {

                testUpdate(stmt, "DROP DATABASE IF EXISTS sr_arrow_flight_sql FORCE;");
                testQuery(stmt, "SHOW PROCESSLIST;");
                testUpdate(stmt, "CREATE DATABASE sr_arrow_flight_sql;");
                testQuery(stmt, "SHOW DATABASES;");
                testUpdate(stmt, "USE sr_arrow_flight_sql;");
                testUpdate(stmt, "CREATE TABLE sr_table_test (id INT, name STRING) ENGINE=OLAP PRIMARY KEY (id) " +
                        "DISTRIBUTED BY HASH(id) BUCKETS 1 " +
                        "PROPERTIES ('replication_num' = '1');");
                testUpdate(stmt, "INSERT INTO sr_table_test VALUES (1, 'Alice'), (2, 'Bob');");
                testQuery(stmt, "SELECT * FROM sr_arrow_flight_sql.sr_table_test;");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * Executes a query and prints the result to the console.
     */
    private static void testQuery(Statement stmt, String sql) throws Exception {
        System.out.println("Test Case: " + testCaseNum);
        System.out.println("▶ Executing query: " + sql);
        ResultSet rs = stmt.executeQuery(sql);
        try {
            System.out.println("Result:");
            int columnCount = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    System.out.print(rs.getString(i) + "\t");
                }
                System.out.println();
            }
        } finally {
            rs.close();
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

実行結果：

```Bash
Test Case: 1
▶ Executing update: DROP DATABASE IF EXISTS sr_arrow_flight_sql FORCE;
Result: ✅ Success

Test Case: 2
▶ Executing query: SHOW PROCESSLIST;
Result:
192.168.124.48_9010_1751449846872	16777217	root			Query	2025-07-02 18:46:49	0	OK	SHOW PROCESSLIST;	false	default_warehouse	

Test Case: 3
▶ Executing update: CREATE DATABASE sr_arrow_flight_sql;
Result: ✅ Success

Test Case: 4
▶ Executing query: SHOW DATABASES;
Result:
_statistics_	
information_schema	
sr_arrow_flight_sql	
sys	

Test Case: 5
▶ Executing update: USE sr_arrow_flight_sql;
Result: ✅ Success

Test Case: 6
▶ Executing update: CREATE TABLE sr_table_test (id INT, name STRING) ENGINE=OLAP PRIMARY KEY (id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num' = '1');
Result: ✅ Success

Test Case: 7
▶ Executing update: INSERT INTO sr_table_test VALUES (1, 'Alice'), (2, 'Bob');
Result: ✅ Success

Test Case: 8
▶ Executing query: SELECT * FROM sr_arrow_flight_sql.sr_table_test;
Result:
1	Alice	
2	Bob
```

### Java ADBC ドライバー

Arrow Flight SQL プロトコルは、標準 JDBC インターフェースと互換性のあるオープンソースの JDBC ドライバーを提供します。従来の JDBC ドライバーと同様に、さまざまな BI ツール（Tableau、Power BI、DBeaver など）に簡単に統合して StarRocks データベースにアクセスできます。このドライバーの大きな利点は、Apache Arrow に基づく高速データ転送をサポートしており、クエリとデータ転送の効率を大幅に向上させることです。使用方法は従来の MySQL JDBC ドライバーとほぼ同じです。

:::note

- コマンドラインを使用して FE サービスを起動する場合は、以下のいずれかの方法を使用できます：

  - 環境変数 `JAVA_TOOL_OPTIONS` を指定します。

    ```Bash
    export JAVA_TOOL_OPTIONS="--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED"
    ```

  - FE 設定項目 `JAVA_OPTS` を指定します（**fe.conf**）。これにより、他の `JAVA_OPTS` の値を追加できます。

    ```Bash
    JAVA_OPTS="--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED ..."
    ```

- IntelliJ IDEA でデバッグする場合は、`Run/Debug Configurations` の `Build and run` に以下のオプションを追加する必要があります：

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

コード例：

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

#### 推奨事項

- 上記の 3 つの Java Arrow Flight SQL 接続方法のうち：
  - 後続のデータ分析が行ベースのデータ形式に依存する場合は、JDBC ResultSet 形式でデータを返す `jdbc:arrow-flight-sql` の使用を推奨します。
  - 分析が Arrow 形式またはその他の列指向データ形式を直接処理できる場合は、Flight AdbcDriver または Flight JdbcDriver を使用できます。これらのオプションは Arrow 形式のデータを直接返し、行列変換を回避し、Arrow の機能を活用してデータ解析を高速化します。

- JDBC ResultSet または Arrow 形式のデータを解析するかどうかにかかわらず、解析時間はデータの読み取り自体にかかる時間よりも長くなることが多いです。Arrow Flight SQL が `jdbc:mysql://` に比べて期待どおりのパフォーマンス向上をもたらさない場合は、データ解析に時間がかかりすぎていないか調査することを検討してください。

- すべての接続方法において、JDK 17 でのデータ読み取りは一般的に JDK 1.8 よりも高速です。

- 大規模データセットを読み取る場合、Arrow Flight SQL は `jdbc:mysql://` と比較してメモリ消費量が少ない傾向があります。そのため、メモリの制約がある場合は、Arrow Flight SQL を試してみる価値があります。

- 上記の 3 つの接続方法に加えて、ネイティブの FlightClient を使用して Arrow Flight Server に接続することもでき、複数のエンドポイントからの柔軟な並列読み取りが可能になります。Java Flight AdbcDriver は FlightClient の上に構築されており、FlightClient を直接使用するよりもシンプルなインターフェースを提供します。

### Spark

現在、公式の Arrow Flight プロジェクトでは Spark や Flink のサポートは予定されていません。将来的には、[starrocks-spark-connector](https://github.com/qwshen/spark-flight-connector)が Arrow Flight SQL 経由で StarRocks にアクセスできるよう、サポートが段階的に追加される予定であり、読み取りパフォーマンスが数倍向上することが期待されています。

Spark で StarRocks にアクセスする場合、従来の JDBC や Java クライアント方式に加えて、オープンソースの Spark-Flight-Connector コンポーネントを使用して、Spark DataSource として StarRocks Flight SQL Server から直接読み書きすることもできます。Apache Arrow Flight プロトコルに基づくこのアプローチには、以下の重要な利点があります：

- **高性能データ転送**Spark-Flight-Connector は Apache Arrow をデータ転送形式として使用し、ゼロコピーで高効率なデータ交換を実現します。StarRocks の `internal Block` データ形式と Arrow 間の変換は非常に効率的で、従来の `CSV` や `JDBC` 方式と比較して最大 10 倍のパフォーマンス向上を達成し、データ転送のオーバーヘッドを大幅に削減します。
- **複雑なデータ型のネイティブサポート**Arrow データ形式は複雑な型（`Map`、`Array`、`Struct` など）をネイティブにサポートしており、従来の JDBC 方式と比較して StarRocks の複雑なデータモデルへの適応性が高く、データの表現力と互換性を向上させます。
- **読み取り、書き込み、ストリーミング書き込みのサポート**このコンポーネントは、Flight SQLクライアントとしてSparkをサポートし、`insert`、`merge`、`update`、`delete` DML文を含む効率的な読み取りおよび書き込み操作をサポートしており、ストリーミング書き込みもサポートしているため、リアルタイムデータ処理シナリオに適しています。
- **述語プッシュダウンと列プルーニングのサポート**データ読み取り時、Spark-Flight-Connectorは述語プッシュダウンと列プルーニングをサポートし、StarRocks側でのデータフィルタリングと列選択を可能にすることで、転送データ量を大幅に削減し、クエリパフォーマンスを向上させます。
- **集計プッシュダウンと並列読み取りのサポート**集計操作（`sum`、`count`、`max`、`min` など）をStarRocksにプッシュダウンして実行することができ、Sparkの計算負荷を軽減します。パーティショニングに基づく並列読み取りもサポートされており、大規模データシナリオでの読み取り効率が向上します。
- **ビッグデータシナリオに最適**従来のJDBCメソッドと比較して、Flight SQLプロトコルは大規模・高並列アクセスシナリオに適しており、StarRocksが高性能な分析能力を最大限に発揮できます。

## 付録

以下は使用チュートリアルの完全なコード例です。

```Python
# =============================================================================
# StarRocks Arrow Flight SQL テストスクリプト
# =============================================================================
# pip install adbc_driver_manager adbc_driver_flightsql pandas
# =============================================================================

# =============================================================================
# Arrow Flight SQL経由でStarRocksに接続するために必要なコアモジュール
# =============================================================================
import adbc_driver_manager
import adbc_driver_flightsql.dbapi as flight_sql

# =============================================================================
# 使いやすさとデバッグのためのオプションモジュール
# =============================================================================
import pandas as pd       # Optional: for better result display using DataFrame
import traceback          # Optional: for detailed error traceback during SQL execution
import time               # Optional: for measuring SQL execution time

# =============================================================================
# StarRocks Flight SQL 設定
# =============================================================================
FE_HOST = "127.0.0.1"
FE_PORT = 9408

# =============================================================================
# StarRocksに接続
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
# 出力フォーマットとSQL実行を改善するためのユーティリティ関数
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
# ステップ1: データベースの削除と作成
# =============================================================================
print_header("Step 1: Drop and Create Database")
execute("DROP DATABASE IF EXISTS sr_arrow_flight_sql FORCE;")
execute("SHOW DATABASES;")
execute("CREATE DATABASE sr_arrow_flight_sql;")
execute("SHOW DATABASES;")
execute("USE sr_arrow_flight_sql;")

# =============================================================================
# ステップ2: テーブルの作成
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
# ステップ3: データの挿入
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
# ステップ4: データのクエリ
# =============================================================================
print_header("Step 4: Query Data")
execute("SELECT * FROM sr_arrow_flight_sql_test ORDER BY k0;")

# =============================================================================
# ステップ5: セッション変数
# =============================================================================
print_header("Step 5: Session Variables")
execute("SHOW VARIABLES LIKE '%query_mem_limit%';")
execute("SET query_mem_limit = 2147483648;")
execute("SHOW VARIABLES LIKE '%query_mem_limit%';")

# =============================================================================
# ステップ6: 集計クエリ
# =============================================================================
print_header("Step 6: Aggregation Query")
execute("""
SELECT k5, SUM(k1) AS total_k1, COUNT(1) AS row_count, AVG(k3) AS avg_k3
FROM sr_arrow_flight_sql_test
GROUP BY k5
ORDER BY k5;
""")

# =============================================================================
# ステップ7: 接続を閉じる
# =============================================================================
print_header("Step 7: Close Connection")
cursor.close()
conn.close()
print("✅ Test completed successfully.")
```
