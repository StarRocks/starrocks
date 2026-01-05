# Apache Airflow

は、DAG (Directed Acyclic Graphs) と SQL オペレーターを使用して、StarRocks とのデータワークフローのオーケストレーションとスケジューリングを可能にします。`SQLExecuteQueryOperator` と `MySQLHook` を使用して、実装や複雑な設定なしでデータロードと変換を行うことができます。
[Apache Airflow GitHub リポジトリ](https://github.com/apache/airflow).

## サポートされている機能
- MySQL プロトコルを介した SQL 実行
- 接続管理
- トランザクションサポート
- パラメータ化されたクエリ
- タスク依存関係
- リトライロジック

## インストール
### 前提条件
- Apache Airflow 2.0+ または 3.0+
- Python 3.8+
- StarRocks クラスターへのアクセス ( [クイックスタートガイド](https://docs.starrocks.io/docs/quick_start/) を参照)
### インストール
StarRocks は MySQL プロトコルを使用するため、MySQL プロバイダーパッケージが必要です。

```sh
pip install apache-airflow-providers-mysql
```

インストールを確認するには、インストールされたプロバイダーを確認します。

```sh
airflow providers list
```

この出力には `apache-airflow-providers-mysql` が含まれているはずです。

## 設定
### StarRocks 接続の作成
Airflow UI または環境変数を介して StarRocks 接続を作成します。接続名は後で DAG によって使用されます。
#### Airflow UI を介して
1. Admin > Connections に移動
2. + ボタンをクリックして新しい接続を追加
3. 接続を設定:

  - Connection Id: `starrocks_default`
  - Connection Type: MySQL
  - Host: `your-starrocks-host.com`
  - Schema: `your_database`
  - Login: `your_username`
  - Password: `your_password`
  - Port: `9030`

#### Airflow CLI を介して

```sh
airflow connections add 'starrocks_default' \
    --conn-type 'mysql' \
    --conn-host 'your-starrocks-host.com' \
    --conn-schema 'your_database' \
    --conn-login 'your_username' \
    --conn-password 'your_password' \
    --conn-port 9030
```

## 使用例

これらの例は、StarRocks と Airflow を統合するための一般的なパターンを示しています。各例は、データロード、変換、ワークフローオーケストレーションの異なるアプローチを紹介しながら、基本的な概念を構築します。

**学べること:**
- **データロード**: CSV ファイルやクラウドストレージから StarRocks への効率的なデータロード
- **データ変換**: SQL クエリの実行と Python を使用した結果の処理
- **高度なパターン**: 増分ロード、非同期操作、クエリ最適化の実装
- **プロダクションのベストプラクティス**: エラーを優雅に処理し、堅牢なパイプラインを構築

すべての例は、 [クイックスタートガイド](../quick_start/shared-nothing.md) で説明されているクラッシュデータテーブルを使用します。

### データロード

#### ストリームデータロード

StarRocks Stream Load API を使用して大きな CSV ファイルを効率的にロードします。Stream Load は以下の用途に推奨されます:
- 高スループットのデータロード (並列ロードをサポート)
- 列変換とフィルタリングを伴うデータのロード

Stream Load は、大規模データセットに対して INSERT INTO VALUES ステートメントよりも優れたパフォーマンスを提供し、エラー耐性などの組み込み機能を含みます。なお、CSV ファイルは Airflow ワーカーのファイルシステムでアクセス可能である必要があります。

```py
from airflow.sdk import dag, task
from airflow.hooks.base import BaseHook
from datetime import datetime
import requests
from requests.auth import HTTPBasicAuth
from urllib.parse import urlparse


class PreserveAuthSession(requests.Session):
    """
    リダイレクトをまたいで Authorization ヘッダーを保持するカスタムセッション。
    StarRocks FE は Stream Load リクエストを BE ノードにリダイレクトすることがあります。
    """
    def rebuild_auth(self, prepared_request, response):
        old = urlparse(response.request.url)
        new = urlparse(prepared_request.url)
        
        # 同じホスト名にリダイレクトする場合のみ認証を保持
        if old.hostname == new.hostname:
            prepared_request.headers["Authorization"] = response.request.headers.get("Authorization")
@dag(
    dag_id="starrocks_stream_load_example",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["starrocks", "stream_load", "example"],
)
def starrocks_stream_load_example():
    @task
    def load_csv_to_starrocks():
        # 設定
        DATABASE = "quickstart"
        TABLE = "crashdata"
        CSV_PATH = "/path/to/crashdata.csv"
        
        conn = BaseHook.get_connection("starrocks_default")
        url = f"http://{conn.host}:{conn.port}/api/{DATABASE}/{TABLE}/_stream_load"
        
        # ユニークなラベルを生成
        from airflow.sdk import get_current_context
        context = get_current_context()
        execution_date = context['logical_date'].strftime('%Y%m%d_%H%M%S')
        label = f"{TABLE}_load_{execution_date}"
        
        headers = {
            "label": label,                    
            "column_separator": ",",            
            "skip_header": "1",                                    
            "max_filter_ratio": "0.1",          # 最大 10% のエラー率を許容
            "Expect": "100-continue",           
            "columns": """
                tmp_CRASH_DATE, tmp_CRASH_TIME, 
                CRASH_DATE=str_to_date(concat_ws(' ', tmp_CRASH_DATE, tmp_CRASH_TIME), '%m/%d/%Y %H:%i'),
                BOROUGH, ZIP_CODE, LATITUDE, LONGITUDE, LOCATION,
                ON_STREET_NAME, CROSS_STREET_NAME, OFF_STREET_NAME,
                NUMBER_OF_PERSONS_INJURED, NUMBER_OF_PERSONS_KILLED,
                NUMBER_OF_PEDESTRIANS_INJURED, NUMBER_OF_PEDESTRIANS_KILLED,
                NUMBER_OF_CYCLIST_INJURED, NUMBER_OF_CYCLIST_KILLED,
                NUMBER_OF_MOTORIST_INJURED, NUMBER_OF_MOTORIST_KILLED,
                CONTRIBUTING_FACTOR_VEHICLE_1, CONTRIBUTING_FACTOR_VEHICLE_2,
                CONTRIBUTING_FACTOR_VEHICLE_3, CONTRIBUTING_FACTOR_VEHICLE_4,
                CONTRIBUTING_FACTOR_VEHICLE_5, COLLISION_ID,
                VEHICLE_TYPE_CODE_1, VEHICLE_TYPE_CODE_2,
                VEHICLE_TYPE_CODE_3, VEHICLE_TYPE_CODE_4, VEHICLE_TYPE_CODE_5
            """.replace("\n", "").replace("  ", ""),
        }
        session = PreserveAuthSession()
        with open(CSV_PATH, "rb") as f:
            response = session.put(
                url,
                headers=headers,
                data=f,
                auth=HTTPBasicAuth(conn.login, conn.password or ""),
                timeout=3600,
            )
       
        result = response.json() 
        print(f"\nStream Load Response:")
        print(f"  Status: {result.get('Status')}")
        print(f"  Loaded Rows: {result.get('NumberLoadedRows', 0):,}")
        
        if result.get("Status") == "Success":
            return result
        else:
            error_msg = result.get("Message", "Unknown error")
            raise Exception(f"Stream Load failed: {error_msg}")
    load_csv_to_starrocks()

starrocks_stream_load_example()
```

#### ファイルからの挿入

StarRocks の [FILES()](https://docs.starrocks.io/docs/sql-reference/sql-functions/table-functions/files/) テーブル関数を使用して、ファイルから直接データをロードします。このアプローチは以下に最適です:
- S3、HDFS、Google Cloud Storage からのデータロード
- ロード中に変換を適用するワンステップのデータ取り込み
- 様々なファイルソースからのアドホックデータロード

`FILES()` は複数のファイル形式とストレージシステムをサポートしており、特定のユースケースに対する Stream Load の柔軟な代替手段となります。データは単一の SQL ステートメントで読み込まれ、挿入されます。

```py
from airflow.sdk import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime

FILE_PATH = "path_to_file_here"

@dag(
    dag_id='crashdata_dynamic_files_load',
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['starrocks', 'files', 'dynamic'],
)
def crashdata_files():
    @task
    def load_file(file_path: str):
        hook = MySqlHook(mysql_conn_id='starrocks_default')
        
        sql = f"""
            INSERT INTO crashdata (
                CRASH_DATE, BOROUGH, ZIP_CODE, LATITUDE, LONGITUDE,
                LOCATION, ON_STREET_NAME, CROSS_STREET_NAME, OFF_STREET_NAME,
                CONTRIBUTING_FACTOR_VEHICLE_1, CONTRIBUTING_FACTOR_VEHICLE_2,
                COLLISION_ID, VEHICLE_TYPE_CODE_1, VEHICLE_TYPE_CODE_2
            )
            SELECT 
                STR_TO_DATE(CONCAT_WS(' ', `CRASH DATE`, `CRASH TIME`), '%m/%d/%Y %H:%i'),
                BOROUGH,
                `ZIP CODE`,
                CAST(LATITUDE as INT),
                CAST(LONGITUDE as INT),
                LOCATION,
                `ON STREET NAME`,
                `CROSS STREET NAME`,
                `OFF STREET NAME`,
                `CONTRIBUTING FACTOR VEHICLE 1`,
                `CONTRIBUTING FACTOR VEHICLE 2`,
                CAST(`COLLISION_ID` as INT),
                `VEHICLE TYPE CODE 1`,
                `VEHICLE TYPE CODE 2`
            FROM FILES(
                "path" = "s3://{file_path}",
                "format" = "parquet",
                "aws.s3.access_key" = "XXXXXXXXXX",
                "aws.s3.secret_key" = "YYYYYYYYYY",
                "aws.s3.region" = "us-west-2"
            )
        """
        result = hook.run(sql)
        
        return file_path
    
    load_file(FILE_PATH)

crashdata_files()
```

### データ変換

StarRocks に対して SQL クエリを実行し、テーブル作成やデータ挿入を行います。これは以下に役立ちます:
- データベーススキーマの設定
- 小規模データセットのロード
- アドホッククエリの実行

```py
from airflow.sdk import dag, chain
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime

@dag(
    dag_id='crashdata_basic_setup',
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['starrocks', 'crashdata'],
)
def crashdata_basic_pipeline():
    """crashdata テーブルを作成し、サンプルの NYC クラッシュデータを挿入します。"""
    
    create_table = SQLExecuteQueryOperator(
        task_id='create_crashdata_table',
        conn_id='starrocks_default',
        sql="""
            CREATE TABLE IF NOT EXISTS crashdata (
                CRASH_DATE DATETIME,
                BOROUGH STRING,
                ZIP_CODE STRING,
                LATITUDE INT,
                LONGITUDE INT,
                LOCATION STRING,
                ON_STREET_NAME STRING,
                CROSS_STREET_NAME STRING,
                OFF_STREET_NAME STRING,
                CONTRIBUTING_FACTOR_VEHICLE_1 STRING,
                CONTRIBUTING_FACTOR_VEHICLE_2 STRING,
                COLLISION_ID INT,
                VEHICLE_TYPE_CODE_1 STRING,
                VEHICLE_TYPE_CODE_2 STRING
            )
            DUPLICATE KEY(CRASH_DATE)
            DISTRIBUTED BY HASH(COLLISION_ID) BUCKETS 10
            PROPERTIES (
                "replication_num" = "1"
            )
        """,
    )

    insert_data = SQLExecuteQueryOperator(
        task_id='insert_sample_data',
        conn_id='starrocks_default',
        sql="""
            INSERT INTO crashdata VALUES
                ('2024-01-15 08:30:00', 'MANHATTAN', '10001', 40748817, -73985428, 
                 '(40.748817, -73.985428)', '5 AVENUE', 'WEST 34 STREET', NULL,
                 'Driver Inattention/Distraction', 'Unspecified', 4567890, 'Sedan', 'Taxi'),
                ('2024-01-15 14:20:00', 'BROOKLYN', '11201', 40693139, -73987664,
                 '(40.693139, -73.987664)', 'FLATBUSH AVENUE', 'ATLANTIC AVENUE', NULL,
                 'Failure to Yield Right-of-Way', 'Unspecified', 4567891, 'SUV', 'Sedan'),
                ('2024-01-15 18:45:00', 'QUEENS', '11354', 40767689, -73827426,
                 '(40.767689, -73.827426)', 'NORTHERN BOULEVARD', 'MAIN STREET', NULL,
                 'Following Too Closely', 'Driver Inattention/Distraction', 4567892, 'Sedan', 'Sedan'),
                ('2024-01-16 09:15:00', 'BRONX', '10451', 40820679, -73925300,
                 '(40.820679, -73.925300)', 'GRAND CONCOURSE', 'EAST 161 STREET', NULL,
                 'Unsafe Speed', 'Unspecified', 4567893, 'Truck', 'Sedan')
        """,
    )

    create_table >> insert_data

crashdata_basic_pipeline()
```

#### MySqlHook を使用したより複雑な操作

MySqlHook を使用して、Python タスク内で高度なデータ分析と処理を行います。このアプローチは以下に役立ちます:
- 分析クエリを実行し、Python で結果を処理
- StarRocks クエリを Python ライブラリ (pandas, numpy など) と組み合わせる
- SQL と Python の両方を必要とする複雑なビジネスロジックの実装
- データ品質チェックと検証ワークフローの作成

MySqlHook はクエリ結果への完全なプログラムアクセスを提供し、DAG 内での高度なデータ変換と分析を可能にします。

```py
from airflow.sdk import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime

@dag(
    dag_id='crashdata_python_analysis',
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['starrocks', 'python', 'analytics'],
)
def crashdata_python_pipeline():
    @task
    def analyze_crash_hotspots():
        """バローとストリートごとのクラッシュホットスポットを特定します。"""
        hook = MySqlHook(mysql_conn_id='starrocks_default')
        
        # 高頻度クラッシュロケーションを見つけるためのクエリ
        sql = """
            SELECT 
                BOROUGH,
                ON_STREET_NAME,
                COUNT(*) as crash_count,
                COUNT(DISTINCT DATE(CRASH_DATE)) as days_with_crashes
            FROM crashdata
            WHERE ON_STREET_NAME IS NOT NULL
            GROUP BY BOROUGH, ON_STREET_NAME
            HAVING crash_count >= 3
            ORDER BY crash_count DESC
            LIMIT 10
        """
        
        results = hook.get_records(sql)
        
        print("トップ 10 クラッシュホットスポット:")
        for row in results:
            borough, street, count, days = row
            print(f"{borough:15} | {street:40} | {count:3} クラッシュ {days} 日間")
        
        return len(results)
    
    @task
    def calculate_contributing_factors():
        """寄与要因の割合分布を計算します。"""
        hook = MySqlHook(mysql_conn_id='starrocks_default')
        
        sql = """
            SELECT 
                CONTRIBUTING_FACTOR_VEHICLE_1 as factor,
                COUNT(*) as count,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
            FROM crashdata
            WHERE CONTRIBUTING_FACTOR_VEHICLE_1 != 'Unspecified'
            GROUP BY CONTRIBUTING_FACTOR_VEHICLE_1
            ORDER BY count DESC
        """
        
        results = hook.get_records(sql)
        
        print("\n寄与要因分析:")
        for factor, count, percentage in results:
            print(f"{factor:50} | {count:4} ({percentage}%)")
        
        return results
    
    # タスク実行順序を定義
    hotspots = analyze_crash_hotspots()
    factors = calculate_contributing_factors()
    
    hotspots >> factors

crashdata_python_pipeline()
```

### 高度なパターン

#### 増分データロード

既存のレコードを再処理せずにデータを増分ロードします。増分ロードは以下に不可欠です:

- 新しいデータのみでテーブルを効率的に更新
- 処理時間とリソース使用量の削減
- 時間とともに増加する大規模データセットの管理
- フルリロードなしでデータの新鮮さを維持

このパターンは、ステージングテーブルとタイムスタンプベースのフィルタリングを使用して、新しいレコードのみがロードされるようにし、スケジュールされたバッチ更新に最適です。

```py
from airflow.sdk import dag, chain
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime

@dag(
    dag_id='crashdata_incremental_load',
    schedule='@hourly',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['starrocks', 'incremental'],
)
def crashdata_incremental_pipeline():
    """ステージングから新しいクラッシュレポートを増分ロードします。"""
    
    create_staging = SQLExecuteQueryOperator(
        task_id='create_staging_table',
        conn_id='starrocks_default',
        sql="""
            CREATE TABLE IF NOT EXISTS crashdata_staging (
                CRASH_DATE DATETIME,
                BOROUGH STRING,
                ZIP_CODE STRING,
                LATITUDE INT,
                LONGITUDE INT,
                LOCATION STRING,
                ON_STREET_NAME STRING,
                CROSS_STREET_NAME STRING,
                OFF_STREET_NAME STRING,
                CONTRIBUTING_FACTOR_VEHICLE_1 STRING,
                CONTRIBUTING_FACTOR_VEHICLE_2 STRING,
                COLLISION_ID INT,
                VEHICLE_TYPE_CODE_1 STRING,
                VEHICLE_TYPE_CODE_2 STRING,
                loaded_at DATETIME
            )
            DUPLICATE KEY(CRASH_DATE)
            DISTRIBUTED BY HASH(COLLISION_ID) BUCKETS 10
            PROPERTIES ("replication_num" = "1")
        """,
    )
    
    incremental_load = SQLExecuteQueryOperator(
        task_id='load_new_crashes',
        conn_id='starrocks_default',
        sql="""
            INSERT INTO crashdata
            SELECT
                CRASH_DATE,
                BOROUGH,
                ZIP_CODE,
                LATITUDE,
                LONGITUDE,
                LOCATION,
                ON_STREET_NAME,
                CROSS_STREET_NAME,
                OFF_STREET_NAME,
                CONTRIBUTING_FACTOR_VEHICLE_1,
                CONTRIBUTING_FACTOR_VEHICLE_2,
                COLLISION_ID,
                VEHICLE_TYPE_CODE_1,
                VEHICLE_TYPE_CODE_2
            FROM crashdata_staging
            WHERE loaded_at >= '{{ data_interval_start }}'
              AND loaded_at < '{{ data_interval_end }}'
              AND COLLISION_ID NOT IN (SELECT COLLISION_ID FROM crashdata)
        """,
    )

    create_staging >> incremental_load

crashdata_incremental_pipeline()
```

#### `SUBMIT TASK` を使用した非同期大規模ジョブ

Airflow タスクをブロックしない長時間実行クエリには `SUBMIT TASK` を使用します。このパターンは以下に有益です:
- 数分または数時間かかる複雑な分析クエリ
- 大規模データ変換 (テーブルコピー、集計)
- 同期モードでタイムアウトする可能性のあるリソース集約型操作
- 複数の重いクエリの並列実行
- ジョブの送信と完了の監視を分離

`SUBMIT TASK` は、データベース接続を開いたままにせずに長時間実行される StarRocks 操作を監視するため、リソース効率と信頼性を向上させます。

```py
from airflow.sdk import dag, chain, task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime
import time

@dag(
    dag_id='crashdata_submit_task',
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['starrocks', 'submit-task'],
)
def crashdata_submit_task_pipeline():
    """
    長時間実行クエリのための SUBMIT TASK の使用例。
    SUBMIT TASK サポートには StarRocks 3.4+ が必要です。
    """
    
    @task
    def submit_long_running_query():
        """非同期タスクとして長時間実行クエリを送信します。"""
        hook = MySqlHook(mysql_conn_id='starrocks_default')
        
        submit_sql = """
            SUBMIT TASK backup_crashdata AS 
            CREATE TABLE crash_data_backup AS
            SELECT * FROM crashdata
        """
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(submit_sql)
        
        # 結果からタスク名を取得
        result = cursor.fetchone()
        task_name = result[0] if result else None
        
        cursor.close()
        conn.close()
        
        if task_name:
            print(f"タスクが正常に送信されました: {task_name}")
            return task_name
        else:
            raise Exception("タスクの送信に失敗しました")
    
    @task
    def monitor_task_completion(task_name: str):
        """送信されたタスクを完了まで監視します。"""
        hook = MySqlHook(mysql_conn_id='starrocks_default')
        
        max_wait_time = 600  # 10 分
        poll_interval = 10   # 10 秒ごとにチェック
        elapsed_time = 0
        
        while elapsed_time < max_wait_time:
            conn = hook.get_conn()
            cursor = conn.cursor()
            
            # information_schema でタスクの状態を確認
            check_sql = f"""
                SELECT STATE, ERROR_MESSAGE
                FROM information_schema.task_runs 
                WHERE TASK_NAME = '{task_name}'
            """
            cursor.execute(check_sql)
            result = cursor.fetchone()
            
            if result:
                state, error_msg = result
                print(f"[{elapsed_time}s] タスクの状態: {state}")
                
                if state == 'SUCCESS':
                    cursor.close()
                    conn.close()
                    return {'status': 'SUCCESS', 'task_name': task_name}
                elif state == 'FAILED':
                    cursor.close()
                    conn.close()
                    raise Exception(f"タスクが失敗しました: {error_msg}")
            
            cursor.close()
            conn.close()
            
            time.sleep(poll_interval)
            elapsed_time += poll_interval
        
        raise Exception(f"タスクが {max_wait_time} 秒以内に完了しませんでした")
    
    @task
    def process_results():
        """完了したタスクの結果を処理または検証します。"""
        print("タスクが正常に完了しました - 結果が利用可能です")
        return "処理完了"
    
    # タスクフローを定義
    task_name = submit_long_running_query()
    monitor_result = monitor_task_completion(task_name)
    result = process_results()
    
    chain(task_name, monitor_result, result)

crashdata_submit_task_pipeline()
```

タスク名は StarRocks でユニークであるため、将来の実行には修飾子 (uuid など) が必要になる場合があります。

#### マテリアライズドビュー

クエリパフォーマンスを加速するためにマテリアライズドビューを作成および管理します。マテリアライズドビューは以下に最適です:
- ダッシュボード用の複雑な集計を事前計算
- 頻繁に実行される分析クエリの加速
- 自動更新されるサマリーテーブルの維持
- 繰り返し計算を回避することで計算コストを削減
- 事前集計データからリアルタイム分析を提供

StarRocks のマテリアライズドビューは自動またはオンデマンドでリフレッシュされ、集計データを新鮮に保ちながらクエリパフォーマンスを劇的に向上させます。

```py
from airflow.sdk import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime, timedelta


@dag(
    dag_id="starrocks_materialized_view_example",
    schedule="0 2 * * *",  # 毎日午前2時に実行
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["starrocks", "materialized_view", "example"],
    doc_md=__doc__,
)
def starrocks_materialized_view_example():
    @task
    def create_materialized_view():
        hook = MySqlHook(mysql_conn_id="starrocks_conn")
        
        drop_sql = """
        DROP MATERIALIZED VIEW IF EXISTS quickstart.mv_daily_crash_stats
        """
        
        create_sql = """
        CREATE MATERIALIZED VIEW IF NOT EXISTS quickstart.mv_daily_crash_stats
        DISTRIBUTED BY HASH(`crash_date`)
        REFRESH ASYNC
        AS
        SELECT 
            DATE(CRASH_DATE) as crash_date,
            BOROUGH,
            COUNT(*) as total_crashes,
            COUNT(DISTINCT COLLISION_ID) as unique_collisions
        FROM quickstart.crashdata
        WHERE CRASH_DATE IS NOT NULL
        GROUP BY DATE(CRASH_DATE), BOROUGH
        """
        
        hook.run(drop_sql)
        hook.run(create_sql)
        
        return "mv_daily_crash_stats"
    
    @task
    def refresh_materialized_view(mv_name: str):
        hook = MySqlHook(mysql_conn_id="starrocks_conn")
        refresh_sql = f"REFRESH MATERIALIZED VIEW quickstart.{mv_name}"
        hook.run(refresh_sql)
        return mv_name
    
    @task
    def check_materialized_view_status(mv_name: str):
        hook = MySqlHook(mysql_conn_id="starrocks_conn")
        
        # MV のタスク名を取得
        task_query = f"""
        SELECT TASK_NAME
        FROM information_schema.tasks 
        WHERE `DATABASE` = 'quickstart'
        AND DEFINITION LIKE '%{mv_name}%'
        ORDER BY CREATE_TIME DESC
        LIMIT 1
        """
        
        task_name = hook.get_first(task_query)[0]
        
        # 最新のタスク実行状態を取得
        state_query = f"""
        SELECT STATE
        FROM information_schema.task_runs
        WHERE TASK_NAME = '{task_name}'
        ORDER BY CREATE_TIME DESC
        LIMIT 1
        """
        
        state = hook.get_first(state_query)[0]
        
        print(f"MV: {mv_name} | タスク: {task_name} | 状態: {state}")
        
        if state not in ('SUCCESS', 'RUNNING'):
            raise Exception(f"マテリアライズドビューのリフレッシュが予期しない状態です: {state}")
        
        return {'task_name': task_name, 'state': state}

    
    create = create_materialized_view()
    refresh = refresh_materialized_view(create)
    status = check_materialized_view_status(refresh)

    status 
    
starrocks_materialized_view_example()
```

#### エラーハンドリング

プロダクションの信頼性のために堅牢なエラーハンドリングを実装します。適切なエラーハンドリングは以下に重要です:

- 一時的な障害 (ネットワーク問題、タイムアウト) から自動的に回復
- 一時的な問題によるデータパイプラインの中断を防止
- 失敗パターンへの可視性を提供

Airflow の組み込みリトライメカニズムは、ほとんどの一時的なエラーを処理します。

```py
from airflow.sdk import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta

@dag(
    dag_id='starrocks_with_retries',
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
        'retry_exponential_backoff': True,
        'max_retry_delay': timedelta(hours=1),
    },
)
def starrocks_resilient_pipeline():
    critical_query = SQLExecuteQueryOperator(
        task_id='critical_query',
        conn_id='starrocks_default',
        sql='SELECT * FROM important_table',
    )
starrocks_resilient_pipeline()
```

### トラブルシューティング

- Airflow インスタンス内からポート 9030 がアクセス可能であることを確認
- Airflow UI から接続をテスト (有効な場合)
- localhost を使用する場合は、127.0.0.1 を使用
