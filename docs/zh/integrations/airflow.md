# Airflow

Apache Airflow。Airflow 通过使用 DAG（有向无环图）和 SQL 操作符来实现与 StarRocks 的数据工作流编排和调度。使用 Airflow 进行数据导入和转换时，可以使用 `SQLExecuteQueryOperator` 和 `MySQLHook`，无需任何实现或复杂配置。
[Apache Airflow GitHub 仓库](https://github.com/apache/airflow)。

## 支持的功能
- 通过 MySQL 协议执行 SQL
- 连接管理
- 事务支持
- 参数化查询
- 任务依赖
- 重试逻辑

## 安装
### 前提条件
- Apache Airflow 2.0+ 或 3.0+
- Python 3.8+
- 访问 StarRocks 集群（请参阅 [快速入门指南](https://docs.starrocks.io/docs/quick_start/)）
### 安装
要使用 StarRocks，需要安装 MySQL 提供程序包，因为 StarRocks 使用 MySQL 协议。

```sh
pip install apache-airflow-providers-mysql
```

通过检查已安装的提供程序来验证安装：

```sh
airflow providers list
```

输出中应列出 `apache-airflow-providers-mysql`。

## 配置
### 创建 StarRocks 连接
在 Airflow UI 或通过环境变量创建一个 StarRocks 连接。连接的名称将在 DAG 中使用。
#### 通过 Airflow UI
1. 导航到 Admin > Connections
2. 点击 + 按钮添加新连接
3. 配置连接：

  - Connection Id: `starrocks_default`
  - Connection Type: MySQL
  - Host: `your-starrocks-host.com`
  - Schema: `your_database`
  - Login: `your_username`
  - Password: `your_password`
  - Port: `9030`

#### 通过 Airflow CLI

```sh
airflow connections add 'starrocks_default' \
    --conn-type 'mysql' \
    --conn-host 'your-starrocks-host.com' \
    --conn-schema 'your_database' \
    --conn-login 'your_username' \
    --conn-password 'your_password' \
    --conn-port 9030
```

## 使用示例

这些示例展示了将 StarRocks 与 Airflow 集成的常见模式。每个示例都基于核心概念，同时展示了不同的数据导入、转换和工作流编排方法。

**您将学习到：**
- **数据导入**：高效地从 CSV 文件和云存储中导入数据到 StarRocks
- **数据转换**：执行 SQL 查询并使用 Python 处理结果
- **高级模式**：实现增量导入、异步操作和查询优化
- **生产最佳实践**：优雅地处理错误并构建可靠的管道

所有示例都使用 [快速入门指南](../quick_start/shared-nothing.md) 中描述的崩溃数据表。

### 数据导入

#### 流式数据导入

使用 StarRocks Stream Load API 高效导入大型 CSV 文件。Stream Load 是推荐的方法：
- 高吞吐量数据导入（支持并行导入）
- 导入数据时进行列转换和过滤

对于大型数据集，Stream Load 比 INSERT INTO VALUES 语句提供更好的性能，并包含内置的错误容忍功能。注意，这需要 CSV 文件可以在 Airflow 工作器的文件系统上访问。

```py
from airflow.sdk import dag, task
from airflow.hooks.base import BaseHook
from datetime import datetime
import requests
from requests.auth import HTTPBasicAuth
from urllib.parse import urlparse


class PreserveAuthSession(requests.Session):
    """
    自定义会话，保留重定向时的授权头。
    StarRocks FE 可能会将 Stream Load 请求重定向到 BE 节点。
    """
    def rebuild_auth(self, prepared_request, response):
        old = urlparse(response.request.url)
        new = urlparse(prepared_request.url)
        
        # 仅在重定向到相同主机名时保留授权
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
        # 配置
        DATABASE = "quickstart"
        TABLE = "crashdata"
        CSV_PATH = "/path/to/crashdata.csv"
        
        conn = BaseHook.get_connection("starrocks_default")
        url = f"http://{conn.host}:{conn.port}/api/{DATABASE}/{TABLE}/_stream_load"
        
        # 生成唯一标签
        from airflow.sdk import get_current_context
        context = get_current_context()
        execution_date = context['logical_date'].strftime('%Y%m%d_%H%M%S')
        label = f"{TABLE}_load_{execution_date}"
        
        headers = {
            "label": label,                    
            "column_separator": ",",            
            "skip_header": "1",                                    
            "max_filter_ratio": "0.1",          # 允许最多 10% 的错误率
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

#### 从文件插入

使用 StarRocks 的 [FILES()](https://docs.starrocks.io/docs/sql-reference/sql-functions/table-functions/files/) 表函数直接从文件加载数据。这种方法适合：
- 从 S3、HDFS、Google Cloud Storage 加载数据
- 在导入时应用转换的一步数据摄取
- 从各种文件源进行临时数据加载

`FILES()` 支持多种文件格式和存储系统，是 Stream Load 的灵活替代方案，适用于某些用例。数据在单个 SQL 语句中读取并插入。

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

### 数据转换

针对 StarRocks 执行 SQL 查询以创建表和插入数据。这对于以下情况很有用：
- 设置数据库模式
- 加载小型数据集
- 运行临时查询

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
    """创建 crashdata 表并插入示例 NYC 崩溃数据。"""
    
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

#### 使用 MySqlHook 进行更复杂的操作

使用 MySqlHook 在 Python 任务中进行高级数据分析和处理。这种方法适用于：
- 运行分析查询并在 Python 中处理结果
- 将 StarRocks 查询与 Python 库（pandas、numpy 等）结合使用
- 实现需要 SQL 和 Python 的复杂业务逻辑
- 创建数据质量检查和验证工作流

MySqlHook 提供对查询结果的全程编程访问，使您能够在 DAG 中进行复杂的数据转换和分析。

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
        """按区和街道识别崩溃热点。"""
        hook = MySqlHook(mysql_conn_id='starrocks_default')
        
        # 查询高频崩溃位置
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
        
        print("Top 10 Crash Hotspots:")
        for row in results:
            borough, street, count, days = row
            print(f"{borough:15} | {street:40} | {count:3} crashes over {days} days")
        
        return len(results)
    
    @task
    def calculate_contributing_factors():
        """计算影响因素的百分比分布。"""
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
        
        print("\nContributing Factors Analysis:")
        for factor, count, percentage in results:
            print(f"{factor:50} | {count:4} ({percentage}%)")
        
        return results
    
    # 定义任务执行顺序
    hotspots = analyze_crash_hotspots()
    factors = calculate_contributing_factors()
    
    hotspots >> factors

crashdata_python_pipeline()
```

### 高级模式

#### 增量数据导入

增量导入数据以避免重新处理现有记录。增量导入对于以下情况至关重要：

- 仅使用新数据高效更新表
- 减少处理时间和资源使用
- 管理随时间增长的大型数据集
- 在不完全重新加载的情况下保持数据新鲜

这种模式使用临时表和基于时间戳的过滤来确保仅加载新记录，使其成为计划批量更新的理想选择。

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
    """增量加载来自临时表的新崩溃报告。"""
    
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

#### 使用 `SUBMIT TASK` 的异步大规模作业

使用 `SUBMIT TASK` 处理不应阻塞 Airflow 任务的长时间运行查询。这种模式有利于：
- 需要几分钟或几小时的复杂分析查询
- 大规模数据转换（表复制、聚合）
- 可能在同步模式下超时的资源密集型操作
- 并行执行多个繁重查询
- 将作业提交与完成监控分离

`SUBMIT TASK` 允许 Airflow 监控长时间运行的 StarRocks 操作，而无需保持数据库连接打开，提高了资源效率和可靠性。

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
    使用 SUBMIT TASK 进行长时间运行查询的示例。
    需要 StarRocks 3.4+ 支持 SUBMIT TASK。
    """
    
    @task
    def submit_long_running_query():
        """将长时间运行的查询提交为异步任务。"""
        hook = MySqlHook(mysql_conn_id='starrocks_default')
        
        submit_sql = """
            SUBMIT TASK backup_crashdata AS 
            CREATE TABLE crash_data_backup AS
            SELECT * FROM crashdata
        """
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(submit_sql)
        
        # 从结果中获取任务名称
        result = cursor.fetchone()
        task_name = result[0] if result else None
        
        cursor.close()
        conn.close()
        
        if task_name:
            print(f"Task submitted successfully: {task_name}")
            return task_name
        else:
            raise Exception("Failed to submit task")
    
    @task
    def monitor_task_completion(task_name: str):
        """监控提交的任务直到完成。"""
        hook = MySqlHook(mysql_conn_id='starrocks_default')
        
        max_wait_time = 600  # 10 分钟
        poll_interval = 10   # 每 10 秒检查一次
        elapsed_time = 0
        
        while elapsed_time < max_wait_time:
            conn = hook.get_conn()
            cursor = conn.cursor()
            
            # 在 information_schema 中检查任务状态
            check_sql = f"""
                SELECT STATE, ERROR_MESSAGE
                FROM information_schema.task_runs 
                WHERE TASK_NAME = '{task_name}'
            """
            cursor.execute(check_sql)
            result = cursor.fetchone()
            
            if result:
                state, error_msg = result
                print(f"[{elapsed_time}s] Task status: {state}")
                
                if state == 'SUCCESS':
                    cursor.close()
                    conn.close()
                    return {'status': 'SUCCESS', 'task_name': task_name}
                elif state == 'FAILED':
                    cursor.close()
                    conn.close()
                    raise Exception(f"Task failed: {error_msg}")
            
            cursor.close()
            conn.close()
            
            time.sleep(poll_interval)
            elapsed_time += poll_interval
        
        raise Exception(f"Task did not complete within {max_wait_time} seconds")
    
    @task
    def process_results():
        """处理或验证已完成的任务结果。"""
        print("Task completed successfully - results are now available")
        return "Processing complete"
    
    # 定义任务流
    task_name = submit_long_running_query()
    monitor_result = monitor_task_completion(task_name)
    result = process_results()
    
    chain(task_name, monitor_result, result)

crashdata_submit_task_pipeline()
```

注意，任务名称在 StarRocks 中是唯一的，因此将来的运行可能需要一个限定符（例如 uuid）。

#### 物化视图

创建和管理物化视图以加速查询性能。物化视图非常适合：
- 为仪表板预计算复杂的聚合
- 加速频繁运行的分析查询
- 维护自动更新的汇总表
- 通过避免重复计算来降低计算成本
- 从预聚合数据中提供实时分析

StarRocks 中的物化视图可以自动或按需刷新，保持聚合数据的新鲜，同时显著提高查询性能。

```py
from airflow.sdk import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime, timedelta


@dag(
    dag_id="starrocks_materialized_view_example",
    schedule="0 2 * * *",  # 每天凌晨 2 点运行
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
        
        # 获取 MV 的任务名称
        task_query = f"""
        SELECT TASK_NAME
        FROM information_schema.tasks 
        WHERE `DATABASE` = 'quickstart'
        AND DEFINITION LIKE '%{mv_name}%'
        ORDER BY CREATE_TIME DESC
        LIMIT 1
        """
        
        task_name = hook.get_first(task_query)[0]
        
        # 获取最新任务运行状态
        state_query = f"""
        SELECT STATE
        FROM information_schema.task_runs
        WHERE TASK_NAME = '{task_name}'
        ORDER BY CREATE_TIME DESC
        LIMIT 1
        """
        
        state = hook.get_first(state_query)[0]
        
        print(f"MV: {mv_name} | Task: {task_name} | State: {state}")
        
        if state not in ('SUCCESS', 'RUNNING'):
            raise Exception(f"Materialized view refresh in unexpected state: {state}")
        
        return {'task_name': task_name, 'state': state}

    
    create = create_materialized_view()
    refresh = refresh_materialized_view(create)
    status = check_materialized_view_status(refresh)

    status 
    
starrocks_materialized_view_example()
```

#### 错误处理

实现健壮的错误处理以确保生产可靠性。适当的错误处理对于以下情况至关重要：

- 自动从瞬时故障（网络问题、超时）中恢复
- 防止数据管道因临时问题中断
- 提供对故障模式的可见性

Airflow 的内置重试机制可以处理大多数瞬时错误。

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

### 故障排除

- 确保端口 9030 可以从 Airflow 实例中访问
- 从 Airflow UI 测试连接（如果启用）
- 如果使用 localhost，请使用 127.0.0.1 代替