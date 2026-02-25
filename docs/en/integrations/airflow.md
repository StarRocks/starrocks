# Apache Airflow

Enables orchestration and scheduling of data workflows with StarRocks using DAGs (Directed Acyclic Graphs) and SQL operators. Use Airflow for data loading and transformation using the `SQLExecuteQueryOperator` and `MySQLHook` without any implementation or complex configuration. 
[Apache Airflow GitHub repo](https://github.com/apache/airflow).

## Supported features 
- SQL Execution through MySQL protocol
- Connection management
- Transaction support
- Parameterized queries
- Task dependencies
- Retry logic

## Installation
### Prerequisites
- Apache Airflow 2.0+ or 3.0+
- Python 3.8+
- Access to a StarRocks cluster (see the [quickstart guide](https://docs.starrocks.io/docs/quick_start/))
### Install 
The MySQL provider package is required to use StarRocks as StarRocks uses MySQL protocol. 

```sh
pip install apache-airflow-providers-mysql
```

Verify the installation by checking the installed providers:

```sh
airflow providers list
```

This should list `apache-airflow-providers-mysql` in the output. 

## Configuration
### Create a StarRocks Connection
Create a StarRocks connection in the Airflow UI or via environment variable. The name of the connection will be used by the DAGs later. 
#### Via Airflow UI
1. Navigate to Admin > Connections
2. Click the + button to add a new connection
3. Configure the connection: 

  - Connection Id: `starrocks_default`
  - Connection Type: MySQL
  - Host: `your-starrocks-host.com`
  - Schema: `your_database`
  - Login: `your_username`
  - Password: `your_password`
  - Port: `9030`

#### Via Airflow CLI

```sh
airflow connections add 'starrocks_default' \
    --conn-type 'mysql' \
    --conn-host 'your-starrocks-host.com' \
    --conn-schema 'your_database' \
    --conn-login 'your_username' \
    --conn-password 'your_password' \
    --conn-port 9030
```

## Usage Examples

These examples demonstrate common patterns for integrating StarRocks with Airflow. Each example builds on core concepts while showcasing different approaches to data loading, transformation, and workflow orchestration.

**What You'll Learn:**
- **Data Loading**: Efficiently load data from CSV files and cloud storage into StarRocks
- **Data Transformation**: Execute SQL queries and process results with Python
- **Advanced Patterns**: Implement incremental loading, async operations, and query optimization
- **Production Best Practices**: Handle errors gracefully and build resilient pipelines

All examples use the crash data tables described in the [quickstart guide](../quick_start/shared-nothing.md).

### Data Loading

#### Stream Data Loading 

Load large CSV files efficiently using StarRocks Stream Load API. Stream Load is the recommended approach for: 
- High-throughput data loading (supports parallel loads)
- Loading data with column transformations and filtering

Stream Load provides better performance than INSERT INTO VALUES statements for large datasets and includes built-in features like error tolerance. Note that this does require the CSV file is accessible on the Airflow worker's filesystem. 

```py
from airflow.sdk import dag, task
from airflow.hooks.base import BaseHook
from datetime import datetime
import requests
from requests.auth import HTTPBasicAuth
from urllib.parse import urlparse


class PreserveAuthSession(requests.Session):
    """
    Custom session that preserves Authorization header across redirects.
    StarRocks FE may redirect Stream Load requests to BE nodes.
    """
    def rebuild_auth(self, prepared_request, response):
        old = urlparse(response.request.url)
        new = urlparse(prepared_request.url)
        
        # Only preserve auth when redirecting to same hostname
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
        # Configuration
        DATABASE = "quickstart"
        TABLE = "crashdata"
        CSV_PATH = "/path/to/crashdata.csv"
        
        conn = BaseHook.get_connection("starrocks_default")
        url = f"http://{conn.host}:{conn.port}/api/{DATABASE}/{TABLE}/_stream_load"
        
        # Generate unique label
        from airflow.sdk import get_current_context
        context = get_current_context()
        execution_date = context['logical_date'].strftime('%Y%m%d_%H%M%S')
        label = f"{TABLE}_load_{execution_date}"
        
        headers = {
            "label": label,                    
            "column_separator": ",",            
            "skip_header": "1",                                    
            "max_filter_ratio": "0.1",          # Allow up to 10% error rate
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

#### Insert From Files

Use StarRocks' [FILES()](https://docs.starrocks.io/docs/sql-reference/sql-functions/table-functions/files/) table function to load data directly from files. This approach is ideal for: 
- Loading data from S3, HDFS, Google Cloud Storage
- One-step data ingestion with transformations applied during load
- Ad-hoc data loads from various file sources

`FILES()` supports multiple file formats and storage systems, making it a flexible alternative to Stream Load for certain use cases. The data is read and inserted in a single SQL statement.

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

### Data Transformation

Execute SQL queries against StarRocks for table creation and data insertion. This is useful for: 
- Setting up database schema 
- Loading small datasets
- Running ad-hoc queries

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
    """Create crashdata table and insert sample NYC crash data."""
    
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

#### More complex operations with MySqlHook 

Use MySqlHook for advanced data analysis and processing within Python tasks. This approach is useful for: 
- Running analytical queries and processing results in Python
- Combining StarRocks queries with Python libraries (pandas, numpy, etc.)
- Implementing complex business logic that requires both SQL and Python
- Creating data quality checks and validation workflows  

MySqlHook provides full programmatic access to query results, enabling sophisticated data transformations and analysis within your DAG.

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
        """Identify crash hotspots by borough and street."""
        hook = MySqlHook(mysql_conn_id='starrocks_default')
        
        # Query to find high-frequency crash locations
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
        """Calculate percentage distribution of contributing factors."""
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
    
    # Define task execution order
    hotspots = analyze_crash_hotspots()
    factors = calculate_contributing_factors()
    
    hotspots >> factors

crashdata_python_pipeline()
```

### Advanced Patterns

#### Incremental Data Loading

Load data incrementally to avoid reprocessing existing records. Incremental loading is essential for:

- Efficiently updating tables with new data only
- Reducing processing time and resource usage
- Managing large datasets that grow over time
- Maintaining data freshness without full reloads  

This pattern uses staging tables and timestamp-based filtering to ensure only new records are loaded, making it ideal for scheduled batch updates.

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
    """Incrementally load new crash reports from staging."""
    
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

#### Asynchronous large-scale jobs with `SUBMIT TASK`

Use `SUBMIT TASK` for long-running queries that shouldn't block the Airflow task. This pattern is beneficial for: 
- Complex analytical queries that take minutes or hours
- Large-scale data transformations (table copies, aggregations)
- Resource-intensive operations that might timeout in synchronous mode
- Parallel execution of multiple heavy queries
- Separating job submission from completion monitoring  

`SUBMIT TASK` allows Airflow to monitor long-running StarRocks operations without holding database connections open, improving resource efficiency and reliability.

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
    Example of using SUBMIT TASK for long-running queries.
    Requires StarRocks 3.4+ for SUBMIT TASK support.
    """
    
    @task
    def submit_long_running_query():
        """Submit a long-running query as an async task."""
        hook = MySqlHook(mysql_conn_id='starrocks_default')
        
        submit_sql = """
            SUBMIT TASK backup_crashdata AS 
            CREATE TABLE crash_data_backup AS
            SELECT * FROM crashdata
        """
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(submit_sql)
        
        # Get the task name from result
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
        """Monitor the submitted task until completion."""
        hook = MySqlHook(mysql_conn_id='starrocks_default')
        
        max_wait_time = 600  # 10 minutes
        poll_interval = 10   # Check every 10 seconds
        elapsed_time = 0
        
        while elapsed_time < max_wait_time:
            conn = hook.get_conn()
            cursor = conn.cursor()
            
            # Check task status in information_schema
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
        """Process or verify the completed task results."""
        print("Task completed successfully - results are now available")
        return "Processing complete"
    
    # Define task flow
    task_name = submit_long_running_query()
    monitor_result = monitor_task_completion(task_name)
    result = process_results()
    
    chain(task_name, monitor_result, result)

crashdata_submit_task_pipeline()
```

Note that the task name is unique in StarRocks, so future runs may need a qualifier (such as uuid).

#### Materialized Views

Create and manage materialized views for accelerated query performance. Materialized views are ideal for: 
- Pre-computing complex aggregations for dashboards
- Accelerating frequently run analytical queries 
- Maintaining summary tables that update automatically 
- Reducing compute costs by avoiding repeated calculations 
- Serving real-time analytics from pre-aggregated data  

Materialized views in StarRocks refresh automatically or on-demand, keeping aggregated data fresh while dramatically improving query performance.

```py
from airflow.sdk import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime, timedelta


@dag(
    dag_id="starrocks_materialized_view_example",
    schedule="0 2 * * *",  # Run daily at 2 AM
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
        
        # Get task name for the MV
        task_query = f"""
        SELECT TASK_NAME
        FROM information_schema.tasks 
        WHERE `DATABASE` = 'quickstart'
        AND DEFINITION LIKE '%{mv_name}%'
        ORDER BY CREATE_TIME DESC
        LIMIT 1
        """
        
        task_name = hook.get_first(task_query)[0]
        
        # Get latest task run state
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

#### Error Handling

Implement robust error handling for production reliability. Proper error handling is critical for:

- Automatically recovering from transient failures (network issues, timeouts) 
- Preventing data pipeline disruptions from temporary problems
- Providing visibility into failure patterns  

Airflow's built-in retry mechanisms handle most transient errors.

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

### Troubleshooting

- Verify that port 9030 is accessible from within the Airflow instance
- Test the connection (if enabled) from the Airflow UI
- If using localhost, use 127.0.0.1 instead
