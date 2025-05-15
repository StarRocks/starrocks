#!/usr/bin/env python3
# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import mysql.connector
import os
import csv
from datetime import datetime
import tarfile
import shutil

def get_connection():
    host = input("Enter StarRocks host (default: 127.0.0.1): ") or "127.0.0.1"
    port = input("Enter StarRocks port (default: 9030): ") or "9030"
    user = input("Enter StarRocks username (default: root): ") or "root"
    password = input("Enter StarRocks password: ")

    try:
        conn = mysql.connector.connect(
            host=host, port=int(port), user=user, password=password
        )
        print("✅ Connected to StarRocks")
        return conn
    except mysql.connector.Error as e:
        print(f"❌ Connection error: {e}")
        exit(1)


def log_warning(perf_dir, message, count=None):
    summary_file = os.path.join(perf_dir, "summary.txt")
    with open(summary_file, "a") as f:
        if count is not None:
            f.write(f"{message} (Found {count} entries)\n")
        else:
            f.write(f"{message}\n")
    print(message)  # Keep the console output for real-time feedback

def save_query_to_csv(cursor, query, output_file, perf_dir, message):
    try:
        cursor.execute(query)
        results = cursor.fetchall()
        if results:
            with open(output_file, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow([i[0] for i in cursor.description])  # Header
                writer.writerows(results)
            # Use the enhanced log_warning to include count
            log_warning(perf_dir, message, count=len(results))
    except mysql.connector.Error as e:
        log_warning(perf_dir, f"❌ Error: {e}")

def get_catalogs(conn):
    cursor = conn.cursor()
    try:
        cursor.execute("SHOW CATALOGS;")
        catalogs = [row[0] for row in cursor.fetchall()]
        return catalogs
    except mysql.connector.Error as e:
        print(f"❌ Error fetching catalogs: {e}")
        return []
    finally:
        cursor.close()

def get_databases(conn, catalog):
    cursor = conn.cursor()
    try:
        cursor.execute(f"SHOW DATABASES FROM `{catalog}`;")
        databases = [row[0] for row in cursor.fetchall()]
        # Exclude system databases
        excluded_dbs = {"_statistics_", "information_schema", "sys"}
        return [db for db in databases if db not in excluded_dbs]
    except mysql.connector.Error as e:
        print(f"❌ Error fetching databases from {catalog}: {e}")
        return []
    finally:
        cursor.close()

def get_tables(conn, catalog, database):
    cursor = conn.cursor()
    try:
        cursor.execute(f"SHOW TABLES FROM `{catalog}`.`{database}`;")
        tables = [row[0] for row in cursor.fetchall()]
        return tables
    except mysql.connector.Error as e:
        print(f"❌ Error fetching tables from {catalog}.{database}: {e}")
        return []
    finally:
        cursor.close()

def get_table_schema(conn, catalog, database, table):
    cursor = conn.cursor()
    try:
        cursor.execute(f"SHOW CREATE TABLE `{catalog}`.`{database}`.`{table}`;")
        schema = cursor.fetchone()[1]  # The second column is the CREATE TABLE statement
        return schema
    except mysql.connector.Error as e:
        print(f"❌ Error fetching schema for {catalog}.{database}.{table}: {e}")
        return None
    finally:
        cursor.close()

def get_table_partitions(conn, catalog, database, table, db_dir):
    cursor = conn.cursor()
    try:
        cursor.execute(f"SHOW PARTITIONS FROM `{catalog}`.`{database}`.`{table}`;")
        partitions = cursor.fetchall()
        if partitions:
            partition_file = os.path.join(db_dir, f"{table}_partitions.csv")
            with open(partition_file, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow([i[0] for i in cursor.description])  # Header
                writer.writerows(partitions)
            print(f"✅ Saved partitions for {catalog}.{database}.{table} to {partition_file}")
    except mysql.connector.Error as e:
        print(f"❌ Error fetching partitions for {catalog}.{database}.{table}: {e}")
    finally:
        cursor.close()

def get_table_tablets(conn, catalog, database, table, db_dir):
    cursor = conn.cursor()
    try:
        cursor.execute(f"SHOW TABLET FROM `{catalog}`.`{database}`.`{table}`;")
        tablets = cursor.fetchall()
        if tablets:
            tablets_file = os.path.join(db_dir, f"{table}_tablets.csv")
            with open(tablets_file, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow([i[0] for i in cursor.description])  # Header
                writer.writerows(tablets)
            print(f"✅ Saved tablets for {catalog}.{database}.{table} to {tablets_file}")
    except mysql.connector.Error as e:
        print(f"❌ Error fetching tablets for {catalog}.{database}.{table}: {e}")
    finally:
        cursor.close()

def detect_cluster_architecture(perf_dir):
    backends_file = os.path.join(perf_dir, "backends.csv")
    compute_nodes_file = os.path.join(perf_dir, "compute_nodes.csv")

    has_backends = os.path.exists(backends_file) and sum(1 for _ in open(backends_file)) > 1
    has_compute_nodes = os.path.exists(compute_nodes_file) and sum(1 for _ in open(compute_nodes_file)) > 1

    if has_backends and not has_compute_nodes:
        arch_type = "shared-nothing"
    elif has_compute_nodes and not has_backends:
        arch_type = "shared-data"
    elif has_backends and has_compute_nodes:
        arch_type = "hybrid"
    else:
        arch_type = "unknown"

    with open(os.path.join(perf_dir, "cluster_architecture.txt"), "w") as f:
        f.write(f"Cluster Architecture: {arch_type}\n")
    log_warning(perf_dir, f"✅ Cluster architecture detected: {arch_type}")

    return arch_type


def check_table_replication(conn, perf_dir, arch_type):
    expected_replication = 3 if arch_type == "shared-nothing" else 1
    query = f"""
    SELECT DISTINCT
        pm.DB_NAME,
        pm.TABLE_NAME,
        pm.REPLICATION_NUM
    FROM
        information_schema.partitions_meta pm
    WHERE
        pm.REPLICATION_NUM != {expected_replication}
        AND pm.REPLICATION_NUM IS NOT NULL
    ORDER BY
        pm.DB_NAME,
        pm.TABLE_NAME;
    """
    save_query_to_csv(conn.cursor(), query, os.path.join(perf_dir, "replication_check.csv"), perf_dir, "⚠️  WARNING: Found tables with incorrect replication factors.")


def check_oversized_tablets(conn, perf_dir):
    query = """
    WITH BECOUNT AS (
        SELECT COUNT(DISTINCT BE_ID) AS be_node_count
        FROM information_schema.be_tablets
    ),
    PartitionData AS (
        SELECT
            pm.DB_NAME,
            pm.TABLE_NAME,
            pm.PARTITION_NAME,
            COALESCE(SUM(tbt.DATA_SIZE), 0) / (1024 * 1024 * 1024) AS partition_data_size_gb
        FROM
            information_schema.partitions_meta pm
        LEFT JOIN
            information_schema.be_tablets tbt ON pm.PARTITION_ID = tbt.PARTITION_ID
        GROUP BY
            pm.DB_NAME,
            pm.TABLE_NAME,
            pm.PARTITION_NAME
    ),
    TableStats AS (
        SELECT
            pd.DB_NAME,
            pd.TABLE_NAME,
            pd.PARTITION_NAME,
            pd.partition_data_size_gb,
            CASE
                WHEN tc.DISTRIBUTE_BUCKET = 0 THEN (SELECT be_node_count * 2 FROM BECOUNT)
                ELSE tc.DISTRIBUTE_BUCKET
            END AS buckets
        FROM
            PartitionData pd
        JOIN
            information_schema.tables_config tc ON pd.DB_NAME = tc.TABLE_SCHEMA AND pd.TABLE_NAME = tc.TABLE_NAME
        CROSS JOIN
            BECOUNT
    )
    SELECT
        ts.DB_NAME,
        ts.TABLE_NAME,
        ts.PARTITION_NAME,
        ts.partition_data_size_gb,
        ts.buckets,
        ts.partition_data_size_gb / ts.buckets AS capacity_per_bucket_gb
    FROM
        TableStats ts
    ORDER BY
        capacity_per_bucket_gb DESC
    LIMIT 100;
    """
    save_query_to_csv(conn.cursor(), query, os.path.join(perf_dir, "oversized_tablets.csv"), perf_dir, "⚠️  WARNING: Found oversized partitions.")


def check_data_skew(conn, perf_dir):
    query = """
    WITH TableBEData AS (
        SELECT
            tc.TABLE_SCHEMA AS db_name,
            tc.TABLE_NAME,
            tbt.BE_ID,
            SUM(tbt.DATA_SIZE) / (1024 * 1024 * 1024) AS data_size_gb
        FROM
            information_schema.tables_config tc
        JOIN
            information_schema.be_tablets tbt ON tc.TABLE_ID = tbt.TABLE_ID
        GROUP BY
            tc.TABLE_SCHEMA,
            tc.TABLE_NAME,
            tbt.BE_ID
    ),
    TableStats AS (
        SELECT
            db_name,
            TABLE_NAME,
            SUM(data_size_gb) AS total_data_size_gb,
            STDDEV(data_size_gb) AS data_skew_stddev,
            AVG(data_size_gb) AS avg_data_size_per_be,
            COUNT(DISTINCT BE_ID) AS be_count,
            CASE
                WHEN AVG(data_size_gb) > 0 THEN STDDEV(data_size_gb) / AVG(data_size_gb)
                ELSE 0
            END AS skew_cv
        FROM
            TableBEData
        GROUP BY
            db_name,
            TABLE_NAME
    )
    SELECT
        db_name,
        TABLE_NAME,
        total_data_size_gb,
        data_skew_stddev,
        avg_data_size_per_be,
        be_count,
        skew_cv,
        CASE
            WHEN skew_cv > 0.5 THEN 'Severe'
            WHEN skew_cv BETWEEN 0.2 AND 0.5 THEN 'Medium'
            ELSE 'Low'
        END AS skew_level
    FROM
        TableStats
    WHERE
        be_count > 1
    ORDER BY
        data_skew_stddev DESC
    LIMIT 100;
    """
    save_query_to_csv(conn.cursor(), query, os.path.join(perf_dir, "data_skew.csv"), perf_dir, "⚠️  WARNING: Found tables with severe data skew.")

def check_too_many_tablets(conn, perf_dir):
    query = """
    WITH BECOUNT AS (
        SELECT COUNT(DISTINCT BE_ID) AS be_node_count
        FROM information_schema.be_tablets
    ),
    PartitionData AS (
        SELECT
            pm.DB_NAME,
            pm.TABLE_NAME,
            pm.PARTITION_NAME,
            COALESCE(SUM(tbt.DATA_SIZE), 0) AS partition_data_size,
            COUNT(tbt.TABLET_ID) AS tablet_count
        FROM
            information_schema.partitions_meta pm
        LEFT JOIN
            information_schema.be_tablets tbt ON pm.PARTITION_ID = tbt.PARTITION_ID
        GROUP BY
            pm.DB_NAME,
            pm.TABLE_NAME,
            pm.PARTITION_NAME
    ),
    TableStats AS (
        SELECT
            pd.DB_NAME,
            pd.TABLE_NAME,
            SUM(pd.tablet_count) AS tablet_count,
            SUM(pd.partition_data_size) / (1024 * 1024 * 1024) AS total_data_size_gb,
            CASE
                WHEN SUM(pd.tablet_count) > 0 THEN SUM(pd.partition_data_size) / SUM(pd.tablet_count) / (1024 * 1024 * 1024)
                ELSE 0
            END AS avg_data_size_per_tablet_gb
        FROM
            PartitionData pd
        GROUP BY
            pd.DB_NAME,
            pd.TABLE_NAME
    )
    SELECT
        ts.DB_NAME,
        ts.TABLE_NAME,
        ts.tablet_count,
        ts.total_data_size_gb,
        ts.avg_data_size_per_tablet_gb
    FROM
        TableStats ts
    ORDER BY
        ts.tablet_count DESC
    LIMIT 100;
    """
    save_query_to_csv(conn.cursor(), query, os.path.join(perf_dir, "too_many_tablets.csv"), perf_dir, "⚠️  WARNING: Found tables with too many tablets.")

def check_partition_data_skew(conn, perf_dir):
    query = """
    WITH PartitionBEData AS (
        SELECT
            pm.DB_NAME AS db_name,
            pm.TABLE_NAME,
            pm.PARTITION_NAME,
            tbt.BE_ID,
            COALESCE(SUM(tbt.DATA_SIZE), 0) / (1024 * 1024 * 1024) AS partition_data_size_gb
        FROM
            information_schema.partitions_meta pm
        JOIN
            information_schema.be_tablets tbt ON pm.PARTITION_ID = tbt.PARTITION_ID
        GROUP BY
            pm.DB_NAME,
            pm.TABLE_NAME,
            pm.PARTITION_NAME,
            tbt.BE_ID
    ),
    PartitionStats AS (
        SELECT
            db_name,
            TABLE_NAME,
            PARTITION_NAME,
            SUM(partition_data_size_gb) AS total_partition_data_size_gb,
            STDDEV(partition_data_size_gb) AS data_skew_stddev,
            AVG(partition_data_size_gb) AS avg_data_size_per_be,
            COUNT(DISTINCT BE_ID) AS be_count,
            CASE
                WHEN AVG(partition_data_size_gb) > 0 THEN STDDEV(partition_data_size_gb) / AVG(partition_data_size_gb)
                ELSE 0
            END AS skew_cv
        FROM
            PartitionBEData
        GROUP BY
            db_name,
            TABLE_NAME,
            PARTITION_NAME
    )
    SELECT
        db_name,
        TABLE_NAME,
        PARTITION_NAME,
        total_partition_data_size_gb,
        data_skew_stddev,
        avg_data_size_per_be,
        be_count,
        skew_cv,
        CASE
            WHEN skew_cv > 0.5 THEN 'Severe'
            WHEN skew_cv BETWEEN 0.2 AND 0.5 THEN 'Medium'
            ELSE 'Low'
        END AS skew_level
    FROM
        PartitionStats
    WHERE
        be_count > 1
    ORDER BY
        data_skew_stddev DESC
    LIMIT 100;
    """
    save_query_to_csv(conn.cursor(), query, os.path.join(perf_dir, "partition_data_skew.csv"), perf_dir, "⚠️  WARNING: Found partitions with severe data skew.")

def check_too_many_versions_or_segments(conn, perf_dir):
    query = """
    SELECT
        pm.DB_NAME,
        pm.TABLE_NAME,
        tbt.TABLET_ID,
        tbt.NUM_VERSION,
        tbt.NUM_SEGMENT,
        tbt.DATA_SIZE / (1024 * 1024 * 1024) AS data_size_gb
    FROM
        information_schema.partitions_meta pm
    JOIN
        information_schema.be_tablets tbt ON pm.PARTITION_ID = tbt.PARTITION_ID
    WHERE
        tbt.NUM_VERSION > 100
        OR tbt.NUM_SEGMENT > 50
    ORDER BY
        tbt.NUM_VERSION DESC,
        tbt.NUM_SEGMENT DESC
    LIMIT 100;
    """
    save_query_to_csv(conn.cursor(), query, os.path.join(perf_dir, "too_many_versions_or_segments.csv"), perf_dir, "⚠️  WARNING: Found tablets with too many versions or segments.")

def check_empty_tables(conn, perf_dir):
    query = """
    SELECT
        pm.DB_NAME,
        pm.TABLE_NAME,
        COUNT(DISTINCT pm.PARTITION_ID) AS partition_count,
        COUNT(tbt.TABLET_ID) AS tablet_count,
        COALESCE(SUM(tbt.DATA_SIZE), 0) AS total_data_size
    FROM
        information_schema.partitions_meta pm
    LEFT JOIN
        information_schema.be_tablets tbt ON pm.PARTITION_ID = tbt.PARTITION_ID
    GROUP BY
        pm.DB_NAME,
        pm.TABLE_NAME
    HAVING
        COALESCE(SUM(tbt.DATA_SIZE), 0) = 0
    ORDER BY
        pm.DB_NAME,
        pm.TABLE_NAME;
    """
    save_query_to_csv(
        conn.cursor(),
        query,
        os.path.join(perf_dir, "empty_tables.csv"),
        perf_dir,
        "⚠️  WARNING: Found empty tables."
    )

def check_empty_partitions(conn, perf_dir):
    query = """
    SELECT
        pm.DB_NAME,
        pm.TABLE_NAME,
        pm.PARTITION_NAME,
        COUNT(tbt.TABLET_ID) AS tablet_count,
        COALESCE(SUM(tbt.DATA_SIZE), 0) AS partition_data_size
    FROM
        information_schema.partitions_meta pm
    LEFT JOIN
        information_schema.be_tablets tbt ON pm.PARTITION_ID = tbt.PARTITION_ID
    GROUP BY
        pm.DB_NAME,
        pm.TABLE_NAME,
        pm.PARTITION_NAME
    HAVING
        COALESCE(SUM(tbt.DATA_SIZE), 0) = 0
    ORDER BY
        pm.DB_NAME,
        pm.TABLE_NAME,
        pm.PARTITION_NAME;
    """
    save_query_to_csv(
        conn.cursor(),
        query,
        os.path.join(perf_dir, "empty_partitions.csv"),
        perf_dir,
        "⚠️  WARNING: Found empty partitions."
    )

def check_large_unpartitioned_tables(conn, perf_dir):
    query = """
    WITH TableData AS (
        SELECT
            pm.DB_NAME,
            pm.TABLE_NAME,
            COUNT(DISTINCT pm.PARTITION_NAME) AS partition_count,
            COALESCE(SUM(tbt.DATA_SIZE), 0) / (1024 * 1024 * 1024) AS total_data_size_gb
        FROM
            information_schema.partitions_meta pm
        LEFT JOIN
            information_schema.be_tablets tbt ON pm.PARTITION_ID = tbt.PARTITION_ID
        GROUP BY
            pm.DB_NAME,
            pm.TABLE_NAME
    )
    SELECT
        td.DB_NAME,
        td.TABLE_NAME,
        td.partition_count,
        td.total_data_size_gb
    FROM
        TableData td
    WHERE
        td.total_data_size_gb > 10
        AND td.partition_count = 1
    ORDER BY
        td.total_data_size_gb DESC;
    """
    save_query_to_csv(
        conn.cursor(),
        query,
        os.path.join(perf_dir, "large_unpartitioned_tables.csv"),
        perf_dir,
        "⚠️  WARNING: Found large tables without partitions."
    )

def check_single_bucket_large_tables(conn, perf_dir):
    query = """
    WITH PartitionData AS (
        -- Calculate the total data size for each partition
        SELECT
            pm.DB_NAME,
            pm.TABLE_NAME,
            pm.PARTITION_NAME,
            COALESCE(SUM(tbt.DATA_SIZE), 0) / (1024 * 1024 * 1024) AS partition_data_size_gb
        FROM
            information_schema.partitions_meta pm
        LEFT JOIN
            information_schema.be_tablets tbt ON pm.PARTITION_ID = tbt.PARTITION_ID
        GROUP BY
            pm.DB_NAME,
            pm.TABLE_NAME,
            pm.PARTITION_NAME
    ),
    TableData AS (
        -- Calculate the total and maximum partition data size for each table
        SELECT
            pd.DB_NAME,
            pd.TABLE_NAME,
            COUNT(DISTINCT pd.PARTITION_NAME) AS partition_count,
            SUM(pd.partition_data_size_gb) AS total_data_size_gb,
            MAX(pd.partition_data_size_gb) AS max_partition_data_size_gb
        FROM
            PartitionData pd
        GROUP BY
            pd.DB_NAME,
            pd.TABLE_NAME
    ),
    FilteredTables AS (
        -- Filter tables with a single bucket and large capacity
        SELECT
            td.DB_NAME,
            td.TABLE_NAME,
            td.partition_count,
            td.total_data_size_gb,
            td.max_partition_data_size_gb
        FROM
            TableData td
        JOIN
            information_schema.tables_config tc ON td.DB_NAME = tc.TABLE_SCHEMA AND td.TABLE_NAME = tc.TABLE_NAME
        WHERE
            tc.DISTRIBUTE_BUCKET = 1
            AND (
                -- Non-partitioned tables: 1 partition and total data size > 1GB
                (td.partition_count = 1 AND td.total_data_size_gb > 1)
                -- Partitioned tables: more than 1 partition and max partition size > 1GB
                OR (td.partition_count > 1 AND td.max_partition_data_size_gb > 1)
            )
    )
    SELECT
        ft.DB_NAME,
        ft.TABLE_NAME,
        ft.partition_count,
        ft.total_data_size_gb,
        ft.max_partition_data_size_gb,
        tc.DISTRIBUTE_BUCKET AS buckets
    FROM
        FilteredTables ft
    JOIN
        information_schema.tables_config tc ON ft.DB_NAME = tc.TABLE_SCHEMA AND ft.TABLE_NAME = tc.TABLE_NAME
    ORDER BY
        ft.total_data_size_gb DESC;
    """
    save_query_to_csv(
        conn.cursor(),
        query,
        os.path.join(perf_dir, "single_bucket_large_tables.csv"),
        perf_dir,
        "⚠️  WARNING: Found tables with a single bucket and large capacity."
    )

def check_non_persistent_index_tables(conn, perf_dir):
    query = """
    WITH TableDataSize AS (
        -- Calculate the total data size for each table
        SELECT
            pm.DB_NAME,
            pm.TABLE_NAME,
            COALESCE(SUM(tbt.DATA_SIZE), 0) / (1024 * 1024 * 1024) AS total_data_size_gb
        FROM
            information_schema.partitions_meta pm
        LEFT JOIN
            information_schema.be_tablets tbt ON pm.PARTITION_ID = tbt.PARTITION_ID
        GROUP BY
            pm.DB_NAME,
            pm.TABLE_NAME
    )
    SELECT
        tc.TABLE_SCHEMA AS "database name",
        tc.TABLE_NAME AS "table name",
        tc.properties,
        td.total_data_size_gb AS "data size (GB)",
        td.total_data_size_gb * 0.15 AS "estimated memory usage (GB)"
    FROM
        information_schema.tables_config tc
    LEFT JOIN
        TableDataSize td ON tc.TABLE_SCHEMA = td.DB_NAME AND tc.TABLE_NAME = td.TABLE_NAME
    WHERE
        tc.table_model = "PRIMARY_KEYS"
        AND tc.properties LIKE '%enable_persistent_index":"false"%'
    ORDER BY
        td.total_data_size_gb DESC;
    """
    save_query_to_csv(
        conn.cursor(),
        query,
        os.path.join(perf_dir, "non_persistent_index_tables.csv"),
        perf_dir,
        "⚠️  WARNING: Found primary key tables without persistent indexes."
    )

def check_abnormal_replica_status(conn, perf_dir):
    query = """
    SELECT
        pm.DB_NAME,
        pm.TABLE_NAME,
        tbt.TABLET_ID,
        tbt.BE_ID,
        tbt.STATE,
        tbt.DATA_SIZE / (1024 * 1024 * 1024) AS data_size_gb
    FROM
        information_schema.partitions_meta pm
    JOIN
        information_schema.be_tablets tbt ON pm.PARTITION_ID = tbt.PARTITION_ID
    WHERE
        tbt.STATE NOT IN ('NORMAL', 'RUNNING')
    ORDER BY
        pm.DB_NAME,
        pm.TABLE_NAME,
        tbt.TABLET_ID;
    """
    save_query_to_csv(
        conn.cursor(),
        query,
        os.path.join(perf_dir, "abnormal_replicas.csv"),
        perf_dir,
        "⚠️  WARNING: Found tablets with abnormal replica state."
    )

def main():
    conn = get_connection()
    timestamp = datetime.now().strftime("%Y%m%d%H%M")
    root_dir = f"starrocks_metadata_{timestamp}"
    perf_dir = os.path.join(root_dir, "performance_indicators")
    os.makedirs(perf_dir, exist_ok=True)

    cursor = conn.cursor()

    save_query_to_csv(cursor, "SHOW FRONTENDS;", os.path.join(perf_dir, "frontends.csv"), perf_dir, "✅ Saved frontends info.")
    save_query_to_csv(cursor, "SHOW BACKENDS;", os.path.join(perf_dir, "backends.csv"), perf_dir, "✅ Saved backends info.")
    save_query_to_csv(cursor, "SHOW COMPUTE NODES;", os.path.join(perf_dir, "compute_nodes.csv"), perf_dir, "✅ Saved compute nodes info.")

    # Collect metadata for each catalog, database, and table
    catalogs = get_catalogs(conn)
    for catalog in catalogs:
        catalog_dir = os.path.join(root_dir, catalog)
        os.makedirs(catalog_dir, exist_ok=True)

        databases = get_databases(conn, catalog)
        for database in databases:
            db_dir = os.path.join(catalog_dir, database)
            os.makedirs(db_dir, exist_ok=True)

            tables = get_tables(conn, catalog, database)
            for table in tables:
                # Save table schema
                schema = get_table_schema(conn, catalog, database, table)
                if schema:
                    schema_file = os.path.join(db_dir, f"{table}.sql")
                    with open(schema_file, "w") as f:
                        f.write(schema)
                    print(f"✅ Saved schema for {catalog}.{database}.{table} to {schema_file}")

                # Save partitions and tablets
                get_table_partitions(conn, catalog, database, table, db_dir)
                get_table_tablets(conn, catalog, database, table, db_dir)


    arch_type = detect_cluster_architecture(perf_dir)
    check_table_replication(conn, perf_dir, arch_type)
    check_oversized_tablets(conn, perf_dir)
    check_data_skew(conn, perf_dir)
    check_too_many_tablets(conn, perf_dir)
    check_partition_data_skew(conn, perf_dir)
    check_too_many_versions_or_segments(conn, perf_dir)
    check_empty_tables(conn, perf_dir)
    check_empty_partitions(conn, perf_dir)
    check_large_unpartitioned_tables(conn, perf_dir)
    check_single_bucket_large_tables(conn, perf_dir)
    check_non_persistent_index_tables(conn, perf_dir)
    check_abnormal_replica_status(conn, perf_dir)

    cursor.close()
    conn.close()

    tar_file = f"{root_dir}.tar.gz"
    with tarfile.open(tar_file, "w:gz") as tar:
        tar.add(root_dir, arcname=os.path.basename(root_dir))
    print(f"✅ Created archive {tar_file}")

    shutil.rmtree(root_dir)
    print(f"🗑️  Deleted directory {root_dir} after archiving")


if __name__ == "__main__":
    main()
