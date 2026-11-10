---
sidebar_position: 101
displayed_sidebar: docs
keywords:
  - PostgreSQL
  - postgres
  - sync
  - Flink CDC
description: "How to use a Flink CDC pipeline to capture PostgreSQL changes and stream them into StarRocks Primary Key tables in real time."
---

# Realtime synchronization from PostgreSQL

import InsertPrivNote from '../_assets/commonMarkdown/insertPrivNote.mdx'
import FlinkStarRocksConnection from '../_assets/commonMarkdown/Edition_Specific_Flink_StarRocks_Connection.mdx'

This topic shows how to stream data from PostgreSQL into StarRocks with a [Flink CDC](https://nightlies.apache.org/flink/flink-cdc-docs-stable/) pipeline. The pipeline copies the existing rows of each table, then keeps applying every INSERT, UPDATE, and DELETE from PostgreSQL within seconds. It creates the StarRocks tables for you, so there is no separate schema migration step.

<InsertPrivNote />

## How it works

A Flink CDC pipeline is a YAML file with a **source** (PostgreSQL), a **sink** (StarRocks), and optional **route** and **transform** rules. You submit the file to a Flink cluster, and it runs as a Flink job:

1. The PostgreSQL source takes a snapshot of each matching table, then reads changes from the PostgreSQL write-ahead log (WAL) through a logical replication slot.
2. The StarRocks sink creates a [Primary Key table](../table_design/table_types/primary_key_table.md) for each source table that does not already exist in StarRocks, then loads the rows with [Stream Load](./StreamLoad.md). Because every table has a primary key, updates overwrite the existing row and deletes remove it.

Delivery is at-least-once: after a failure, some changes can be written again, and the Primary Key table makes the repeat harmless.

:::note

The pipeline does not synchronize schema changes from PostgreSQL. See [Change a table's schema](#change-a-tables-schema).

:::

## Before you begin

You need:

- PostgreSQL 10 or later. This topic uses the built-in `pgoutput` logical decoding plugin, so no server extension is required.
- A StarRocks cluster.
- A Flink 1.20 cluster, and Java 11 or 17 to run it.
- Network access from every Flink TaskManager to PostgreSQL, on its port (default `5432`), and to StarRocks, as described in [Connect to StarRocks](#connect-to-starrocks).
- A primary key on every PostgreSQL table you synchronize. StarRocks creates the destination table with the same primary key.

## Step 1: Prepare PostgreSQL

Run these steps as a PostgreSQL superuser, or as the owner of the tables. The examples synchronize the tables in the `public` schema of a database named `shop`.

1. Enable logical replication. Set `wal_level` to `logical` in `postgresql.conf`:

   ```properties
   wal_level = logical
   # Each running pipeline uses one replication slot and one WAL sender.
   max_replication_slots = 4
   max_wal_senders = 4
   ```

   Changing `wal_level` requires a restart of PostgreSQL. For a managed service such as Amazon RDS or Cloud SQL, set the equivalent parameter (for example, `rds.logical_replication = 1`) in the instance's parameter group instead.

   Check the setting after the restart:

   ```sql
   SHOW wal_level;
   ```

   ```plaintext
    wal_level
   -----------
    logical
   ```

2. Create a user for Flink CDC. It needs the `REPLICATION` attribute and read access to the tables:

   ```sql
   CREATE ROLE flink_cdc WITH LOGIN REPLICATION PASSWORD '<password>';
   GRANT CONNECT ON DATABASE shop TO flink_cdc;
   GRANT USAGE ON SCHEMA public TO flink_cdc;
   GRANT SELECT ON ALL TABLES IN SCHEMA public TO flink_cdc;
   ```

   If `pg_hba.conf` restricts replication connections, allow this user to connect from the Flink TaskManager hosts. For example:

   ```plaintext
   # TYPE  DATABASE     USER       ADDRESS          METHOD
   host    shop         flink_cdc  10.0.0.0/24      scram-sha-256
   host    replication  flink_cdc  10.0.0.0/24      scram-sha-256
   ```

3. Create a publication for the tables to synchronize. It must list every table that the pipeline reads (the `tables` option in [Step 4](#step-4-define-the-pipeline)):

   ```sql
   CREATE PUBLICATION flink_pub FOR TABLE public.orders, public.customers;
   ```

   :::tip

   Create the publication yourself. If none exists, the connector tries to run `CREATE PUBLICATION ... FOR ALL TABLES`, which only a superuser can do. For any other user that fails, and Flink retries until the error it reports is about `max_wal_senders` rather than the missing publication.

   :::

4. Set the replica identity of each table to `FULL`, so that the WAL records the complete previous row for every UPDATE and DELETE:

   ```sql
   ALTER TABLE public.orders REPLICA IDENTITY FULL;
   ALTER TABLE public.customers REPLICA IDENTITY FULL;
   ```

   Do this before you start the pipeline. With the default replica identity, the first UPDATE or DELETE fails the Flink job with a `NullPointerException`, and the job keeps failing on that change after every restart.

## Step 2: Prepare StarRocks

Create the destination database and a user for the pipeline:

```sql
CREATE DATABASE shop;

CREATE USER flink_sink IDENTIFIED BY '<password>';
GRANT CREATE TABLE ON DATABASE shop TO USER flink_sink;
GRANT SELECT, INSERT, UPDATE, DELETE, ALTER ON ALL TABLES IN DATABASE shop TO USER flink_sink;
```

## Step 3: Install Flink CDC

1. Download and start Flink 1.20. See [First steps](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/try-flink/local_installation/) in the Flink documentation.

2. Turn on checkpointing by adding this line to the Flink configuration file, `conf/config.yaml`, and then restart the cluster:

   ```yaml
   execution.checkpointing.interval: 10s
   ```

   The PostgreSQL source reports its position in the WAL back to PostgreSQL as checkpoints complete. Without checkpoints, the replication slot keeps every WAL segment since the pipeline started, and the PostgreSQL disk fills up.

3. Download the Flink CDC release that matches your Flink version from the [Flink CDC downloads](https://flink.apache.org/downloads/#apache-flink-cdc) page, and extract it. Starting with Flink CDC 3.6, each release is built for one Flink version, so for Flink 1.20 download `flink-cdc-3.6.0-1.20-bin.tar.gz`, not the `-2.2` package.

   ```bash
   tar -xzf flink-cdc-3.6.0-1.20-bin.tar.gz
   cd flink-cdc-3.6.0-1.20
   ```

4. Download the PostgreSQL and StarRocks pipeline connectors from Maven Central into the `lib` directory of Flink CDC. Use the same version as the Flink CDC release:

   ```bash
   cd lib
   wget https://repo1.maven.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-postgres/3.6.0-1.20/flink-cdc-pipeline-connector-postgres-3.6.0-1.20.jar
   wget https://repo1.maven.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-starrocks/3.6.0-1.20/flink-cdc-pipeline-connector-starrocks-3.6.0-1.20.jar
   cd ..
   ```

## Step 4: Define the pipeline

Create a file named `postgres-to-starrocks.yaml`:

```yaml
source:
  type: postgres
  name: PostgreSQL source
  hostname: <postgres_host>
  port: 5432
  username: flink_cdc
  password: <password>
  # Comma-separated <database>.<schema>.<table> entries; the table part can be a
  # regular expression. List the same tables as the publication.
  tables: shop.public.orders, shop.public.customers
  # Replication slot to create and use. Lowercase letters, digits, and underscores only.
  slot.name: flink_starrocks
  decoding.plugin.name: pgoutput
  debezium.publication.name: flink_pub

sink:
  type: starrocks
  name: StarRocks sink
  # For the StarRocks addresses, see "Connect to StarRocks" below.
  jdbc-url: jdbc:mysql://<fe_host>:9030
  load-url: <fe_host>:8030
  username: flink_sink
  password: <password>
  # The default is 300000 (5 minutes). A lower value makes changes visible sooner.
  sink.buffer-flush.interval-ms: 5000

route:
  # Write each table in the public schema to the StarRocks database shop.
  - source-table: public.\.*
    sink-table: shop.<>
    replace-symbol: <>

pipeline:
  name: PostgreSQL to StarRocks
  parallelism: 1
```

Pay attention to these settings:

- `tables` must match the publication. The snapshot copies every table that `tables` selects, but PostgreSQL streams changes only for the tables in the publication. A table outside the publication is copied once and then never updated, and the job reports no error.
- `tables` names tables as `<database>.<schema>.<table>`. Inside the pipeline, however, a PostgreSQL table is identified only by `<schema>.<table>`, so `route` and `transform` rules match `public.orders`, not `shop.public.orders`.
- Without the `route` block, the sink writes each table to a StarRocks database named after the PostgreSQL **schema**, so the tables in `public` land in a database called `public`.
- `sink.buffer-flush.interval-ms` defaults to 5 minutes. With the default, a new pipeline looks idle for several minutes after it starts.
- To set properties on the tables that the sink creates, add `table.create.properties.<property>` to the `sink` section, for example `table.create.properties.replication_num: 1`.

For every source and sink option, see the Flink CDC [Postgres connector](https://nightlies.apache.org/flink/flink-cdc-docs-stable/docs/connectors/pipeline-connectors/postgres/) and [StarRocks connector](https://nightlies.apache.org/flink/flink-cdc-docs-stable/docs/connectors/pipeline-connectors/starrocks/) references.

### Connect to StarRocks

<FlinkStarRocksConnection />

### Columns with a function as the default value

The sink copies each column's default value from PostgreSQL into the StarRocks CREATE TABLE statement. A default that is a function call, such as `DEFAULT now()`, is not valid in StarRocks, and the job fails with an error like this one:

```plaintext
Invalid default value for 'created_at': date literal [now()] is invalid.
```

Creating the StarRocks table yourself beforehand does not avoid the error, because the sink still sends its CREATE TABLE statement. Instead, add a `transform` rule that recomputes the column. A computed column has no default value:

```yaml
transform:
  - source-table: public.orders
    projection: order_id, customer, amount, status, CAST(created_at AS TIMESTAMP(6)) AS created_at
```

List every column of the table in `projection`. A column that is not listed is not synchronized.

## Step 5: Start the pipeline

Set `FLINK_HOME` to your Flink installation, and submit the pipeline from the Flink CDC directory:

```bash
export FLINK_HOME=/path/to/flink-1.20
bin/flink-cdc.sh postgres-to-starrocks.yaml
```

```plaintext
Pipeline has been submitted to cluster.
Job ID: cda97bac3b504442d8106a2d70c6074c
Job Description: PostgreSQL to StarRocks
```

The job appears in the Flink web UI (by default `http://<jobmanager_host>:8081`). Its status should stay `RUNNING`. If it changes to `RESTARTING`, open the job's **Exceptions** tab, and see [Troubleshooting](#troubleshooting).

## Step 6: Verify the synchronization

1. After the snapshot finishes, the existing rows are in StarRocks:

   ```sql
   SELECT * FROM shop.orders ORDER BY order_id;
   ```

2. Change some data in PostgreSQL:

   ```sql
   INSERT INTO public.orders (order_id, customer, amount, status) VALUES (5, 'erin', 42.00, 'new');
   UPDATE public.orders SET status = 'returned' WHERE order_id = 3;
   DELETE FROM public.orders WHERE order_id = 4;
   ```

3. Within a few seconds (the flush interval plus the load time), run the query in StarRocks again. It returns the same rows as the same query in PostgreSQL.

## Manage the pipeline

### Monitor the replication slot

A replication slot keeps WAL on the PostgreSQL server until the pipeline confirms that it has read it. While the pipeline is stopped, or if it falls behind, WAL builds up. Check how much WAL the slot is holding:

```sql
SELECT slot_name,
       active,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS retained_wal
FROM pg_replication_slots;
```

If you stop a pipeline for good, drop its slot so that PostgreSQL can release the WAL:

```sql
SELECT pg_drop_replication_slot('flink_starrocks');
```

### Change a table's schema

The pipeline does not pick up schema changes from PostgreSQL. If you add a column in PostgreSQL, the job keeps running, but it does not write the new column, even after you add the column to the StarRocks table.

To synchronize a table after a schema change:

1. Make the same change to the StarRocks table, for example with [ALTER TABLE](../sql-reference/sql-statements/table_bucket_part_index/ALTER_TABLE.md).
2. [Take a new snapshot](#take-a-new-snapshot).

### Take a new snapshot

To copy the tables again from the current state of PostgreSQL and then resume streaming:

1. Cancel the Flink job.
2. Drop the replication slot, as shown in [Monitor the replication slot](#monitor-the-replication-slot).
3. Truncate every StarRocks table that the pipeline writes to:

   ```sql
   TRUNCATE TABLE shop.orders;
   TRUNCATE TABLE shop.customers;
   ```

4. Submit the pipeline again. The pipeline takes a new snapshot of every table and then continues streaming changes.

Do not skip the truncate. Changes made in PostgreSQL between cancelling the job and the new snapshot are not replayed. The snapshot overwrites the rows that still exist, but a row deleted in PostgreSQL during that time would stay in StarRocks. The truncated tables are empty, or only partly loaded, until the snapshot finishes.

## Troubleshooting

When the job is `RESTARTING`, the **Exceptions** tab of the Flink web UI shows the cause. Look for the innermost `Caused by` line.

| Error | Cause and solution |
| --- | --- |
| `number of requested standby connections exceeds max_wal_senders` | Often a symptom of an earlier failure, because every retry opens a replication connection. Check the PostgreSQL server log for the first error. A common one is `permission denied for database` on `CREATE PUBLICATION`, which means the publication does not exist. See [Step 1](#step-1-prepare-postgresql). |
| `NullPointerException` in `DebeziumSchemaDataTypeInference` or `extractBeforeDataRecord` | A table does not have `REPLICA IDENTITY FULL`. Set it, and then [take a new snapshot](#take-a-new-snapshot). |
| `Invalid default value for '<column>'` | A column's PostgreSQL default is a function. See [Columns with a function as the default value](#columns-with-a-function-as-the-default-value). |
| `Connect to <host>:8040 ... Connection refused` | The TaskManager cannot reach the BE or CN that the FE redirected the load to. Make sure every BE and CN HTTP port is reachable from the TaskManagers at the address the BE or CN is registered with. |
| Nothing arrives in StarRocks, but the job is `RUNNING` | Wait for the flush interval. The default `sink.buffer-flush.interval-ms` is 5 minutes. |
| Rows arrive in a database named `public` | The pipeline has no `route` rule. See [Step 4](#step-4-define-the-pipeline). |

## See also

- [Realtime synchronization from MySQL](./Flink_cdc_load.md)
- [Continuously load data from Apache Flink®](./Flink-connector-starrocks.md)
- [Change data through loading](./Load_to_Primary_Key_tables.md)
