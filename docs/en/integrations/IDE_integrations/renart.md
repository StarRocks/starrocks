---
sidebar_position: 60
displayed_sidebar: docs
description: Connect the Renart open source data platform to StarRocks and query tables from a local workspace backed by Git.
---

# Renart

[Renart](https://github.com/renart-data/renart) is an open source data platform
for moving, transforming and analyzing data across databases and warehouses.
It combines SQL and Python pipelines, notebooks and dashboards in a local
workspace, with type checking, scheduling and definitions kept as code in Git.
This guide connects Renart to a StarRocks
database, browses tables and columns, previews rows and runs a query.

## Prerequisites

- Renart v0.5.5, installed using the [Renart installation guide](https://getrenart.com/docs/installation/).
- A local Git repository to open as a Renart workspace.
- A running StarRocks cluster, its FE hostname and query port, and credentials
  authorized to query the target database.

The connection and query workflow was tested with Renart v0.5.5 and StarRocks
3.5.21.

## Connect to StarRocks

1. From your local Git repository, run `renart web` to open Renart.
2. Open **Build → Connections**, add a connection, and select **StarRocks**.
3. Enter a connection name, such as `starrocks-analytics`, and the following values:

   | Field | Value |
   | --- | --- |
   | Host | Your StarRocks FE hostname or IP address |
   | Port | The FE query port, normally `9030` |
   | Database | The database you want to query |
   | Username | Your StarRocks username |
   | Password | The password for that user |

4. Use a credential source for the password. Renart supports an operating-system
   credential store, an encrypted local vault, or an environment-variable binding.
   See [Renart connection credentials](https://getrenart.com/docs/connections-environments/managing-credentials/).
5. Select **Access → Read-only** for a query-only connection.
6. Click **Verify**, then save the connection in the intended environment.

:::note
Read-only access restricts operations managed by Renart. Also use a database
account with appropriate permissions. An ad-hoc query on a writable connection
can execute writes.
:::

## Browse tables and columns

Open **Data Browser** in Renart, choose the saved StarRocks connection and
navigate to your database. Select a table to inspect its column names and database types, and preview
its rows. Use the same connection and environment for the SQL queries below.

## Run a query

In Renart's SQL query workspace, select the saved connection and environment.
First check the connected StarRocks version:

```sql
SELECT current_version();
```

Then query a table that the account can read. Replace the example database and
table names with your own:

```sql
SELECT * FROM my_database.my_table LIMIT 100;
```

Keep an explicit SQL limit when exploring a large table. A result display cap
does not necessarily limit the work performed by the database.

## Troubleshooting

- If verification fails, check FE reachability, the query port, the database
  name and the user's permissions. The query port is not the HTTP load endpoint.
- Table materialization and loading require a writable connection and their
  own configuration. This query guide does not configure Stream Load.

Report Renart issues in the [Renart repository](https://github.com/renart-data/renart/issues).
