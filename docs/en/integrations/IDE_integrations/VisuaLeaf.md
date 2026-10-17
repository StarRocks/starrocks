---
sidebar_position: 37
displayed_sidebar: docs
description: "VisuaLeaf is a cross-platform desktop database client. Connect to StarRocks over its MySQL protocol to browse databases and run queries."
---

# VisuaLeaf

[VisuaLeaf](https://visualeaf.com/) is a desktop database client for Windows, macOS, and Linux. It
includes a SQL editor with a results grid, a visual query builder, ER diagrams, and charts built from
query results. VisuaLeaf has no dedicated StarRocks connection type; it connects to StarRocks over
the MySQL protocol using its **MySQL** connection type.

## Prerequisites

- A running StarRocks cluster reachable from the machine where VisuaLeaf runs, and its FE query port
  (MySQL protocol, default `9030`).
- VisuaLeaf itself. Download it from [https://visualeaf.com/download](https://visualeaf.com/download).
  A free Community Edition is available, as well as paid editions.

VisuaLeaf is a desktop application, so if StarRocks runs in Docker on the same machine, publish the
FE query port to the host and connect to `localhost`:

```sh
docker run -p 9030:9030 -p 8030:8030 starrocks/allin1-ubuntu
```

With this setup, use `localhost` as the host and `9030` as the port in the connection settings
below.

## Integration

1. Launch VisuaLeaf and click **New Connection**.
2. Search for `mysql` and select **MySQL** as the connection type. StarRocks speaks the MySQL
   protocol, so there is no separate StarRocks connection type to pick.
3. Fill in the connection settings:
   - **Host**: your FE hostname or IP address (`localhost` if StarRocks runs in Docker with its port
     published, as above)
   - **Port**: the FE query port, `9030` by default (not MySQL's `3306`)
   - **Database**: the target database name
   - **User** / **Password**: your StarRocks credentials, for example, `root` (which has no password
     by default)
4. Click **Test Connection** to verify the settings, then click **Save & Connect**.
5. Expand the connection in the **Connections** sidebar. It lists the databases in your cluster,
   with their tables and views.
6. Right-click a database and select **New SQL Editor**. Write a query and press **F5** to run all
   statements (**Run All**), or **F9** to run only the selected text (**Run**). Results appear in
   the grid below the editor.

![VisuaLeaf - Query result against StarRocks](../../_assets/IDE_visualeaf_1.png)
