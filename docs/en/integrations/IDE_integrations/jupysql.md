---
displayed_sidebar: "English"
---

# Jupyter

This guide describes how to integrate your StarRocks cluster with [Jupyter](https://jupyter.org/), the latest web-based interactive development environment for notebooks, code, and data.

All of this is made possible via [JupySQL](https://jupysql.ploomber.io/) which allows you to run SQL and plot large datasets in Jupyter via a %sql, %%sql, and %sqlplot magics.

You can use JupySQL on top of Jupyter to run queries on top of StarRocks.

Once the data is loaded into the cluster, you can query and visualize it via SQL plotting.

## Prerequisites

Before getting started, you must have the following software installed locally:

- [JupySQL](https://jupysql.ploomber.io/en/latest/quick-start.html): `pip install jupysql`
- Jupyterlab: `pip install jupyterlab`
- [SKlearn Evaluation](https://github.com/ploomber/sklearn-evaluation): `pip install sklearn-evaluation`
- Python
- pymysql: `pip install pymysql`

> **NOTE**
>
> Once you have the above requirements fulfilled, you can open Jupyter lab simply by calling `jupyterlab` - this will open the notebook interface.
> If Jupyter lab is already running in a notebook, you can simply run the cell bellow to get the dependencies.

```python
# Install required packages.
%pip install --quiet jupysql sklearn-evaluation pymysql
```

> **NOTE**
>
> You may need to restart the kernel to use updated packages.

```python
import pandas as pd
from sklearn_evaluation import plot

# Import JupySQL Jupyter extension to create SQL cells.
%load_ext sql
%config SqlMagic.autocommit=False
```

**You will need to make sure your StarRocks instance is up and reachable for the next stages.**

> **NOTE**
>
> You will need to adjust the connection string according to the instance type you are trying to connect to (url, user, and password). The example below uses a local instance.

## Connecting to StarRocks via JupySQL

In this example, a docker instance is used, and that is reflecting the data in the connection string.

The `root` user is used to connect to the local StarRocks instance, create a database, and check that data can actually be read from and written into the table.

```python
%sql mysql+pymysql://root:@localhost:9030
```

Create and use that JupySQL database:

```python
%sql CREATE DATABASE jupysql;
```

```python
%sql USE jupysql;
```

Create a table:

```python
%%sql
CREATE TABLE tbl(c1 int, c2 int) distributed by hash(c1) properties ("replication_num" = "1");
INSERT INTO tbl VALUES (1, 10), (2, 20), (3, 30);
SELECT * FROM tbl;
```

## Saving and loading queries

Now after you create a database, you can write some sample data into it and query it.

JupySQL allows you to break queries into multiple cells, simplifying the process of building large queries.

You can write complex queries, save them, and execute them when needed, in a similar manner to CTEs in SQL.

```python
# This is pending for the next JupySQL release.
%%sql --save initialize-table --no-execute
CREATE TABLE tbl(c1 int, c2 int) distributed by hash(c1) properties ("replication_num" = "1");
INSERT INTO tbl VALUES (1, 1), (2, 2), (3, 3);
SELECT * FROM tbl;
```

> **NOTE**
>
> `--save` stores the query, not the data.

Note that we are using `--with;`, this will retrieve previously saved queries, and prepend them (using CTEs). Then, we save the query in `track_fav`.

## Plotting directly on StarRocks

JupySQL comes with a few plots by default, allowing you to visualize the data directly in SQL.

You can use a bar plot to visualize the data in your newly created table:

```python
top_artist = %sql SELECT * FROM tbl
top_artist.bar()
```

Now you have a new bar plot without any extra code. You can run SQL directly from your notebook via JupySQL (by ploomber). This adds lots of possibilities around StarRocks for data scientists and engineers. In case that you got stuck or need any support, please reach out to us via Slack.
