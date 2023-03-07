# Integrate Jupyter with StarRocks

This guide describes how to integrate your StarRocks cluster with [Jupyter](https://jupyter.org/), the latest web-based interactive development environment for notebooks, code, and data.
All of this is made possible via [Jupysql](https://jupysql.ploomber.io/) which allows you to run SQL and plot large datasets in Jupyter via a %sql, %%sql, and %sqlplot magics.

We will use Jupysql on top of Jupyter to run queries on top of StarRocks.
Once the data is loaded into the cluster, we'll query & visualize it via SQL plotting.

## Prerequisites

Before getting started, you must have the following software installed locally:

- [Jupysql](https://jupysql.ploomber.io/en/latest/quick-start.html): `pip install jupysql`
- Jupyterlab: `pip install jupyterlab`
- [SKlearn Evaluation](https://github.com/ploomber/sklearn-evaluation): `pip install sklearn-evaluation`
- Python
- pymysql: `pip install pymysql`


> **NOTE**
>
> Once we have our requirements we can open jupyter lab simply by calling `jupyterlab` - this will open the notebook interface.
> If already running in a notebook, you can simply run the cell bellow to get the dependencies.

```python
# Install required packages
%pip install --quiet jupysql sklearn-evaluation pymysql
```

* Note: you may need to restart the kernel to use updated packages.

```python
import pandas as pd
from sklearn_evaluation import plot

# Import jupysql Jupyter extension to create SQL cells
%load_ext sql
%config SqlMagic.autocommit=False
```

**You'd need to make sure your StarRocks instance is up and reachable for the next stages.**

**Note:** you will need to adjust the connection string according to the instance type you're trying to connect to (url, user, password). In the example below we've used a local instance. To learn more about it, check out the [introduction guide](https://docs.starrocks.io/en-us/latest/introduction/StarRocks_intro).

## Connecting to StarRocks via Jupysql
In this example, we've used a docker instance and that's reflecting the data in the connection string.
We'll use root to connect to our local StarRocks instance.
We can then create a database and check we can actually read and write data into it.

```python
%sql mysql+pymysql://root:@localhost:9030
```

And now we create and use that jupysql database:

```python
%sql CREATE DATABASE jupysql;
```

```python
%sql USE jupysql;
```

We can then create the table:

```python
%%sql
CREATE TABLE tbl(c1 int, c2 int) distributed by hash(c1) properties ("replication_num" = "1");
INSERT INTO tbl VALUES (1, 10), (2, 20), (3, 30);
SELECT * FROM tbl;
```


## Saving and loading queries
Now once we've created a database, we can write some sample data into it and query it.
JupySQL allows you to break queries into multiple cells, simplifying the process of building large queries.
It allows us to write complex queries, save them and execute them when needed, in a similar manner to CTEs in SQL.

```python
# This is pending for the next Jupysql release
%%sql --save initialize-table --no-execute
CREATE TABLE tbl(c1 int, c2 int) distributed by hash(c1) properties ("replication_num" = "1");
INSERT INTO tbl VALUES (1, 1), (2, 2), (3, 3);
SELECT * FROM tbl;
```

* Note: `--save` stores the query, not the data

* Note that we are using --with; this will retrieve previously saved queries, and preprend them (using CTEs), then, we save the query in track_fav .


## Plotting directly on StarRocks

Jupysql comes with a few plots by default, allowing the user to visualize the data directly in SQL.
We can use a bar plot to visualize the data in our newly created table:

```python
top_artist = %sql SELECT * FROM tbl
top_artist.bar()
```

We can note we now have a new bar plot without any extra code.

We hope you enjoyed this guide, now you can run SQL directly from your notebook via Jupysql (by ploomber).
This adds lots of possibilities around StarRocks for Data scientists and Engineers.
In case you got stuck or need any support, please reach out to us via slack. 