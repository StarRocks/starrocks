# StarRocks Docker Images

## What is Starrocks?

StarRocks is a next-gen, high-performance analytical data warehouse that enables real-time, multi-dimensional, and highly concurrent data analysis. StarRocks has an MPP architecture and is equipped with a fully vectorized execution engine, a columnar storage engine that supports real-time updates, and is powered by a rich set of features including a fully-customized cost-based optimizer (CBO), intelligent materialized view, and more. StarRocks supports real-time and batch data ingestion from a variety of data sources. It also allows you to directly analyze data stored in data lakes with zero data migration.

For more information see https://docs.starrocks.io/

### Architecture

StarRocks consists of frontend (FE), backend (BE), and when using object storage compute (CN), nodes. 

#### FE

FEs are responsible for metadata management, client connection management, query planning, and query scheduling. Each FE stores and maintains a complete copy of metadata in its memory, which guarantees indiscriminate services among the FEs. FEs can work as the leader, followers, and observers. Followers can elect a leader according to the Paxos-like BDB JE protocol. BDB JE is short for Berkeley DB Java Edition.

#### BE

BEs are responsible for data storage and SQL execution.

##### Data storage
BEs have data storage capabilities. FEs distribute data to BEs based on predefined rules. BEs transform the ingested data, write the data into the required format, and generate indexes for the data.

##### SQL execution
When an SQL query arrives, FEs parse it into a logical execution plan according to the semantics of the query, and then transform the logical plan into physical execution plans that can be executed on BEs. BEs that store the destination data execute the query. This eliminates the need for data transmission and copy, achieving high query performance.

#### CN

CNs are stateless BEs; data is stored in object storage instead of local storage. CN nodes are responsible for tasks such as data loading, query computation, and cache management.

[Architecture docs](https://docs.starrocks.io/docs/introduction/Architecture/)

## How to use this image

For each of the use-cases listed there are Quick Starts (step-by-step tutorials).

### Allin1

Use the [StarRocks basics](https://docs.starrocks.io/docs/quick_start/shared-nothing/) to deploy a single container containing both a StarRocks frontend and backend, load some data, and analyze the data.

### Separate storage and compute

Follow the shared-data [Quick Start tutorial](https://docs.starrocks.io/docs/quick_start/shared-data/) to deploy StarRocks and MinIO. You can modify the provided Docker Compose file to switch to S3, GCS, Azure, or other object storage.

[Compose file](https://github.com/StarRocks/demo/blob/master/documentation-samples/quickstart/docker-compose.yml)

### Helm/Operator

Deploy using the StarRocks Helm chart and Kubernetes operator:

[Quick Start tutorial](https://docs.starrocks.io/docs/quick_start/helm/)

[Operator repo](https://github.com/StarRocks/starrocks-kubernetes-operator/blob/main/examples/starrocks/README.md) with more examples

## Connecting to StarRocks with a MySQL client

The default port used to connect to StarRocks using the MySQL protocol is `9030`. The provided Docker Compose files will expose port 9030 using notation similar to:

```yaml
    ports:
      - "8030:8030"
      - "9020:9020"
      - "9030:9030"
```

If you are running the containers from the command line, expose the port with something like:

```bash
docker run -p 9030:9030 ...
```

If you have exposed port 9030, connect:

```bash
mysql -P9030 -h 127.0.0.1 -u root --prompt="StarRocks > " -p
```

If you have not configured a password for `root` just hit enter when prompted.

You can also use the MySQL client provided in the container:

```bash
docker compose exec starrocks-fe \
mysql -P9030 -h127.0.0.1 -uroot --prompt="StarRocks > "
```

Or:

```bash
docker exec -ti starrocks-fe mysql -P9030 -h127.0.0.1 -uroot --prompt="StarRocks > " -p
```

> Tip
>
> Substitute you service or container name for `starrocks-fe` in the exec commands

### DBeaver and other clients

You can use other clients that support the MySQL protocol, just use port 9030 in place of the default MySQL port.
