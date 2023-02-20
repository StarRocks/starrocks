# Deploy StarRocks in Docker

This document is used to deploy the simplest StarRocks cluster, which contains one FE and one BE.

## Prerequisites

1. docker
2. mysql client

## Step 1: Get docker-compose file

You need to download the docker compose yaml file of starrocks.

```sh
wget https://raw.githubusercontent.com/StarRocks/starrocks/main/docker/docker-compose/docker-compose.yml
```

## Step 2: Run StarRocks cluster

You should run the following command

```sh
docker-compose up -d
```

Then you can check the containers status.

## Step 3: Connect to StarRocks

Connect to StarRocks cluster

```sh
mysql -h 127.0.0.1 -P9030 -u root
```

## Step4: Create table, Ingest data, Query

```SQL
CREATE DATABASE test;
USE test;
CREATE TABLE tbl(c1 int, c2 int) distributed by hash(c1) properties ("replication_num" = "1");
INSERT INTO tbl VALUES (1, 1), (2, 2), (3, 3);
SELECT * FROM tbl;
```
