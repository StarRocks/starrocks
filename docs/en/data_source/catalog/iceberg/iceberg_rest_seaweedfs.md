---
displayed_sidebar: docs
description: "How to create an Iceberg REST Catalog in StarRocks for accessing SeaweedFS table buckets."
---

# Create Iceberg REST Catalog for SeaweedFS

This article explains how to create an Iceberg REST Catalog in StarRocks for access to data in [SeaweedFS](https://github.com/seaweedfs/seaweedfs) table buckets.

SeaweedFS is an open-source distributed object store with an S3-compatible gateway. Its table buckets provide both halves of an Iceberg deployment: the embedded Iceberg REST catalog serves the table metadata, and the table bucket stores the table data as Parquet files behind the same S3 gateway. Catalog requests are authenticated with AWS SigV4 signing using the same access key and secret key as the S3 gateway.

## (Optional) Run SeaweedFS locally

You can skip this step if you already have a SeaweedFS cluster with a table bucket.

Create a file `s3config.json` with the S3 credentials:

```json
{
  "identities": [
    {
      "name": "analyst",
      "credentials": [
        {
          "accessKey": "your_access_key",
          "secretKey": "your_secret_key"
        }
      ],
      "actions": ["Admin", "Read", "Write", "List", "Tagging"]
    }
  ]
}
```

Then start the whole SeaweedFS stack in one container. The `-tableBucket` flag pre-creates a table bucket named `analytics` that serves as the Iceberg warehouse:

```bash
docker run -d --name seaweedfs -p 8333:8333 -p 8181:8181 \
    -v "$(pwd)/s3config.json:/etc/seaweedfs/s3config.json" \
    chrislusf/seaweedfs:latest \
    mini -dir=/data -s3.config=/etc/seaweedfs/s3config.json -tableBucket=analytics
```

The S3 endpoint listens on port 8333 and the Iceberg REST catalog on port 8181.

## Create Iceberg REST Catalog

Create an Iceberg REST catalog in StarRocks:

```SQL
CREATE EXTERNAL CATALOG seaweedfs_catalog PROPERTIES(
  "type" = "iceberg",
  "iceberg.catalog.type" = "rest",
  "iceberg.catalog.uri" = "http://<seaweedfs_host>:8181",
  "iceberg.catalog.rest.sigv4-enabled" = "true",
  "iceberg.catalog.rest.signing-name" = "s3",
  "iceberg.catalog.rest.signing-region" = "us-east-1",
  "iceberg.catalog.rest.access-key-id" = "<your_access_key>",
  "iceberg.catalog.rest.secret-access-key" = "<your_secret_key>",
  "iceberg.catalog.warehouse" = "s3://<table_bucket_name>",
  "aws.s3.region" = "us-east-1",
  "aws.s3.endpoint" = "http://<seaweedfs_host>:8333",
  "aws.s3.access_key" = "<your_access_key>",
  "aws.s3.secret_key" = "<your_secret_key>",
  "aws.s3.enable_path_style_access" = "true"
);
```

> **NOTE**
>
> - `iceberg.catalog.rest.signing-region` is required to initialize the SigV4 signer. SeaweedFS does not validate the region value, so any region works as long as the same value is used consistently.
> - SeaweedFS serves path-style requests, so `aws.s3.enable_path_style_access` must be `true`.

You can then create databases and tables and run queries in it.

Example:

```SQL
-- Switch to the catalog
StarRocks> SET CATALOG seaweedfs_catalog;

-- Create database
StarRocks> CREATE DATABASE sales_db;
Query OK, 0 rows affected

-- Switch database
StarRocks> USE sales_db;
Database changed

-- Create table
StarRocks> CREATE TABLE orders (id BIGINT, region STRING, amount DOUBLE);
Query OK, 0 rows affected

-- Insert data
StarRocks> INSERT INTO orders VALUES (1, 'NA', 12.5), (2, 'EU', 40.0);
Query OK, 2 rows affected

-- Query data
StarRocks> SELECT * FROM orders ORDER BY id;
+------+--------+--------+
| id   | region | amount |
+------+--------+--------+
|    1 | NA     |   12.5 |
|    2 | EU     |   40.0 |
+------+--------+--------+
2 rows in set
```

Tables created through the catalog are standard Iceberg tables: other engines can read and write them through the same SeaweedFS REST catalog and S3 endpoint.
