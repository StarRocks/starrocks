---
description: "Apache Iceberg を使用したデータレイクハウス"
displayed_sidebar: docs
---

import DataLakeIntro from '../_assets/commonMarkdown/datalakeIntro.mdx'
import Clients from '../_assets/quick-start/_clientsCompose.mdx'

# Apache Iceberg Lakehouse

このガイドでは、StarRocks™ を使用して Apache Iceberg™ を始める方法を説明し、いくつかの強力な機能を示すサンプルコードを提供します。

### Docker-Compose

最も簡単に始める方法は、`starrocks/fe-ubuntu` と `starrocks/be-ubuntu` イメージを使用する docker-compose ファイルを使うことです。これには、Iceberg catalog が設定されたローカル StarRocks クラスターが含まれています。これを使用するには、Docker CLI をインストールする必要があります。

Docker をインストールしたら、以下の yaml を docker-compose.yml という名前のファイルに保存してください。

```yml
services:

  starrocks-fe:
    image: starrocks/fe-ubuntu:3.3.5
    hostname: starrocks-fe
    container_name: starrocks-fe
    user: root
    command: |
      bash /opt/starrocks/fe/bin/start_fe.sh --host_type FQDN
    ports:
      - 8030:8030
      - 9020:9020
      - 9030:9030
    networks:
      iceberg_net:
    environment:
      - AWS_ACCESS_KEY_ID=admin
      - AWS_SECRET_ACCESS_KEY=password
      - AWS_REGION=us-east-1
    healthcheck:
      test: 'mysql -u root -h starrocks-fe -P 9030 -e "SHOW FRONTENDS\G" |grep "Alive: true"'
      interval: 10s
      timeout: 5s
      retries: 3

  starrocks-be:
    image: starrocks/be-ubuntu:3.3.5
    command:
      - /bin/bash
      - -c
      - |
        ulimit -n 65535;
        echo "# Enable data cache"  >> /opt/starrocks/be/conf/be.conf
        echo "block_cache_enable = true"  >> /opt/starrocks/be/conf/be.conf
        echo "block_cache_mem_size = 536870912" >> /opt/starrocks/be/conf/be.conf
        echo "block_cache_disk_size = 1073741824" >> /opt/starrocks/be/conf/be.conf
        sleep 15s
        mysql --connect-timeout 2 -h starrocks-fe -P 9030 -u root -e "ALTER SYSTEM ADD BACKEND \"starrocks-be:9050\";"
        bash /opt/starrocks/be/bin/start_be.sh
    ports:
      - 8040:8040
    hostname: starrocks-be
    container_name: starrocks-be
    user: root
    depends_on:
      starrocks-fe:
        condition: service_healthy
    healthcheck:
      test: 'mysql -u root -h starrocks-fe -P 9030 -e "SHOW BACKENDS\G" |grep "Alive: true"'
      interval: 10s
      timeout: 5s
      retries: 3
    networks:
      iceberg_net:
    environment:
      - HOST_TYPE=FQDN

  rest:
    image: apache/iceberg-rest-fixture
    container_name: iceberg-rest
    networks:
      iceberg_net:
        aliases:
          - iceberg-rest.minio
    ports:
      - 8181:8181
    environment:
      - AWS_ACCESS_KEY_ID=admin
      - AWS_SECRET_ACCESS_KEY=password
      - AWS_REGION=us-east-1
      - CATALOG_WAREHOUSE=s3://warehouse/
      - CATALOG_IO__IMPL=org.apache.iceberg.aws.s3.S3FileIO
      - CATALOG_S3_ENDPOINT=http://minio:9000

  minio:
    image: minio/minio:RELEASE.2024-10-29T16-01-48Z
    container_name: minio
    environment:
      - MINIO_ROOT_USER=admin
      - MINIO_ROOT_PASSWORD=password
      - MINIO_DOMAIN=minio
    networks:
      iceberg_net:
        aliases:
          - warehouse.minio
    ports:
      - 9001:9001
      - 9000:9000
    command: ["server", "/data", "--console-address", ":9001"]
  mc:
    depends_on:
      - minio
    image: minio/mc:RELEASE.2024-10-29T15-34-59Z
    container_name: mc
    networks:
      iceberg_net:
    environment:
      - AWS_ACCESS_KEY_ID=admin
      - AWS_SECRET_ACCESS_KEY=password
      - AWS_REGION=us-east-1
    entrypoint: >
      /bin/sh -c "
      until (/usr/bin/mc config host add minio http://minio:9000 admin password) do echo '...waiting...' && sleep 1; done;
      /usr/bin/mc rm -r --force minio/warehouse;
      /usr/bin/mc mb minio/warehouse;
      /usr/bin/mc policy set public minio/warehouse;
      tail -f /dev/null
      "
networks:
  iceberg_net:
```

次に、このコマンドで docker コンテナを起動します。

```Plain
docker compose up --detach --wait --wait-timeout 400
```

その後、以下のコマンドを実行して StarRocks セッションを開始できます。

```bash
docker exec -it starrocks-fe \
mysql -P 9030 -h 127.0.0.1 -u root --prompt="StarRocks > "
```

### カタログの追加と使用

```SQL
CREATE EXTERNAL CATALOG 'demo'
COMMENT "External catalog to Apache Iceberg on MinIO"
PROPERTIES
(
  "type"="iceberg",
  "iceberg.catalog.type"="rest",
  "iceberg.catalog.uri"="http://iceberg-rest:8181",
  "iceberg.catalog.warehouse"="warehouse",
  "aws.s3.access_key"="admin",
  "aws.s3.secret_key"="password",
  "aws.s3.endpoint"="http://minio:9000",
  "aws.s3.enable_path_style_access"="true",
  "client.factory"="com.starrocks.connector.iceberg.IcebergAwsClientFactory"  
);
```

```SQL
SHOW CATALOGS\G
```

```SQL
*************************** 1. row ***************************
Catalog: default_catalog
   Type: Internal
Comment: An internal catalog contains this cluster's self-managed tables.
*************************** 2. row ***************************
Catalog: demo
   Type: Iceberg
Comment: External catalog to Apache Iceberg on MinIO
2 rows in set (0.00 sec)
```

```SQL
SET CATALOG demo;
```

### データベースの作成と使用

```SQL
CREATE DATABASE nyc;
```

```SQL
USE nyc;
```

### テーブルの作成

```SQL
CREATE TABLE demo.nyc.taxis
(
    trip_id            bigint,
    trip_distance      float,
    fare_amount double,
    store_and_fwd_flag string,
    vendor_id          bigint
) PARTITION BY (vendor_id);
```

### テーブルへのデータの書き込み

```SQL
INSERT INTO demo.nyc.taxis
VALUES (1000371, 1.8, 15.32, 'N', 1),
       (1000372, 2.5, 22.15, 'N', 2),
       (1000373, 0.9, 9.01, 'N', 2),
       (1000374, 8.4, 42.13, 'Y', 1);
```

### テーブルからのデータの読み取り

```SQL
SELECT *
FROM demo.nyc.taxis;
```

### データがオブジェクトストレージに保存されていることを確認

外部カタログを追加して使用したとき、StarRocks は `demo.nyc.taxis` テーブルのオブジェクトストアとして MinIO を使用し始めました。http://localhost:9001 にアクセスし、Object Browser メニューを通じて `warehouse/nyc/taxis/` に移動すると、StarRocks が MinIO をストレージとして使用していることを確認できます。

:::tip

MinIO のユーザー名とパスワードは docker-compose.yml ファイルにあります。チュートリアルのためにパスワードを変更するように求められますが、このアドバイスは無視してください。

![img](../_assets/quick-start/MinIO-Iceberg-data.png)
:::

### 次のステップ

#### StarRocks に Iceberg を追加

すでに StarRocks 3.2.0 以降の環境をお持ちの場合、Iceberg 1.6.0 が含まれています。追加のダウンロードや jar は必要ありません。

#### 詳しく学ぶ

Iceberg と StarRocks を使い始めた今、[StarRocks-Iceberg ドキュメント](../data_source/catalog/iceberg/iceberg_catalog.md)をチェックして、さらに学びましょう！
