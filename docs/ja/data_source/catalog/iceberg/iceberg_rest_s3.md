---
displayed_sidebar: docs
---

# AWS S3 テーブル用の Iceberg REST Catalog の作成

この記事では、AWS Glue Iceberg REST エンドポイントを通じて AWS S3 テーブルのデータにアクセスするために、StarRocks で Iceberg REST Catalog を作成する方法を説明します。

AWS Glue Iceberg REST エンドポイントは、[Iceberg REST Catalog Open API 仕様](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml)を実装しており、Iceberg テーブルとやり取りするための標準化されたインターフェースを提供します。このエンドポイントを使用して S3 のテーブルにアクセスするには、IAM ポリシーと AWS Lake Formation の認可を組み合わせて資格情報を設定する必要があります。以下のセクションでは、必要なポリシーの定義、データベースおよびテーブルレベルでの Lake Formation の権限の確立、StarRocks を使用して S3 テーブルにアクセスするための Iceberg REST catalog の作成について説明します。

## （オプション）テーブルバケットの作成

Iceberg テーブル用のテーブルバケットが既にある場合は、このステップをスキップできます。

1. 管理者権限を持つユーザーとして [Amazon S3 コンソール](https://console.aws.amazon.com/s3) にサインインします。
2. ページの右上隅で、AWS リージョンを選択します。
3. 左側のナビゲーションペインで、ナビゲーションパネルから **Table buckets** を選択します。
4. **Create table bucket** をクリックして、テーブルバケットを作成します。
5. テーブルバケットを作成した後、それを選択し、**Create table with Athena** をクリックします。
6. 名前空間を作成します。
7. 名前空間を作成した後、再度 **Create table with Athena** をクリックしてテーブルを作成します。

> **NOTE**
> 
> Athena を使用してデータベースとテーブルを作成し、それを StarRocks でクエリすることができます。あるいは、テーブルバケットを作成し、StarRocks を使用してデータベースとテーブルを作成することもできます。

## IAM ポリシーの作成

AWS Glue エンドポイントを介してテーブルにアクセスするには、AWS Glue および Lake Formation の操作に対する権限を持つ IAM ポリシーを作成します。

1. 管理者権限を持つユーザーとして [Amazon IAM コンソール](https://console.aws.amazon.com/iam) にサインインします。
2. ページの右上隅で、AWS リージョンを選択します。
3. 左側のナビゲーションペインで、ナビゲーションパネルから **Policies** を選択します。
4. **Create a policy** を選択し、ポリシーエディタで **JSON** を選択します。
5. AWS Glue および Lake Formation のアクションへのアクセスを許可するために、以下のポリシーを追加します。

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "VisualEditor0",
            "Effect": "Allow",
            "Action": [
                "glue:GetCatalog",
                "glue:GetDatabase",
                "glue:GetDatabases",
                "glue:GetTable",
                "glue:GetTables",
                "glue:CreateTable",
                "glue:UpdateTable"
            ],
            "Resource": [
                "arn:aws:glue:<region>:<account-id>:catalog",
                "arn:aws:glue:<region>:<account-id>:catalog/s3tablescatalog",
                "arn:aws:glue:<region>:<account-id>:catalog/s3tablescatalog/<s3_table_bucket_name>",
                "arn:aws:glue:<region>:<account-id>:table/s3tablescatalog/<s3_table_bucket_name>/<namespace>/*",
                "arn:aws:glue:<region>:<account-id>:database/s3tablescatalog/<s3_table_bucket_name>/<namespace>"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "lakeformation:GetDataAccess"
            ],
            "Resource": "*"
        }
    ]
}
```

IAM ポリシーを作成した後、ターゲットの IAM ユーザーにアタッチします。

1. ナビゲーションパネルから **Users** を選択します。
2. S3 テーブルアクセスを必要とするユーザーを選択します。
3. **Add permissions** をクリックし、**Attach policies directly** を選択します。
4. 新しく作成したポリシーをアタッチします。

## Lake Formation を介した権限の管理

S3 テーブルにアクセスするには、StarRocks が Lake Formation を使用して、サードパーティのクエリエンジンが S3 テーブルにアクセスできるようにする権限を設定する必要があります。

1. 管理者権限を持つユーザーとして [Lake Formation コンソール](https://console.aws.amazon.com/lakeformation) にサインインします。
2. ページの右上隅で、AWS リージョンを選択します。
3. 左側のナビゲーションペインで、ナビゲーションパネルから **Application integration settings** を選択します。
4. **Allow external engines to access data in Amazon S3 locations with full table access.** を選択します。

次に、上記の IAM ユーザーに Lake Formation でアクセス権限を付与します。

1. [Lake Formation コンソール](https://console.aws.amazon.com/lakeformation) の左側のナビゲーションペインで、ナビゲーションパネルから **Data permissions** を選択します。
2. **Grant** をクリックします。
3. **Principals** セクションで **IAM users and roles** を選択し、**IAM users and roles** ドロップダウンリストから認可された IAM ユーザーを選択します。
4. **LF-Tags or catalog resources** セクションで **Named Data Catalog resources** を選択し、**Catalogs** ドロップダウンリストから作成したテーブルバケットを選択します。
5. **Catalog permissions** セクションで **Catalog permissions** に対して **Super** を選択します。
6. **Grant** をクリックします。

> **NOTE**
>
> ここでは、テストの便宜上、Super 権限が付与されています。実際の運用環境では、適切な権限を割り当てる必要があります。

## Iceberg REST Catalog の作成

StarRocks で Iceberg REST catalog を作成します。

```SQL
CREATE EXTERNAL CATALOG starrocks_lakehouse_s3tables PROPERTIES(
  "type"="iceberg",
  "iceberg.catalog.type" = "rest",
  "iceberg.catalog.uri"  = "https://glue.<region>.amazonaws.com/iceberg",
  "iceberg.catalog.rest.sigv4-enabled" = "true",
  "iceberg.catalog.rest.signing-name" = "glue",
  "iceberg.catalog.rest.access-key-id" = "<iam_user_access_key>",
  "iceberg.catalog.rest.secret-access-key" = "<iam_user_secret_key>",
  "iceberg.catalog.warehouse" = "<accountid>:s3tablescatalog/<table-bucket-name>",
  "aws.s3.region" = "<region>"
);
```

その後、データベースとテーブルを作成し、クエリを実行できます。

例:

```SQL
-- カタログを切り替える
StarRocks> SET CATALOG starrocks_lakehouse_s3tables;
-- データベースを作成
StarRocks> CREATE DATABASE s3table_db;
Query OK, 0 rows affected

-- データベースを切り替える
StarRocks> USE s3table_db;
Database changed

-- テーブルを作成
StarRocks> CREATE TABLE taxis (
    trip_id BIGINT,
    trip_distance FLOAT,
    fare_amount DOUBLE,
    store_and_fwd_flag STRING,
    vendor_id BIGINT
) PARTITION BY (vendor_id);
Query OK, 0 rows affected 

-- データを挿入
StarRocks> INSERT INTO taxis 
VALUES (1000371, 1.8, 15.32, 'N', 1), 
       (1000372, 2.5, 22.15, 'N', 2),
       (1000373, 0.9, 9.01, 'N', 2),
       (1000374, 8.4, 42.13, 'Y', 1);
Query OK, 4 rows affected

-- データをクエリ
StarRocks> SELECT * FROM taxis;
+---------+---------------+-------------+--------------------+-----------+
| trip_id | trip_distance | fare_amount | store_and_fwd_flag | vendor_id |
+---------+---------------+-------------+--------------------+-----------+
| 1000372 |           2.5 |       22.15 | N                  |         2 |
| 1000373 |           0.9 |        9.01 | N                  |         2 |
| 1000371 |           1.8 |       15.32 | N                  |         1 |
| 1000374 |           8.4 |       42.13 | Y                  |         1 |
+---------+---------------+-------------+--------------------+-----------+
4 rows in set
```
