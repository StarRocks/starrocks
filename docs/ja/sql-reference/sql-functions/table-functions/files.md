---
displayed_sidebar: docs
---

# FILES

## 説明

クラウドストレージからデータファイルを読み取ります。FILES() はファイルのパス関連プロパティを使用してクラウドストレージにアクセスし、ファイル内のデータのテーブルスキーマを推測してデータ行を返します。[SELECT](../../sql-statements/table_bucket_part_index/SELECT.md) を使用してデータ行を直接クエリしたり、[INSERT](../../sql-statements/loading_unloading/INSERT.md) を使用して既存のテーブルにデータ行をロードしたり、[CREATE TABLE AS SELECT](../../sql-statements/table_bucket_part_index/CREATE_TABLE_AS_SELECT.md) を使用して新しいテーブルを作成してデータ行をロードしたりできます。この機能は v3.1.0 以降でサポートされています。

現在、FILES() 関数は以下のデータソースとファイル形式をサポートしています。

- **データソース:**

  - AWS S3

- **ファイル形式:**

  - Parquet
  - ORC

## 構文

```SQL
FILES( data_location , data_format [, StorageCredentialParams ] )

data_location ::=
    "path" = "s3://<s3_path>"

data_format ::=
    "format" = "{parquet | orc}"
```

## パラメータ

すべてのパラメータは `"key" = "value"` のペアで指定します。

| **キー** | **必須** | **説明**                                              |
| ------- | ------------ | ------------------------------------------------------------ |
| path    | はい          | ファイルにアクセスするために使用される URI。例: `s3://testbucket/parquet/test.parquet`。 |
| format  | はい          | データファイルの形式。有効な値: `parquet` と `orc`。 |

### StorageCredentialParams

StarRocks がストレージシステムにアクセスするために使用する認証情報。

- IAM ユーザーに基づく認証を使用して AWS S3 にアクセスします:

  ```SQL
  "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
  "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
  "aws.s3.region" = "<s3_region>"
  ```

  | **キー**           | **必須** | **説明**                                              |
  | ----------------- | ------------ | ------------------------------------------------------------ |
  | aws.s3.access_key | はい          | Amazon S3 バケットにアクセスするために使用できるアクセスキー ID。 |
  | aws.s3.secret_key | はい          | Amazon S3 バケットにアクセスするために使用できるシークレットアクセスキー。 |
  | aws.s3.region     | はい          | AWS S3 バケットが存在するリージョン。例: `us-west-2`。 |

## 戻り値

SELECT と共に使用すると、FILES() はファイル内のデータをテーブルとして返します。

- Parquet または ORC ファイルをクエリする際、SELECT 文で必要な列の名前を直接指定するか、`*` を指定してすべての列からデータを取得できます。

  ```SQL
  SELECT * FROM FILES(
      "path" = "s3://inserttest/parquet/file2.parquet",
      "format" = "parquet",
      "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
      "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
      "aws.s3.region" = "us-west-2"
  )
  WHERE c1 IN (101,105);
  +------+------+---------------------+
  | c1   | c2   | c3                  |
  +------+------+---------------------+
  |  101 |    9 | 2018-05-15T18:30:00 |
  |  105 |    6 | 2018-05-15T18:30:00 |
  +------+------+---------------------+
  2 rows in set (0.29 sec)

  SELECT c1, c3 FROM FILES(
      "path" = "s3://inserttest/parquet/file2.parquet",
      "format" = "parquet",
      "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
      "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
      "aws.s3.region" = "us-west-2"
  );
  +------+---------------------+
  | c1   | c3                  |
  +------+---------------------+
  |  101 | 2018-05-15T18:30:00 |
  |  102 | 2018-05-15T18:30:00 |
  |  103 | 2018-05-15T18:30:00 |
  |  104 | 2018-05-15T18:30:00 |
  |  105 | 2018-05-15T18:30:00 |
  |  106 | 2018-05-15T18:30:00 |
  |  107 | 2018-05-15T18:30:00 |
  |  108 | 2018-05-15T18:30:00 |
  |  109 | 2018-05-15T18:30:00 |
  |  110 | 2018-05-15T18:30:00 |
  +------+---------------------+
  10 rows in set (0.55 sec)
  ```

## 例

例 1: AWS S3 バケット `inserttest` 内の Parquet ファイル **parquet/par-dup.parquet** からデータをクエリします:

```Plain
MySQL > SELECT * FROM FILES(
     "path" = "s3://inserttest/parquet/par-dup.parquet",
     "format" = "parquet",
     "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
     "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
     "aws.s3.region" = "us-west-2"
);
+------+---------------------------------------------------------+
| c1   | c2                                                      |
+------+---------------------------------------------------------+
|    1 | {"1": "key", "1": "1", "111": "1111", "111": "aaaa"}    |
|    2 | {"2": "key", "2": "NULL", "222": "2222", "222": "bbbb"} |
+------+---------------------------------------------------------+
2 rows in set (22.335 sec)
```

例 2: AWS S3 バケット `inserttest` 内の Parquet ファイル **parquet/insert_wiki_edit_append.parquet** からテーブル `insert_wiki_edit` にデータ行を挿入します:

```Plain
MySQL > INSERT INTO insert_wiki_edit
    SELECT * FROM FILES(
        "path" = "s3://inserttest/parquet/insert_wiki_edit_append.parquet",
        "format" = "parquet",
        "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
        "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
        "aws.s3.region" = "us-west-2"
);
Query OK, 2 rows affected (23.03 sec)
{'label':'insert_d8d4b2ee-ac5c-11ed-a2cf-4e1110a8f63b', 'status':'VISIBLE', 'txnId':'2440'}
```

例 3: テーブル `ctas_wiki_edit` を作成し、AWS S3 バケット `inserttest` 内の Parquet ファイル **parquet/insert_wiki_edit_append.parquet** からデータ行をテーブルに挿入します:

```Plain
MySQL > CREATE TABLE ctas_wiki_edit AS
    SELECT * FROM FILES(
        "path" = "s3://inserttest/parquet/insert_wiki_edit_append.parquet",
        "format" = "parquet",
        "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
        "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
        "aws.s3.region" = "us-west-2"
);
Query OK, 2 rows affected (22.09 sec)
{'label':'insert_1a217d70-2f52-11ee-9e4a-7a563fb695da', 'status':'VISIBLE', 'txnId':'3248'}
```