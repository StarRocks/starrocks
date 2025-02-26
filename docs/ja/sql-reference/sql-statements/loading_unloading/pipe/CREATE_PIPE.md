---
displayed_sidebar: docs
---

# CREATE PIPE

## 説明

指定されたソースデータファイルから宛先テーブルにデータをロードするために使用される INSERT INTO SELECT FROM FILES ステートメントを定義する新しいパイプを作成します。このコマンドは v3.2 以降でサポートされています。

## 構文

```SQL
CREATE [OR REPLACE] PIPE [db_name.]<pipe_name> 
[PROPERTIES ("<key>" = "<value>"[, "<key> = <value>" ...])]
AS <INSERT_SQL>
```

StarRocks は v3.2.3 以降で CREATE [OR REPLACE] PIPE をサポートしています。CREATE [OR REPLACE] PIPE を使用してパイプを作成する際に、`pipe_name` で指定されたパイプ名が現在のデータベース内の既存のパイプ名と同じ場合、既存のデータベースは新しいパイプによって置き換えられます。

## パラメータ

### db_name

パイプが属するデータベースの一意の名前。

> **注意**
>
> 各パイプは特定のデータベースに属します。パイプが属するデータベースを削除すると、そのパイプもデータベースと共に削除され、データベースを復元してもパイプは復元されません。

### pipe_name

パイプの名前。パイプ名は、パイプが作成されるデータベース内で一意でなければなりません。命名規則については、[System limits](../../../System_limit.md) を参照してください。

### INSERT_SQL

指定されたソースデータファイルから宛先テーブルにデータをロードするために使用される INSERT INTO SELECT FROM FILES ステートメント。

FILES() テーブル関数の詳細については、[FILES](../../../sql-functions/table-functions/files.md) を参照してください。

### PROPERTIES

パイプを実行する方法を指定するオプションのパラメータセット。形式: `"key" = "value"`。

| プロパティ      | デフォルト値 | 説明                                                  |
| :------------ | :------------ | :----------------------------------------------------------- |
| AUTO_INGEST   | `TRUE`        | 自動インクリメンタルデータロードを有効にするかどうか。 有効な値: `TRUE` と `FALSE`。このパラメータを `TRUE` に設定すると、自動インクリメンタルデータロードが有効になります。このパラメータを `FALSE` に設定すると、ジョブ作成時に指定されたソースデータファイルの内容のみがロードされ、その後の新規または更新されたファイル内容はロードされません。バルクロードの場合、このパラメータを `FALSE` に設定できます。 |
| POLL_INTERVAL | `300` 秒     | 自動インクリメンタルデータロードのポーリング間隔。   |
| BATCH_SIZE    | `1GB`         | バッチとしてロードされるデータのサイズ。このパラメータ値に単位を含めない場合、デフォルトの単位バイトが使用されます。 |
| BATCH_FILES   | `256`         | バッチとしてロードされるソースデータファイルの数。     |

## 例

サンプルデータセット `s3://starrocks-examples/user_behavior_ten_million_rows.parquet` のデータを `user_behavior_replica` テーブルにロードするために、現在のデータベースに `user_behavior_replica` という名前のパイプを作成します。

```SQL
CREATE PIPE user_behavior_replica
PROPERTIES
(
    "AUTO_INGEST" = "TRUE"
)
AS
INSERT INTO user_behavior_replica
SELECT * FROM FILES
(
    "path" = "s3://starrocks-examples/user_behavior_ten_million_rows.parquet",
    "format" = "parquet",
    "aws.s3.region" = "us-east-1",
    "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
    "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"
); 
```

> **注意**
>
> 上記のコマンドで `AAA` と `BBB` にあなたの認証情報を置き換えてください。任意の有効な `aws.s3.access_key` と `aws.s3.secret_key` を使用できます。オブジェクトは任意の AWS 認証済みユーザーによって読み取り可能です。

この例では、IAM ユーザー認証方法と StarRocks テーブルと同じスキーマを持つ Parquet ファイルを使用しています。他の認証方法や CREATE PIPE の使用法についての詳細は、[Authenticate to AWS resources](../../../../integrations/authenticate_to_aws_resources.md) および [FILES](../../../sql-functions/table-functions/files.md) を参照してください。

## 参考文献

- [ALTER PIPE](ALTER_PIPE.md)
- [DROP PIPE](DROP_PIPE.md)
- [SHOW PIPES](SHOW_PIPES.md)
- [SUSPEND or RESUME PIPE](SUSPEND_or_RESUME_PIPE.md)
- [RETRY FILE](RETRY_FILE.md)