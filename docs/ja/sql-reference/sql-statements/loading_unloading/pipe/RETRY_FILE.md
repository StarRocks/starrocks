# RETRY FILE

## 説明

パイプ内のすべてのデータファイルまたは特定のデータファイルのロードを再試行します。このコマンドは v3.2 以降でサポートされています。

## 構文

```SQL
ALTER PIPE <pipe_name> { RETRY ALL | RETRY FILE '<file_name>' }
```

## パラメータ

### pipe_name

パイプの名前。

### file_name

再試行してロードしたいデータファイルのストレージパス。ファイルの完全なストレージパスを指定する必要があります。指定したファイルが `pipe_name` で指定したパイプに属していない場合、エラーが返されます。

## 例

次の例は、`user_behavior_replica` という名前のパイプ内のすべてのデータファイルのロードを再試行します。

```SQL
ALTER PIPE user_behavior_replica RETRY ALL;
```

次の例は、`user_behavior_replica` という名前のパイプ内のデータファイル `s3://starrocks-examples/user_behavior_ten_million_rows.parquet` のロードを再試行します。

```SQL
ALTER PIPE user_behavior_replica RETRY FILE 's3://starrocks-examples/user_behavior_ten_million_rows.parquet';
```

## 参照

- [CREATE PIPE](CREATE_PIPE.md)
- [ALTER PIPE](ALTER_PIPE.md)
- [DROP PIPE](DROP_PIPE.md)
- [SHOW PIPES](SHOW_PIPES.md)
- [SUSPEND or RESUME PIPE](SUSPEND_or_RESUME_PIPE.md)