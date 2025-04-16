---
displayed_sidebar: docs
---

# ALTER PIPE

## 説明

パイプのプロパティ設定を変更します。このコマンドは v3.2 以降でサポートされています。

## 構文

```SQL
ALTER PIPE [db_name.]<pipe_name> 
SET PROPERTY
(
    "<key>" = <value>[, "<key>" = "<value>" ...]
) 
```

## パラメータ

### db_name

パイプが属するデータベースの名前。

### pipe_name

パイプの名前。

### PROPERTIES

パイプの設定を変更したいプロパティ。フォーマット: `"key" = "value"`。サポートされているプロパティの詳細については、[CREATE PIPE](CREATE_PIPE.md) を参照してください。

## 例

データベース `mydatabase` にあるパイプ `user_behavior_replica` の `AUTO_INGEST` プロパティを `FALSE` に変更します。

```SQL
USE mydatabase;
ALTER PIPE user_behavior_replica
SET
(
    "AUTO_INGEST" = "FALSE"
);
```

## 参考

- [CREATE PIPE](CREATE_PIPE.md)
- [DROP PIPE](DROP_PIPE.md)
- [SHOW PIPES](SHOW_PIPES.md)
- [SUSPEND or RESUME PIPE](SUSPEND_or_RESUME_PIPE.md)
- [RETRY FILE](RETRY_FILE.md)