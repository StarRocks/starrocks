---
displayed_sidebar: docs
---

# DROP PIPE

## Description

パイプと関連するジョブおよびメタデータを削除します。このステートメントをパイプに対して実行しても、このパイプを介してロードされたデータは取り消されません。このコマンドは v3.2 以降でサポートされています。

## Syntax

```SQL
DROP PIPE [IF EXISTS] [db_name.]<pipe_name>
```

## Parameters

### db_name

パイプが属するデータベースの名前。

### pipe_name

パイプの名前。

## Examples

データベース `mydatabase` にある `user_behavior_replica` という名前のパイプを削除します。

```SQL
USE mydatabase;
DROP PIPE user_behavior_replica;
```

## References

- [CREATE PIPE](CREATE_PIPE.md)
- [ALTER PIPE](ALTER_PIPE.md)
- [SHOW PIPES](SHOW_PIPES.md)
- [SUSPEND or RESUME PIPE](SUSPEND_or_RESUME_PIPE.md)
- [RETRY FILE](RETRY_FILE.md)