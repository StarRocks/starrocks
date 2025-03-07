---
displayed_sidebar: docs
---

# SUSPEND または RESUME PIPE

## 説明

パイプを一時停止または再開します:

- ロードジョブが進行中（つまり、`RUNNING` 状態）の場合、ジョブのパイプを一時停止（`SUSPEND`）すると、ジョブが中断されます。
- ロードジョブでエラーが発生した場合、ジョブのパイプを再開（`RESUME`）すると、エラーのあるジョブが続行されます。

このコマンドは v3.2 以降でサポートされています。

## 構文

```SQL
ALTER PIPE <pipe_name> { SUSPEND | RESUME [ IF SUSPENDED ] }
```

## パラメータ

### pipe_name

パイプの名前。

## 例

### パイプを一時停止する

データベース `mydatabase` にある `user_behavior_replica` という名前のパイプ（`RUNNING` 状態）を一時停止します:

```SQL
USE mydatabase;
ALTER PIPE user_behavior_replica SUSPEND;
```

[SHOW PIPES](SHOW_PIPES.md) を使用してパイプをクエリすると、その状態が `SUSPEND` に変わったことが確認できます。

### パイプを再開する

データベース `mydatabase` にある `user_behavior_replica` という名前のパイプを再開します:

```SQL
USE mydatabase;
ALTER PIPE user_behavior_replica RESUME;
```

[SHOW PIPES](SHOW_PIPES.md) を使用してパイプをクエリすると、その状態が `RUNNING` に変わったことが確認できます。

## 参考

- [CREATE PIPE](CREATE_PIPE.md)
- [ALTER PIPE](ALTER_PIPE.md)
- [DROP PIPE](DROP_PIPE.md)
- [SHOW PIPES](SHOW_PIPES.md)
- [RETRY FILE](RETRY_FILE.md)