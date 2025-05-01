---
displayed_sidebar: docs
---

# CREATE INDEX

## 説明

このステートメントはインデックスを作成するために使用されます。

構文:

```sql
CREATE INDEX index_name ON table_name (column [, ...],) [USING BITMAP] [COMMENT'balabala']
```

注意:

1. 現行バージョンではビットマップインデックスのみをサポートしています。
2. 単一のカラムにのみビットマップインデックスを作成します。

## 例

1. `table1` の `siteid` に対してビットマップインデックスを作成します。

    ```sql
    CREATE INDEX index_name ON table1 (siteid) USING BITMAP COMMENT 'balabala';
    ```