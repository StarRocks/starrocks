---
displayed_sidebar: docs
---

# current_timestamp

現在の日付を取得し、DATETIME 型の値を返します。

この関数は [now()](./now.md) 関数の同義語です。

## Syntax

```Haskell
DATETIME CURRENT_TIMESTAMP()
```

## Examples

Example 1: 現在の時刻を返します。

```Plain Text
MySQL > select current_timestamp();
+---------------------+
| current_timestamp() |
+---------------------+
| 2019-05-27 15:59:33 |
+---------------------+
```

Example 2: テーブルを作成する際に、この関数をカラムに使用して、カラムのデフォルト値として現在の時刻を設定できます。

```SQL
CREATE TABLE IF NOT EXISTS sr_member (
    sr_id            INT,
    name             STRING,
    city_code        INT,
    reg_date         DATETIME DEFAULT current_timestamp,
    verified         BOOLEAN
);
```

## keyword

CURRENT_TIMESTAMP,CURRENT,TIMESTAMP