---
displayed_sidebar: docs
---

# current_timestamp

現在の日付を取得し、DATETIME 型の値を返します。

この関数は [now()](./now.md) 関数の同義語です。

## Syntax

```Haskell
DATETIME CURRENT_TIMESTAMP()
DATETIME CURRENT_TIMESTAMP(INT p)
```

## パラメータ

`p`: 任意指定で、秒の後に保持する桁数を指定します。範囲は [1,6] の INT 値でなければなりません。`select current_timestamp(0)` は `select current_timestamp()` と同等です。

## 戻り値

- `p` が指定されていない場合、この関数は秒単位の精度で DATETIME 値を返します。
- `p` が指定されている場合、この関数は指定された精度の日付と時刻の値を返します。

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
