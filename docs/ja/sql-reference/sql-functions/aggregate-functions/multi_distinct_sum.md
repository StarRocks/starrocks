---
displayed_sidebar: docs
---

# multi_distinct_sum

`expr` 内の異なる値の合計を返します。これは sum(distinct expr) と同等です。

## 構文

```Haskell
multi_distinct_sum(expr)
```

## パラメータ

- `expr`: 計算に関与するカラム。カラムの値は次の型のいずれかである必要があります: TINYINT, SMALLINT, INT, LARGEINT, FLOAT, DOUBLE, または DECIMAL。

## 戻り値

カラム値と戻り値の型の対応は次のとおりです:

- BOOLEAN -> BIGINT
- TINYINT -> BIGINT
- SMALLINT -> BIGINT
- INT -> BIGINT
- BIGINT -> BIGINT
- FLOAT -> DOUBLE
- DOUBLE -> DOUBLE
- LARGEINT -> LARGEINT
- DECIMALV2 -> DECIMALV2
- DECIMAL32 -> DECIMAL128
- DECIMAL64 -> DECIMAL128
- DECIMAL128 -> DECIMAL128

## 例

1. `k0` を INT フィールドとして持つテーブルを作成します。

    ```sql
    CREATE TABLE tabl
    (k0 BIGINT NOT NULL) ENGINE=OLAP
    DUPLICATE KEY(`k0`)
    COMMENT "OLAP"
    DISTRIBUTED BY HASH(`k0`)
    PROPERTIES(
        "replication_num" = "3",
        "storage_format" = "DEFAULT"
    );
    ```

2. テーブルに値を挿入します。

    ```sql
    -- 
    INSERT INTO tabl VALUES ('0'), ('1'), ('1'), ('1'), ('2'), ('2');
    ```

3. multi_distinct_sum() を使用して `k0` カラム内の異なる値の合計を計算します。

    ```plain text
    MySQL > select multi_distinct_sum(k0) from tabl;
    +------------------------+
    | multi_distinct_sum(k0) |
    +------------------------+
    |                      3 |
    +------------------------+
    1 row in set (0.03 sec)
    ```

    `k0` の異なる値は 0, 1, 2 であり、それらを合計すると 3 になります。