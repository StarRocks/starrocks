---
displayed_sidebar: docs
---

# bool_or

`expr` のいずれかの行が true と評価された場合に true を返します。それ以外の場合は false を返します。`boolor_agg` はこの関数のエイリアスです。

## 構文

```Haskell
bool_or(expr)
```

## パラメータ

`expr`: この式は、true または false のブール値に評価される必要があります。

## 戻り値

ブール値を返します。 `expr` が存在しない場合はエラーが返されます。

## 使用上の注意

この関数は NULL 値を無視します。

## 例

1. `employees` という名前のテーブルを作成します。

    ```SQL
    CREATE TABLE IF NOT EXISTS employees (
        region_num    TINYINT,
        id            BIGINT,
        is_manager    BOOLEAN
        )
        DISTRIBUTED BY HASH(region_num);
    ```

2. `employees` テーブルにデータを挿入します。

    ```SQL
    INSERT INTO employees VALUES
    (3,432175, TRUE),
    (4,567832, FALSE),
    (3,777326, FALSE),
    (5,342611, TRUE),
    (2,403882, FALSE);
    ```

3. `employees` テーブルからデータを照会します。

    ```Plain Text
    MySQL > select * from employees;
    +------------+--------+------------+
    | region_num | id     | is_manager |
    +------------+--------+------------+
    |          3 | 432175 |          1 |
    |          4 | 567832 |          0 |
    |          3 | 777326 |          0 |
    |          5 | 342611 |          1 |
    |          2 | 403882 |          0 |
    +------------+--------+------------+
    5 rows in set (0.01 sec)
    ```

4. この関数を使用して、各地域にマネージャーがいるかどうかを確認します。

    例 1: 各地域に少なくとも 1 人のマネージャーがいるかどうかを確認します。

    ```Plain Text
    MySQL > SELECT region_num, bool_or(is_manager) from employees
    group by region_num;

    +------------+---------------------+
    | region_num | bool_or(is_manager) |
    +------------+---------------------+
    |          3 |                   1 |
    |          4 |                   0 |
    |          5 |                   1 |
    |          2 |                   0 |
    +------------+---------------------+
    4 rows in set (0.01 sec)
    ```

## キーワード

BOOL_OR, bool_or, boolor_agg
