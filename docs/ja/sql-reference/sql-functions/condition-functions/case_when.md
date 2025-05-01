---
displayed_sidebar: docs
---

# case

## Description

CASE は条件式です。WHEN 節の条件が true と評価されると、THEN 節の結果を返します。どの条件も true と評価されない場合、オプションの ELSE 節の結果を返します。ELSE が存在しない場合は、NULL が返されます。

## Syntax

CASE 式には 2 つの形式があります。

- Simple CASE

```SQL
CASE expression
    WHEN expression1 THEN result1
    [WHEN expression2 THEN result2]
    ...
    [WHEN expressionN THEN resultN]
    [ELSE result]
END
```

この構文では、`expression` は WHEN 節の各式と比較されます。等しい式が見つかれば、THEN 節の結果が返されます。等しい式が見つからない場合、ELSE が存在すれば ELSE 節の結果が返されます。

- Searched CASE

```SQL
CASE WHEN condition1 THEN result1
    [WHEN condition2 THEN result2]
    ...
    [WHEN conditionN THEN resultN]
    [ELSE result]
END
```

この構文では、WHEN 節の各条件が評価され、true となるものが見つかるまで評価され、対応する THEN 節の結果が返されます。どの条件も true と評価されない場合、ELSE が存在すれば ELSE 節の結果が返されます。

最初の CASE は次のように 2 番目のものと等しいです。

```SQL
CASE WHEN expression = expression1 THEN result1
    [WHEN expression = expression2 THEN result2]
    ...
    [WHEN expression = expressionN THEN resultN]
    [ELSE result]
END
```

## Parameters

- `expressionN`: 比較する式。複数の式はデータ型が互換性がある必要があります。この式は次のデータ型のいずれかに評価される必要があります: BOOLEAN, TINYINT, SMALLINT, INT, BIGINT, LARGEINT, FLOAT, DOUBLE, DATETIME, DATE, DECIMALV2, DECIMAL32, DECIMAL64, DECIMAL128, VARCHAR, BITMAP, PERCENTILE, HLL, TIME, JSON。

- `conditionN`: BOOLEAN 値に評価される条件。

- `resultN` はデータ型が互換性がある必要があります。

## Return value

戻り値は THEN 節のすべての型の共通型です。

## Examples

テーブル `test_case` が次のデータを持っているとします。

```SQL
CREATE TABLE test_case(
    name          STRING,
    gender         INT,
    ) DISTRIBUTED BY HASH(name);

INSERT INTO test_case VALUES
    ("Andy",1),
    ("Jules",0),
    ("Angel",-1),
    ("Sam",null);

SELECT * FROM test_case;
+-------+--------+
| name  | gender |
+-------+--------+
| Angel |     -1 |
| Andy  |      1 |
| Sam   |   NULL |
| Jules |      0 |
+-------+--------+-------+
```

### Use simple CASE

- ELSE が指定されており、等しい式が見つからない場合に ELSE の結果が返されます。

```plain
mysql> select gender, case gender 
                    when 1 then 'male'
                    when 0 then 'female'
                    else 'error'
               end gender_str
from test_case;
+--------+------------+
| gender | gender_str |
+--------+------------+
|   NULL | error      |
|      0 | female     |
|      1 | male       |
|     -1 | error      |
+--------+------------+
```

- ELSE が指定されておらず、条件が true と評価されない場合は NULL が返されます。

```plain
select gender, case gender 
                    when 1 then 'male'
                    when 0 then 'female'
               end gender_str
from test_case;
+--------+------------+
| gender | gender_str |
+--------+------------+
|      1 | male       |
|     -1 | NULL       |
|   NULL | NULL       |
|      0 | female     |
+--------+------------+
```

### Use searched CASE with no ELSE specified

```plain
mysql> select gender, case when gender = 1 then 'male'
                           when gender = 0 then 'female'
                      end gender_str
from test_case;
+--------+------------+
| gender | gender_str |
+--------+------------+
|   NULL | NULL       |
|     -1 | NULL       |
|      1 | male       |
|      0 | female     |
+--------+------------+
```

## Keywords

case when, case, case_when, case...when