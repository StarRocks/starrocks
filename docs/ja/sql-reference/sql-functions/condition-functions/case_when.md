---
displayed_sidebar: docs
---

# case

## 説明

CASE は条件式です。WHEN 節の条件が真と評価されると、THEN 節の結果を返します。どの条件も真と評価されない場合、オプションの ELSE 節の結果を返します。ELSE が存在しない場合、NULL が返されます。

## 構文

CASE 式には2つの形式があります。

- シンプル CASE

```SQL
CASE expression
    WHEN expression1 THEN result1
    [WHEN expression2 THEN result2]
    ...
    [WHEN expressionN THEN resultN]
    [ELSE result]
END
```

この構文では、`expression` は WHEN 節の各式と比較されます。同じ式が見つかれば、THEN 節の結果が返されます。同じ式が見つからない場合、ELSE 節が存在すればその結果が返されます。

- 検索 CASE

```SQL
CASE WHEN condition1 THEN result1
    [WHEN condition2 THEN result2]
    ...
    [WHEN conditionN THEN resultN]
    [ELSE result]
END
```

この構文では、WHEN 節の各条件が評価され、真となる条件が見つかるまで評価されます。真となる条件が見つかれば、対応する THEN 節の結果が返されます。どの条件も真と評価されない場合、ELSE 節が存在すればその結果が返されます。

最初の CASE は次のように2番目の CASE と等価です。

```SQL
CASE WHEN expression = expression1 THEN result1
    [WHEN expression = expression2 THEN result2]
    ...
    [WHEN expression = expressionN THEN resultN]
    [ELSE result]
END
```

## パラメータ

- `expressionN`: 比較する式。複数の式はデータ型が互換性がある必要があります。

- `conditionN`: BOOLEAN 値に評価される条件。

- `resultN` はデータ型が互換性がある必要があります。

## 戻り値

戻り値は THEN 節内のすべての型の共通の型です。

## 例

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

### シンプル CASE の使用

- ELSE が指定されており、同じ式が見つからない場合、ELSE の結果が返されます。

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

- ELSE が指定されておらず、どの条件も真と評価されない場合、NULL が返されます。

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

### ELSE が指定されていない検索 CASE の使用

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

## キーワード

case when, case, case_when, case...when