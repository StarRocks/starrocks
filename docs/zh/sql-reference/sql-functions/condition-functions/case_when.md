---
displayed_sidebar: "Chinese"
---

# case

## 功能

CASE 是一种条件表达式，有两种写法：简单 CASE 表达式和搜索 CASE 表达式。

- 在简单 CASE 表达式中，将一个表达式 `expression` 与一个值比较。如果能找到匹配项，则返回 THEN 中的结果。如果未找到匹配项，则返回 ELSE 中的结果。如果未指定 ELSE，则返回 NULL。

- 在搜索 CASE 表达式中，会判断布尔表达式 `condition` 的结果是否为 TRUE。为 TRUE 的话返回 THEN 中的结果，否则返回 ELSE 中的结果。如果未指定 ELSE，则返回 NULL。

## 语法

- 简单 CASE 表达式

```SQL
CASE expression
    WHEN expression1 THEN result1
    [WHEN expression2 THEN result2]
    ...
    [WHEN expressionN THEN resultN]
    [ELSE result]
END
```

- 搜索 CASE 表达式

```SQL
CASE WHEN condition1 THEN result1
    [WHEN condition2 THEN result2]
    ...
    [WHEN conditionN THEN resultN]
    [ELSE result]
END
```

简单 CASE 表达式也可以用下面的搜索 CASE 表达式来表达，功能上对等。

```SQL
CASE WHEN expression = expression1 THEN result1
    [WHEN expression = expression2 THEN result2]
    ...
    [WHEN expression = expressionN THEN resultN]
    [ELSE result]
END
```

## 参数说明

- `expressionN`：要进行对比的表达式。多个表达式必须在数据类型上兼容。

- `conditionN`：要进行判断的条件。

- `resultN`：返回的结果。多个结果必须在数据类型上兼容。

## 返回值

返回值的类型是所有 THEN 子句结果中的公共类型 (common type)。

## 示例

假设有表 `test_case`，数据如下：

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

### 简单 CASE 表达式

- 指定了 ELSE，找不到匹配项时返回 ELSE 中的结果。

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

- 未指定 ELSE，找不到匹配项时返回 NULL。

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

### 搜索 CASE 表达式（指定了 ELSE）

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
