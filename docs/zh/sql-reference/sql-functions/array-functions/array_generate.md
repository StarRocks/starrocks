---
displayed_sidebar: "Chinese"
---

# array_generate

## 功能

生成一个包含数值元素的数组，数值范围在 `start` 和 `end` 之间，步长为 `step`。

该函数从 3.1 开始支持。

## 语法

```Haskell
ARRAY array_generate([start,] end [, step])
```

## 参数说明

- `start`：可选参数。支持数据类型为 TINYINT、SMALLINT、INT、BIGINT、LARGEINT 的常量或列。如果不指定，默认值为 1。
- `end`：必选参数。支持数据类型为 TINYINT、SMALLINT、INT、BIGINT、LARGEINT 的常量或列。
- `step`：可选参数。支持数据类型为 TINYINT、SMALLINT、INT、BIGINT、LARGEINT 的常量或列。当 `start` < `end` 时, 如果不指定，默认值为 1。当 `start` > `end` 时，如果不指定，默认值为 -1。

## 返回值说明

返回值的数据类型为 ARRAY。数组中的元素类型与输入参数的类型相同。

## 注意事项

- 当任意参数为列时，需指定列所属的表。
- 当任意参数为列时，其他参数必须指定，不支持使用默认值。
- 当任意参数为 NULL 时，结果返回 NULL。
- 当 `step` = 0 时，返回空数组。
- 当 `start` = `end` 时，返回该值。

## 示例

### 输入参数为常量的情况

```Plain Text
mysql> select array_generate(9);
+---------------------+
| array_generate(9)   |
+---------------------+
| [1,2,3,4,5,6,7,8,9] |
+---------------------+

mysql> select array_generate(9,12);
+-----------------------+
| array_generate(9, 12) |
+-----------------------+
| [9,10,11,12]          |
+-----------------------+

mysql> select array_generate(9,6);
+----------------------+
| array_generate(9, 6) |
+----------------------+
| [9,8,7,6]            |
+----------------------+

mysql> select array_generate(9,6,-1);
+--------------------------+
| array_generate(9, 6, -1) |
+--------------------------+
| [9,8,7,6]                |
+--------------------------+

mysql> select array_generate(3,3);
+----------------------+
| array_generate(3, 3) |
+----------------------+
| [3]                  |
+----------------------+
```

### 输入参数为列的情况

```sql
CREATE TABLE `array_generate`
(
  `c1` TINYINT,
  `c2` SMALLINT,
  `c3` INT
)
ENGINE = OLAP
DUPLICATE KEY(`c1`)
DISTRIBUTED BY HASH(`c1`);

INSERT INTO `array_generate` VALUES
(1, 6, 3),
(2, 9, 4);
```

```Plain Text
mysql> select array_generate(1,c2,2) from `array_generate`;
+--------------------------+
| array_generate(1, c2, 2) |
+--------------------------+
| [1,3,5]                  |
| [1,3,5,7,9]              |
+--------------------------+
```
