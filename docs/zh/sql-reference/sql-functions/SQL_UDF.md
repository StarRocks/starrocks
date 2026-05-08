---
displayed_sidebar: docs
sidebar_position: 0.95
---

# SQL UDF

自 4.1 版本起，StarRocks 支持创建 SQL 自定义函数（UDFs），允许用户使用 SQL 表达式创建函数。

SQL UDF 是一种轻量级的函数定义方式，它将一个 SQL 表达式封装为可重用的函数，在查询时动态展开为实际的 SQL 表达式。

## 功能概述

SQL UDF 的主要特点：

- **动态展开**: 在查询优化阶段将函数调用展开为实际的 SQL 表达式
- **类型安全**: 支持参数类型和返回类型的显式声明
- **参数化**: 支持命名参数，提高函数可读性

适用场景：
- 需要复用的复杂 SQL 表达式
- 简单的数据转换和计算逻辑
- 标准化的数据处理规则

## 语法

### 创建 SQL UDF

```SQL
CREATE [GLOBAL] FUNCTION
    function_name(arg1_name arg1_type, arg2_name arg2_type, ...)
RETURNS expression
```

#### 参数说明

| **参数**           | **说明**                                                                                     |
| ------------------ | -------------------------------------------------------------------------------------------- |
| GLOBAL             | 可选。如果指定，表示创建全局函数，所有数据库可见                                             |
| function_name      | 函数名称。可以包含数据库名称，如 `db1.my_func`                                               |
| arg_name           | 参数名称，用于在表达式中引用                                                                 |
| arg_type           | 参数类型，支持 StarRocks 所有基础数据类型                                                    |
| expression         | SQL 表达式，函数调用时会被展开为实际的表达式                                                 |

### 删除 SQL UDF

```SQL
DROP FUNCTION [IF EXISTS] function_name(arg_type [, ...])
```

### 查看 SQL UDF

```SQL
SHOW [GLOBAL] FUNCTIONS;
```

## 使用示例

### 示例 1: 字符串处理函数

```SQL
-- 创建一个将字符串转换为大写并添加前缀的函数
CREATE FUNCTION format_username(name STRING)
RETURNS concat('USER_', upper(name));

-- 使用函数
SELECT format_username('alice');
-- 结果: USER_ALICE

-- 在查询中使用
SELECT format_username(username) as display_name FROM users;
```

### 示例 2: 多参数计算函数

```SQL
-- 创建一个计算折扣后价格的函数
CREATE FUNCTION calculate_discount_price(original_price DECIMAL(10,2), discount_rate DOUBLE)
RETURNS original_price * (1 - discount_rate);

-- 使用函数
SELECT calculate_discount_price(100.00, 0.2);  -- 结果: 80.00
SELECT calculate_discount_price(price, 0.15) as final_price FROM products;
```

### 示例 3: 复杂表达式封装

```SQL
-- 创建一个提取 JSON 并转换的函数
CREATE FUNCTION extract_user_info(json_str STRING, field_name STRING)
RETURNS get_json_string(get_json_string(json_str, concat('$.', field_name)), '$.value');

-- 简化复杂的嵌套调用
SELECT extract_user_info(user_data, 'email') as user_email FROM events;
```

### 示例 4: 条件逻辑函数

```SQL
-- 创建一个有条件逻辑的函数
CREATE FUNCTION classify_temperature(temp DOUBLE)
RETURNS CASE
    WHEN temp >= 30 THEN 'hot'
    WHEN temp >= 20 THEN 'warm'
    WHEN temp >= 10 THEN 'cool'
    ELSE 'cold'
END;

-- 使用函数
SELECT classify_temperature(25);  -- 结果: warm
```

### 示例 5: 全局 SQL UDF

```SQL
-- 创建全局函数，所有数据库可见
CREATE GLOBAL FUNCTION format_date_display(dt DATETIME)
RETURNS concat(year(dt), '-', lpad(month(dt), 2, '0'), '-', lpad(day(dt), 2, '0'));

-- 在任何数据库中都可以直接使用
SELECT format_date_display(create_time) from my_table;
```

## 高级特性

### 1. 嵌套使用

SQL UDF 支持嵌套调用：

```SQL
CREATE FUNCTION func_a(x INT, y INT) RETURNS x + y;
CREATE FUNCTION func_b(a INT, b INT) RETURNS func_a(a, b) * 2;

SELECT func_b(3, 4);  -- 结果: 14
```

### 2. 类型转换

SQL UDF 支持隐式类型转换：

```SQL
CREATE FUNCTION convert_and_add(a STRING, b INT)
RETURNS cast(a AS INT) + b;

SELECT convert_and_add('100', 50);  -- 结果: 150
```

### 3. 与内置函数组合

SQL UDF 可以与 StarRocks 内置函数自由组合：

```SQL
CREATE FUNCTION get_year_month(dt DATETIME)
RETURNS concat(year(dt), '-', lpad(month(dt), 2, '0'));

SELECT get_year_month(create_time) as ym FROM events GROUP BY ym;
```

## 查看和管理

### 查看所有函数

```SQL
-- 查看当前数据库的函数
SHOW FUNCTIONS;

-- 查看所有全局函数
SHOW GLOBAL FUNCTIONS;
```

### 删除函数

```SQL
-- 删除当前数据库的函数
DROP FUNCTION format_username(STRING);

-- 删除全局函数
DROP GLOBAL FUNCTION format_date_display(DATETIME);

-- 使用 IF EXISTS 避免错误
DROP FUNCTION IF EXISTS my_function(INT, STRING);
```

### 替换函数

使用 `OR REPLACE` 关键字可以替换已存在的函数：

```SQL
-- 修改函数定义
CREATE OR REPLACE FUNCTION calculate_tax(amount DECIMAL(10,2))
RETURNS amount * 0.1 + 5.0;
```

## 性能特点

SQL UDF 在查询优化阶段被展开为实际的 SQL 表达式，具有以下性能特点：

1. **零函数调用开销**: 没有传统 UDF 的函数调用开销
2. **优化器可见**: 优化器可以看到完整的表达式，进行更好的优化
3. **谓词下推**: 支持谓词下推和其他优化规则

例如，以下查询：

```SQL
CREATE FUNCTION my_func(x INT) RETURNS x * 2 + 1;

SELECT * FROM t WHERE my_func(a) > 10;
```

会被优化器展开为：

```SQL
SELECT * FROM t WHERE a * 2 + 1 > 10;
```

## 限制和注意事项

1. **参数数量限制**: 参数数量应与表达式中使用的变量一致
2. **类型匹配**: 参数类型应与表达式中的使用方式匹配
