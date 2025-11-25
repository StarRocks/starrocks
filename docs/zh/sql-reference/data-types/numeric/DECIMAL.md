---
displayed_sidebar: docs
---

# DECIMAL

DECIMAL(P[,S]) 是一种高精度定点数值。`P` 代表有效数字的总数（精度，Precision）。`S` 代表小数点后的最大位数（刻度，Scale）。

如果省略 `P`，则默认值为 10。如果省略 `S`，则默认值为 0。

- 对于 v4.0 之前的版本，StarRocks 支持基于 [Fast DECIMAL](#fast-decimal) 的 DECIMAL128。
- 对于 v4.0 及更高版本，StarRocks 支持 [DECIMAL256](#decimal256)。

## 功能

### Fast DECIMAL

由 FE 动态参数 `enable_decimal_v3` 控制，Fast DECIMAL 默认启用。

对于 Fast DECIMAL，`P` 的范围是 [1,38]，`S` 的范围是 [0, P]。`S` 的默认值是 0。

Fast DECIMAL 使用可变宽度整数来表示小数。

- Decimal(P &le; 18, S)
  - LogicalType：Decimal64
  - 存储格式：int64
  - Delegate LogicalType：BIGINT

- Decimal(P &gt; 18 & P &le; 38, S)
  - LogicalType：Decimal128
  - 存储格式：int128
  - Delegate LogicalType：LARGEINT

从 v3.1 开始，StarRocks 支持在 [ARRAY](../semi_structured/Array.md)、[MAP](../semi_structured/Map.md) 和 [STRUCT](../semi_structured/STRUCT.md) 中使用 Fast Decimal 类型。

## DECIMAL256

StarRocks 从 v4.0 开始引入 DECIMAL256。

对于 DECIMAL256，`P` 的范围是 (38,76]，`S` 的范围是 [0, P]。`S` 的默认值是 0。

DECIMAL256 将精度的上限扩展到 76 位。其数值容量是 DECIMAL128 的 10³⁸ 倍，极大地降低了溢出的概率。

DECIMAL256 使用与 Fast DECIMAL 相同的策略来表示小数。除了上述两种小数类型外，它还支持：

- Decimal(P &gt; 38 & P &le; 76, S)
  - LogicalType：Decimal256
  - 存储格式：int256
  - Delegate LogicalType：INT_256

DECIMAL256 的实际可表示范围是 `-57896044618658097711785492504343953926634992332820282019728792003956564819968` 到 `57896044618658097711785492504343953926634992332820282019728792003956564819967`。

#### 类型转换操作

DECIMAL256 支持与下列所有类型之间的双向类型转换：

| 源类型          | 目标类型                                                        |
| --------------- | ----------------------------------------------------------------- |
| TYPE_DECIMAL256 | BOOL                                                              |
| TYPE_DECIMAL256 | TYPE_TINYINT, TYPE_SMALLINT, TYPE_INT, TYPE_BIGINT, TYPE_LARGEINT |
| TYPE_DECIMAL256 | TYPE_FLOAT, TYPE_DOUBLE                                           |
| TYPE_DECIMAL256 | TYPE_VARCHAR                                                      |

#### 聚合函数

目前，DECIMAL256 支持以下聚合函数：`COUNT`、`COUNT DISTINCT`、`SUM`、`SUM DISTINCT`、`AVG`、`AVG DISTINCT`、`MAX`、`MIN` 和 `ABS`。

#### 限制

DECIMAL256 有以下限制：

- 目前，DECIMAL256 不支持小数运算的自动精度提升。

  例如，`DECIMAL128 * DECIMAL128` 不会自动提升为 DECIMAL256。DECIMAL256 的特性仅在 `DECIMAL256` 被明确指定为操作数时应用，无论是在 CREATE TABLE 语句中还是使用 CAST（`SELECT CAST(p38s10 as DECIMAL(70, 30))`）。

- DECIMAL256 不支持窗口函数。

- 聚合表不支持 DECIMAL256 类型。

## 示例

### Fast DECIMAL 示例

在创建表时定义 DECIMAL 列。

```SQL
CREATE TABLE decimalDemo (
    pk BIGINT(20) NOT NULL COMMENT "",
    account DECIMAL(20,10) COMMENT ""
) ENGINE=OLAP 
DUPLICATE KEY(pk)
COMMENT "OLAP"
DISTRIBUTED BY HASH(pk);

INSERT INTO decimalDemo VALUES
(1,3.141592656),
(2,21.638378),
(3,4873.6293048479);

SELECT * FROM decimalDemo;
+------+-----------------+
| pk   | account         |
+------+-----------------+
|    1 |    3.1415926560 |
|    3 | 4873.6293048479 |
|    2 |   21.6383780000 |
+------+-----------------+
```

### DECIMAL256 示例

创建一个包含 DECIMAL256 类型列的表。

```SQL
CREATE TABLE test_decimal256(
    p50s48 DECIMAL(50, 48) COMMENT ""
);
```

插入不同的值：

```SQL
INSERT INTO test_decimal256(p50s48) SELECT 1.222222;
```

此语句成功执行，值被正确写入。

```SQL
INSERT INTO test_decimal256(p50s48) SELECT 11111111111111111111111111111111111111111111.222222;
```

此语句成功执行，值被写为 NULL。因为整数部分和小数部分（有 44 位数字）与指定的刻度（即 `48`）一起超过了 256 位整数可表示的最大值，FE 直接将值转换为 NULL。

```SQL
INSERT INTO test_decimal256(p50s48) SELECT 333;
```

此语句将返回错误 `ERROR 1064 (HY000): Insert has filtered data`。因为整数部分和小数部分与指定的刻度（即 `48`）一起超过了 50 位整数可表示的最大值，BE 返回错误。

## 注意事项

您可以将系统变量 `sql_mode` 设置为 `ERROR_IF_OVERFLOW`，以便在算术溢出时让系统返回错误而不是 NULL。

## 关键词

decimal, decimalv3, fast decimal, decimal128, decimal256