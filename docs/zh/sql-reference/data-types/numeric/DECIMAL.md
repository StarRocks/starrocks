---
displayed_sidebar: docs
description: "DECIMAL(P[,S]) 是高精度定点数值类型，P 代表精度，S 代表刻度。"
---

# DECIMAL

## 描述

DECIMAL(P [, S])

高精度定点数，`P` 代表一共有多少个有效数字 (precision)，`S` 代表小数点后最多有多少数字 (scale)。

1.19.0 及以后版本对 DECIMAL 类型的（P，S）有默认值设置，默认是 Decimal（10，0）。

例如：

- 1.19.0 版本可成功执行 `select cast（‘12.35’ as decimal）;`，无需指定 P, S 的值。

- 1.19 之前版本需明确指定 P, S 的值，如：`select cast（‘12.35’ as decimal（5，1）;`。如果不指定，例如 `select cast（‘12.35’ as decimal）;` 或 `select cast（‘12.35’ as decimal（5））;`，会提示 failed。

### Decimal V2

`P` 的范围是 [1,27]，`S` 的范围 [0, 9]。另外，`P` 必须要大于等于 `S` 的取值。默认的 `S` 取值为 0。

<<<<<<< HEAD
### Fast Decimal (1.18 版本默认)
=======
- Decimal(P `>` 18 & P `<` 38, S)
  - LogicalType：Decimal128
  - 存储格式：int128
  - Delegate LogicalType：LARGEINT
>>>>>>> e16d27a64e ([Doc] generate descriptions (#74345))

`P` 的范围是 [1,38]，`S` 的范围 [0, P]。默认的 `S` 取值为 0。

StarRocks 1.18 版本开始起，Decimal 类型支持更高精度的 Fast Decimal (也称 Decimal V3)。

主要优化有：

1. 内部采用多种宽度的整数来表示 Decimal。比如使用 64-bit 整数来表示 P &le; 18 的 Decimal 数值。相比于 Decimal V2 统一采用 128-bit 整数，算数运算和转换运算在 64-bit 的处理器上使用更少的指令，因此性能有大幅提升。

2. 和 Decimal V2 相比，Fast Decimal 的算法做了极致的优化，尤其是乘法运算，性能提升有 4 倍左右。

Fast Decimal 功能由 FE 动态参数 `enable_decimal_v3` 控制，默认值为 `true`，表示开启。

<<<<<<< HEAD
从 3.1 版本开始，[ARRAY](../semi_structured/Array.md)、[MAP](../semi_structured/Map.md)、[STRUCT](../semi_structured/STRUCT.md) 支持 Fast Decimal。
=======
- Decimal(P `>` 38 & P `<` 76, S)
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
>>>>>>> e16d27a64e ([Doc] generate descriptions (#74345))

## 示例

建表时指定字段类型为 DECIMAL。

```sql
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

## keywords

<<<<<<< HEAD
decimal, decimalv2, decimalv3, fast decimal
=======
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
>>>>>>> e16d27a64e ([Doc] generate descriptions (#74345))
