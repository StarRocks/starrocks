---
displayed_sidebar: "Chinese"
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

### Fast Decimal (1.18 版本默认)

`P` 的范围是 [1,38]，`S` 的范围 [0, P]。默认的 `S` 取值为 0。

StarRocks 1.18 版本开始起，Decimal 类型支持更高精度的 Fast Decimal (也称 Decimal V3)。

主要优化有：

1. 内部采用多种宽度的整数来表示 Decimal。比如使用 64-bit 整数来表示 P &le; 18 的 Decimal 数值。相比于 Decimal V2 统一采用 128-bit 整数，算数运算和转换运算在 64-bit 的处理器上使用更少的指令，因此性能有大幅提升。

2. 和 Decimal V2 相比，Fast Decimal 的算法做了极致的优化，尤其是乘法运算，性能提升有 4 倍左右。

Fast Decimal 功能由 FE 动态参数 `enable_decimal_v3` 控制，默认值为 `true`，表示开启。

从 3.1 版本开始，[ARRAY](Array.md)、[MAP](Map.md)、[STRUCT](STRUCT.md) 支持 Fast Decimal。

### 使用限制

读取外部 Hive 数据时，暂不支持读取 ORC 和 Parquet 文件中的 Decimal 数据，会有精度丢失。

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

decimal, decimalv2, decimalv3, fast decimal
