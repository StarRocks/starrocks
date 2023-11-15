# DECIMAL

## 描述

DECIMAL(P [, S])

高精度定点数，`P` 代表一共有多少个有效数字 (precision)，`S` 代表小数点后最多有多少数字 (scale)。

1.19.0 及以后版本对 decimal 类型的（P，S）有默认值设置，默认是 decimal（10，0）。

例如：

1.19.0 版本可成功执行 `select cast（‘12.35’ as decimal）;`。

1.19 之前版本执行 `select cast（‘12.35’ as decimal）;` 或 `select cast（‘12.35’ as decimal（5））;` 会提示 failed，需明确指定 P, S 的值，如：`select cast（‘12.35’ as decimal（5，1）;`。

* DecimalV2

  `P` 的范围是 [1,27]， `S` 的范围 [0, 9]。另外，`P` 必须要大于等于 `S` 的取值。默认的 `S` 取值为 0。

* Fast Decimal  (1.18 版本默认)

  `P` 的范围是 [1,38]，`S` 的范围 [0, P]。默认的 `S` 取值为 0。

  StarRocks-1.18 版本开始起，decimal 类型支持更高精度的 Fast Decimal。

  主要优化有：
  
  1. 内部采用多种宽度的整数表示 `decimal，decimal(P <= 18, S)`使用 64-bit 整数，相比于原来 DecimalV2 实现统一采用 128-bit 整数，算数运算和转换运算在 64-bit 的处理器上使用更少的指令数量，因此性能有大幅提升。

  2. Fast Decimal 实现和 DecimalV2 相比，具体算法做了极致的优化，尤其是乘法运算，性能提升有 4 倍左右。
  
  当前的限制：
  
  1. 目前 Fast Decimal 不支持 ARRAY 类型。如果用户想使用 array(decimal)类型，请使用 array(double) 类型，或者关闭 DecimalV3 之后，使用 array(decimal) 类型。
  
  2. Hive 直连外表中，ORC 和 Parquet 数据格式对 Decimal 暂未支持。

## 示例

创建表时指定字段类型为 DECIMAL。

```sql
CREATE TABLE decimalDemo (
    pk BIGINT(20) NOT NULL COMMENT "",
    account DECIMAL(12,4) COMMENT ""
) ENGINE=OLAP 
DUPLICATE KEY(pk)
COMMENT "OLAP"
DISTRIBUTED BY HASH(pk) BUCKETS 4;
```

## 关键字

DECIMAL
