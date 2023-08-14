# DECIMAL

## Description

DECIMAL(P[,S])

High-precision fixed-point value. `P` stands for the total number of significant numbers (precision). `S` stands for the maximum number of decimal points (scale).

If `P` is omitted, the default is 10. If `S` is omitted, the default is 0.

* Decimal V2

  The range of `P` is [1,27] and the range of `S` is [0,9]. `P` must be greater than or equal to the value of `S`. The default value of `S` is 0.

* Fast Decimal (Decimal V3)

  The range of `P` is [1,38] and the range of `S` is [0, P]. The default value of `S` is 0. Fast Decimal provides a higher precision.
  
  Major optimizations:
  
  ​1. Fast Decimal uses variable-width integers to express decimals. For example, it uses 64-bit integers to express decimals whose precision is less than or equal to 18. Whereas, Decimal V2 uses 128-bit integers uniformly for all decimals. Arithmetic operations and conversion operations on 64-bit processors use fewer instructions, which greatly improves performance.
  
  ​2. Compared with Decimal V2, Fast Decimal made significant optimizations in some algorithms, especially in multiplication, which improves performance by about 4 times.

Fast Decimal is controlled by the FE dynamic parameter `enable_decimal_v3`, which is `true` by default.

From v3.1 onwards, StarRocks supports Fast Decimal entries in [ARRAY](Array.md), [MAP](Map.md), and [STRUCT](STRUCT.md).
  
## Limits

StarRocks does not support querying DECIMAL data in ORC and Parquet files from Hive.

## Examples

Define DECIMAL columns when creating a table.

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
