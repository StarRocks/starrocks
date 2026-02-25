---
displayed_sidebar: docs
---

# DECIMAL

DECIMAL(P[,S]) is a high-precision fixed-point value. `P` stands for the total number of significant numbers (precision). `S` stands for the maximum number of decimal points (scale).

If `P` is omitted, the default value is 10. If `S` is omitted, the default value is 0.

- For versions earlier than v4.0, StarRocks supports DECIMAL128 based on [Fast DECIMAL](#fast-decimal).
- For v4.0 and later versions, StarRocks supports [DECIMAL256](#decimal256).

## Features

### Fast DECIMAL

Controlled by the FE dynamic parameter `enable_decimal_v3`, Fast DECIMAL is enabled by default.

For Fast DECIMAL, the range of `P` is [1,38] and the range of `S` is [0, P]. The default value of `S` is 0.

Fast DECIMAL uses variable-width integers to express decimals.

- Decimal(P &le; 18, S)
  - LogicalType: Decimal64
  - Stored as: int64
  - Delegate LogicalType: BIGINT

- Decimal(P &gt; 18 & P &le; 38, S)
  - LogicalType: Decimal128
  - Stored as: int128
  - Delegate LogicalType: LARGEINT

From v3.1 onwards, StarRocks supports Fast Decimal entries in [ARRAY](../semi_structured/Array.md), [MAP](../semi_structured/Map.md), and [STRUCT](../semi_structured/STRUCT.md).

## DECIMAL256

StarRocks introduces DECIMAL256 from v4.0 onwards.

For DECIMAL256, the range of `P` is (38,76] and the range of `S` is [0, P]. The default value of `S` is 0.

DECIMAL256 expands the upper limit of precision to 76 bits. Its numerical capacity is 10³⁸ times higher than that of DECIMAL128, reducing the probability of overflow to a negligible level.

DECIMAL256 uses the same strategy to express decimals as that of Fast DECIMAL. In addition to the two decimal types mentioned above, it supports:

- Decimal(P &gt; 38 & P &le; 76, S)
  - LogicalType: Decimal256
  - Stored as: int256
  - Delegate LogicalType: INT_256

The actual representable range of DECIMAL256 is `-57896044618658097711785492504343953926634992332820282019728792003956564819968` to `57896044618658097711785492504343953926634992332820282019728792003956564819967`.

#### Type casting operations

DECIMAL256 supports bidirectional type conversion between all types listed below:

| Source Type     | Target Types                                                      |
| --------------- | ----------------------------------------------------------------- |
| TYPE_DECIMAL256 | BOOL                                                              |
| TYPE_DECIMAL256 | TYPE_TINYINT, TYPE_SMALLINT, TYPE_INT, TYPE_BIGINT, TYPE_LARGEINT |
| TYPE_DECIMAL256 | TYPE_FLOAT, TYPE_DOUBLE                                           |
| TYPE_DECIMAL256 | TYPE_VARCHAR                                                      |

#### Aggregate functions

Currently, DECIMAL256 supports these aggregation functions: `COUNT`, `COUNT DISTINCT`, `SUM`, `SUM DISTINCT`, `AVG`, `AVG DISTINCT`, `MAX`, `MIN`, and `ABS`.

#### Limitations

DECIMAL256 has the following limitations:

- Currently, DECIMAL256 does not support automatic precision scale-up for decimal operations.

  For example, `DECIMAL128 * DECIMAL128` will not be automatically scaled up to DECIMAL256. DECIMAL256 features are only applied when `DECIMAL256` is explicitly specified as the operand either in the CREATE TABLE statement or using CAST (`SELECT CAST(p38s10 as DECIMAL(70, 30))`).

- DECIMAL256 does not support window functions.

- Aggregate tables do not support DECIMAL256 type.

## Examples

### Fast DECIMAL example

Define DECIMAL columns when creating a table.

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

### DECIMAL256 example

Create a table with a DECIMAL256 type column.

```SQL
CREATE TABLE test_decimal256(
    p50s48 DECIMAL(50, 48) COMMENT ""
);
```

INSERT different values:

```SQL
INSERT INTO test_decimal256(p50s48) SELECT 1.222222;
```

This statement succeeds with the values written correctly.

```SQL
INSERT INTO test_decimal256(p50s48) SELECT 11111111111111111111111111111111111111111111.222222;
```

This statement succeeds with the values written as NULL. Because the integer part and decimal part (which has 44 digits) with the specified scale (which is `48`) together exceed the maximum integer that can be represented by 256 bits, FE directly casts the value to NULL.

```SQL
INSERT INTO test_decimal256(p50s48) SELECT 333;
```

This statement will be returned with an error `ERROR 1064 (HY000): Insert has filtered data`. Because the integer part and decimal part with the specified scale (which is `48`) together exceed the maximum value represented by a 50-digit integer, BE returns the error.

## Usage notes

You can set the system variable `sql_mode` to `ERROR_IF_OVERFLOW` to allow the system to return an error instead of NULL in the case of arithmetic overflow.

## Keywords

decimal, decimalv3, fast decimal, decimal128, decimal256
