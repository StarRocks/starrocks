---
displayed_sidebar: "English"
---

# DECIMAL

## Description

DECIMAL(P[,S])

High-precision fixed-point, P stands for the total number of significant numbers (precision), S stands for the maximum number of decimal points (scale).

* Decimal V2

  The range of P is [1,27], the range of S is [0,9], in addition, P must be greater than or equal to the value of S. The default value of S is 0.

* Fast Decimal (version 1.18 default)

  The range of P is [1,38], and the range of S is [0, P]. The default value of S is 0. Since starrocks-1.18, the decimal type supports FastDecimal with higher precision.
  
  The main optimizations are:
  
  ​    1. Multiple width integers are used internally to represent decimal. Decimal (P < = 18, S) uses 64bit integers. Compared with the original decimal V2 implementation, 128bit integers are used uniformly. Arithmetic operations and conversion operations use fewer instructions on 64bit processors, so the performance is greatly improved.
  
  ​    2. Compared with decimal V2, the Fast Decimal implementation has made extreme optimization of specific algorithms, especially multiplication, and the performance is improved by about 4 times.
  
  Current restrictions:
  
  ​     1. At present, fast decimal does not support array type. If users want to use array (decimal) type, please use array (double) type, or use array (decimal) type after closing decimal v3.
  
  ​     2. In hive direct connection, orc and parquet data formats do not support decimal yet.
