# DECIMAL

## description

DECIMAL(M[,D])

高精度定点数，M代表一共有多少个有效数字(precision)，D代表小数点后最多有多少数字(scale)

* Decimal V2

  M的范围是[1,27], D的范围[1, 9], 另外，M必须要大于等于D的取值。默认的D取值为0。

* Fast Decimal  (1.18版本支持)

  M的范围是[1,38], D的范围[1, M]。默认的D取值为0。
  StarRocks-1.18版本开始起, decimal类型支持更高精度的FastDecimal

  主要优化有：
  
  1. 内部采用多种宽度的整数表示decimal, decimal(M<=18, D)使用64bit整数, 相比于原来decimal v2实现统一采用128bit整数, 算数运算和转换运算在64bit的处理器上使用更少的指令数量, 因此性能有大幅提升。
  2. Fast Decimal 实现和decimal v2相比, 具体算法做了极致的优化, 尤其是乘法运算，性能提升有4倍左右。
  
  当前的限制：
  
  1. 目前fast decimal 不支持array类型, 如果用户想使用array(decimal)类型, 请使用array(double)类型, 或者关闭decimal v3之后, 使用array(decimal)类型.
  2. hive直连外表中, orc和parquet数据格式对decimal暂未支持

## keyword

DECIMAL
