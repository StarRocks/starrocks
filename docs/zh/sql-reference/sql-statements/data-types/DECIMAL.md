# DECIMAL

## description

DECIMAL(P[,S])

高精度定点数，P代表一共有多少个有效数字(precision)，S代表小数点后最多有多少数字(scale)

1.19.0及以后版本对decimal类型的（P，S）有默认值设置，默认是decimal（10，0）

例如：
1.19.0版本可成功执行`select cast（‘12.35’ as decimal）;`1.19之前版本执行`select cast（‘12.35’ as decimal）;`或者`select cast（‘12.35’ as decimal（5））;`会提示failed，需明确指定p,s的值,如：`select cast（‘12.35’ as decimal（5，1）;`

* Decimal V2

  P的范围是[1,27], S的范围[0, 9], 另外，P必须要大于等于S的取值。默认的S取值为0。

* Fast Decimal  (1.18版本默认)

  P的范围是[1,38], S的范围[0, P]。默认的S取值为0。
  StarRocks-1.18版本开始起, decimal类型支持更高精度的FastDecimal

  主要优化有：
  
  1. 内部采用多种宽度的整数表示decimal, `decimal(P<=18, S)`使用64bit整数, 相比于原来decimal v2实现统一采用128bit整数, 算数运算和转换运算在64bit的处理器上使用更少的指令数量, 因此性能有大幅提升。
  2. Fast Decimal 实现和decimal v2相比, 具体算法做了极致的优化, 尤其是乘法运算，性能提升有4倍左右。
  
  当前的限制：
  
  1. 目前fast decimal 不支持array类型, 如果用户想使用array(decimal)类型, 请使用array(double)类型, 或者关闭decimal v3之后, 使用array(decimal)类型。
  2. hive直连外表中, orc和parquet数据格式对decimal暂未支持

## keyword

DECIMAL
