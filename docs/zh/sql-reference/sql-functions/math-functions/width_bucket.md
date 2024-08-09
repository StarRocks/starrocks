# width_bucket

## 功能

返回等宽直方图中某个值的 Bucket 编号。

## 语法

```Shell
WIDTH_BUCKET( <expr> , <min_value> , <max_value> , <num_buckets> )
```

## Parameter

- `expr` : 需要设置bucket的数值或者表达式，必须为数值类型或者可以转换成数值类型。

- `min_value` 和 `max_value` : bucket的上下界，且上界不能等于下界。

- `num_buckets` : 正整数，需要设置的bucket数。

## 注意

   - 如果 expr 在 下界 之外，则结果为 0。
   - 如果 expr 在 上界 之外，则结果为 numbuckets + 1。
   - 如果参数有 null 返回 null。


## 返回值

返回bucket的位置。

## 示例

```Plain
mysql> select width_bucket(5.35, 0.024, 10.06, 5);
+-------------------------------------+
| width_bucket(5.35, 0.024, 10.06, 5) |
+-------------------------------------+
|                                   3 |
+-------------------------------------+
1 row in set (0.00 sec)
```
