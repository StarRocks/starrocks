# BITMAP

这里通过一个简单的示例来介绍 Bitmap 中的几个聚合函数的用法，具体的函数定义以及更多 Bitmap 函数请参考 [bitmap-functions](../bitmap-functions/bitmap_and.md)。

## 建表

建表时需要使用聚合模型，数据类型是 bitmap，聚合函数为 bitmap_union。

```SQL
CREATE TABLE `pv_bitmap` (
  `dt` int(11) NULL COMMENT "",
  `page` varchar(10) NULL COMMENT "",
  `user_id` bitmap BITMAP_UNION NULL COMMENT ""
) ENGINE=OLAP
AGGREGATE KEY(`dt`, `page`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`dt`);
```

>当数据量很大时，最好为高频率的 bitmap_union 查询建立对应的 rollup 表，如:

```SQL
ALTER TABLE pv_bitmap ADD ROLLUP pv (page, user_id);
```

## 加载数据

`TO_BITMAP(expr)`: 将 0 ~ 18446744073709551615 的 unsigned bigint 转为 bitmap。

`BITMAP_EMPTY()`: 生成空 bitmap 列，用于 insert 或导入的时填充默认值。

`BITMAP_HASH(expr)`: 将任意类型的列通过 Hash 的方式转为 bitmap。

### Stream Load

用 Stream Load 方式导入数据时，可按如下方式转换为 Bitmap 字段:

``` bash
cat data | curl --location-trusted -u user:passwd -T - \
    -H "columns: dt,page,user_id, user_id=to_bitmap(user_id)" \
    http://host:8410/api/test/testDb/_stream_load
```

``` bash
cat data | curl --location-trusted -u user:passwd -T - \
    -H "columns: dt,page,user_id, user_id=bitmap_hash(user_id)" \
    http://host:8410/api/test/testDb/_stream_load
```

``` bash
cat data | curl --location-trusted -u user:passwd -T - \
    -H "columns: dt,page,user_id, user_id=bitmap_empty()" \
    http://host:8410/api/test/testDb/_stream_load
```

### Insert Into

用 Insert Into 方式导入数据时，需要根据 source 表中的列的类型来选择对应的方式

* source 表的 id2 的列类型是 bitmap

```SQL
insert into bitmap_table1
select id, id2 from bitmap_table2;
```

* target 表的 id2 的列类型是 bitmap

```SQL
insert into bitmap_table1 (id, id2)
values (1001, to_bitmap(1000))
, (1001, to_bitmap(2000));
```

* source 表的 id2 的列类型是 bitmap，并且是要用到 bitmap_union() 进行聚合后的结果

```SQL
insert into bitmap_table1
select id, bitmap_union(id2) from bitmap_table2 group by id;
```

* source 表的 id2 的列类型是 int，通过 to_bitmap() 生成 bitmap 类型

```SQL
insert into bitmap_table1
select id, to_bitmap(id2) from table;
```

* source 表的 id2 的列类型是 String，通过 bitmap_hash() 生成 bitmap 类型

```SQL
insert into bitmap_table1
select id, bitmap_hash(id2) from table;
```

## 语法及对应功能

`BITMAP_UNION(expr)`: 计算输入 Bitmap 的并集，返回新的bitmap。

`BITMAP_UNION_COUNT(expr)`: 计算输入 Bitmap 的并集，返回其基数，和 `BITMAP_COUNT(BITMAP_UNION(expr))` 等价,目前推荐优先使用 `BITMAP_UNION_COUNT()`，其性能优于 `BITMAP_COUNT(BITMAP_UNION(expr))`。

`BITMAP_UNION_INT(expr)`: 计算 TINYINT，SMALLINT 和 INT 类型的列中不同值的个数，返回值和
COUNT(DISTINCT expr) 相同。

`INTERSECT_COUNT(bitmap_column_to_count, filter_column, filter_values ...)`: 计算满足
filter_column 过滤条件的多个 bitmap 的交集的基数值
bitmap_column_to_count 是 bitmap 类型的列，filter_column 是变化的维度列，filter_values 是维度取值列表。

`BITMAP_INTERSECT(expr)`: 计算这一组 bitmap 值的交集，返回新的 bitmap。

## 示例

下面的 SQL 以上面的 pv_bitmap table 为例。

计算 user_id 的去重值:

```SQL
select bitmap_union_count(user_id)
from pv_bitmap;

select bitmap_count(bitmap_union(user_id))
from pv_bitmap;
```

计算 id 的去重值:

```SQL
select bitmap_union_int(id)
from pv_bitmap;
```

计算 user_id 的 留存:

```SQL
select intersect_count(user_id, page, 'meituan') as meituan_uv,
    intersect_count(user_id, page, 'waimai') as waimai_uv,
    intersect_count(user_id, page, 'meituan', 'waimai') as retention -- 在 'meituan' 和 'waimai' 两个页面都出现的用户数
from pv_bitmap
where page in ('meituan', 'waimai');
```
