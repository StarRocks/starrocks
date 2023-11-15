# 排序键

## 排序列的原理

StarRocks中为加速查询，在内部组织并存储数据时，会把表中数据按照指定的列进行排序，这部分用于排序的列（可以是一个或多个列），可以称之为Sort Key。**明细模型**中Sort Key就是指定的用于排序的列（即 DUPLICATE KEY 指定的列），**聚合模型**中Sort Key列就是用于聚合的列（即 AGGREGATE KEY 指定的列），**更新模型**中Sort Key就是指定的满足唯一性约束的列（即 UNIQUE KEY 指定的列）。下图中的建表语句中Sort Key都为 (site\_id、city\_code)。

~~~SQL
CREATE TABLE site_access_duplicate
(
site_id INT DEFAULT '10',
city_code SMALLINT,
user_name VARCHAR(32) DEFAULT '',
pv BIGINT DEFAULT '0'
)
DUPLICATE KEY(site_id, city_code)
DISTRIBUTED BY HASH(site_id) BUCKETS 10;

CREATE TABLE site_access_aggregate
(
site_id INT DEFAULT '10',
city_code SMALLINT,
pv BIGINT SUM DEFAULT '0'
)
AGGREGATE KEY(site_id, city_code)
DISTRIBUTED BY HASH(site_id) BUCKETS 10;

CREATE TABLE site_access_unique
(
site_id INT DEFAULT '10',
city_code SMALLINT,
user_name VARCHAR(32) DEFAULT '',
pv BIGINT DEFAULT '0'
)
UNIQUE KEY(site_id, city_code)
DISTRIBUTED BY HASH(site_id) BUCKETS 10;
~~~

:-: 图 5.1 ：三种建表模型分别对应的Sort Key

图5.1中，各表数据都依照site\_id、city\_code这两列排序。这里有两点需要注意：

1. 排序列的定义必须出现在建表语句中其他列的定义之前。以图5.1中的建表语句为例，三个表的排序列可以是site\_id、city\_code，或者site\_id、city\_code、user\_name，但不能是city\_code、user\_name，或者site\_id、city\_code、pv。
2. 排序列的顺序是由create table语句中的列顺序决定的。DUPLICATE/UNIQUE/AGGREGATE KEY中顺序需要和create table语句保持一致。以site\_access\_duplicate表为例，也就是说下面的建表语句会报错。

~~~ SQL
-- 错误的建表语句
CREATE TABLE site_access_duplicate
(
site_id INT DEFAULT '10',
city_code SMALLINT,
user_name VARCHAR(32) DEFAULT '',
pv BIGINT DEFAULT '0'
)
DUPLICATE KEY(city_code, site_id)
DISTRIBUTED BY HASH(site_id) BUCKETS 10;

-- 正确的建表语句
CREATE TABLE site_access_duplicate
(
    site_id INT DEFAULT '10',
    city_code SMALLINT,
    user_name VARCHAR(32) DEFAULT '',
    pv BIGINT DEFAULT '0'
)
DUPLICATE KEY(site_id, city_code)
DISTRIBUTED BY HASH(site_id) BUCKETS 10;
~~~

:-: 图 5.2 ：DUPLICATE KEY列顺序与CREATE TABLE中不一致

再来看一下排序列在查询中的效果，图1中排序列的效果可分三种情况进行描述：

1. 用户查询时如果条件包含上述两列，则可以大幅地降低扫描数据行，如：  
    select sum(pv) from site\_access\_duplicate where site\_id = 123 and city\_code = 2;
2. 如果查询只包含site\_id一列，也能定位到只包含site\_id的数据行，如：  
    select sum(pv) from site\_access\_duplicate where site\_id = 123;
3. 如果查询只包含city\_code一列，那么需要扫描所有的数据行，排序的效果相当于大打折扣，如：  
    select sum(pv) from site\_access\_duplicate where city\_code = 2;

在第一个case中，为了定位到数据行的位置，需进行二分查找，以找到指定区间。假设数据行非常多，直接对site\_id, city\_code进行二分查找，需要把两列数据都加载到内存中，这会消耗大量内存空间。为优化这个细节，StarRocks在Sort Key的基础上引入稀疏的shortkey index，Sort Index的内容会比数据量少1024倍，因此会全量缓存在内存中，实际查找的过程中可以有效加速查询。当Sort Key列数非常多时，会占用大量内存, 为了避免这种情况, 对shortkey index索引项做了限制:

* shortkey 的列只能是排序键的前缀;
* shortkey 列数不超过3;
* 字节数不超过36字节;
* 不包含FLOAT/DOUBLE类型的列;
* VARCHAR类型列只能出现一次, 并且是末尾位置;
* 当shortkey index的末尾列为CHAR或者VARCHAR类型时, shortkey的长度会超过36字节;
* 当用户在建表语句中指定PROPERTIES `{short\_key = "integer"}`时, 可突破上述限制;

### 3.4.2 如何选择排序列

从上面的介绍可以看出，如果用户在查询site\_access\_duplicate表时只选择city\_code做查询条件，排序列相当于失去了功效。因此排序列的选择是和查询模式息息相关的，经常作为查询条件的列建议放在Sort Key中。

当Sort Key涉及多个列的时候，谁先谁后也有讲究，区分度高、经常查询的列建议放在前面。在site\_access\_duplicate表中，city\_code的取值个数是固定的（城市数目是固定的），而site\_id的取值个数要比city\_code大得多，而且还在不断变多，因此site\_id区分度就比city\_code要高不少。

还是以site\_access\_duplicate表为例：

* 如果用户需要经常按site\_id+city\_code的组合进行查询，那么把site\_id放在Sort Key第一列就是更加有效的一种方式。
* 如果用户需要经常用city\_code进行查询，偶尔按照site\_id+city\_code组合查询，那么把city\_code放在Sort Key的第一列就更为合适。
* 当然有一种极端情况，就是按site\_id+city\_code组合查询、以及city\_code单独查询的比例不相上下。那么这个时候，可以创建一个city\_code为第一列的RollUp表，RollUp表会为city\_code再建一个Sort Index。

### 3.4.3 注意事项

由于StarRocks的shortkey索引大小固定（只有36字节），所以不会存在内存膨胀的问题。需要注意的是：

1. 排序列中包含的列必须是从第一列开始，并且连续的。
2. 排序列的顺序是由create table语句中的列顺序决定的。
3. Sort Key不应该包含过多的列。如果选择了大量的列用于Sort Key，那么排序的开销会导致数据导入的开销增加。
4. 在大多数时候，Sort Key的前面几列也能很准确的定位到数据行所在的区间，更多列的排序也不会带来查询的提升。
