# 数据模型介绍

目前，StarRocks根据摄入数据和实际存储数据之间的映射关系, 其中明细表对应明细模型（Duplicate Key），聚合表对应聚合模型（Aggregate Key），更新表对应更新模型（Unique Key）和主键模型（Primary Key）。为了描述方便, 我们借鉴关系模式中的主键概念, 称StarRocks表的维度列的取值构成数据表的排序键, StarRocks的排序键对比传统的主键具有:

* 数据表所有维度列构成排序键, 所以后文中提及的排序列, key列本质上都是维度列。
* 排序键可重复, 不必满足唯一性约束。
* 数据表的每一列, 以排序键的顺序, 聚簇存储。
* 排序键使用稀疏索引。

对于摄入(ingest)的主键重复的多行数据, 填充于(populate)数据表中时, 按照三种处理方式划分:

* 明细模型:  表中存在主键重复的数据行, 和摄入数据行一一对应, 用户可以召回所摄入的全部历史数据。
* 聚合模型:  表中不存在主键重复的数据行, 摄入的主键重复的数据行合并为一行, 这些数据行的指标列通过聚合函数合并, 用户可以召回所摄入的全部历史数据的累积结果, 但无法召回全部历史数据。
* 更新模型&主键模型:  聚合模型的特殊情形, 主键满足唯一性约束, 最近导入的数据行, 替换掉其他主键重复的数据行. 相当于在聚合模型中, 为数据表的指标列指定的聚合函数为REPLACE, REPLACE函数返回一组数据中的最新数据。

需要注意:

* 建表语句, 排序列的定义必须出现在指标列定义之前。
* 排序列在建表语句中的出现次序为数据行的多重排序的次序。
* 排序键的稀疏索引(Shortkey Index)会选择排序键的若干前缀列。

本章介绍特种模型的特点和使用场景, 帮助用户选择匹配业务需求的最佳模型。

## 明细模型

### 适用场景

StarRocks建表的默认模型是明细模型（Duplicate Key）。

  <br/>

一般用明细模型来处理的场景有如下特点：

1. 需要保留原始的数据（例如原始日志，原始操作记录等）来进行分析；
2. 查询方式灵活, 不局限于预先定义的分析方式, 传统的预聚合方式难以命中;
3. 数据更新不频繁。导入数据的来源一般为日志数据或者是时序数据,  以追加写为主要特点, 数据产生后就不会发生太多变化。

### 原理

用户可以指定数据表的排序列, 没有明确指定的情况下, 那么StarRocks会为表选择默认的几个列作为排序列。这样，在查询中，有相关排序列的过滤条件时，StarRocks能够快速地过滤数据，降低整个查询的时延。

注意：在向StarRocks明细模型表中导入完全相同的两行数据时，StarRocks会认为是两行数据。

### 如何使用

数据表默认采用明细模型. 排序列使用shortkey index, 可快速过滤数据. 用户可以考虑将过滤条件中频繁使用的维度列的定义放置其他列的定义之前. 例如用户经常查看某时间范围的某一类事件的数据，可以将事件时间和事件类型作为排序键。

  <br/>

以下是一个使用明细模型创建数据表的例子

* 其中`DUPLICATE KEY(event_time, event_type)`说明采用明细模型, 并且指定了排序键, 并且排序列的定义在其他列定义之前。

~~~sql
CREATE TABLE IF NOT EXISTS detail (
    event_time DATETIME NOT NULL COMMENT "datetime of event",
    event_type INT NOT NULL COMMENT "type of event",
    user_id INT COMMENT "id of user",
    device_code INT COMMENT "device of ",
    channel INT COMMENT ""
)
DUPLICATE KEY(event_time, event_type)
DISTRIBUTED BY HASH(user_id) BUCKETS 8
~~~

### 注意事项

1. 充分利用排序列，在建表时将经常在查询中用于过滤的列放在表的前面，这样能够提升查询速度。
2. 明细模型中, 可以指定部分的维度列为排序键; 而聚合模型和更新模型中, 排序键只能是全体维度列。

  <br/>

## 聚合模型

### 适用场景

在数据分析领域，有很多需要对数据进行统计和汇总操作的场景，就需要使用聚合模型（Aggregate Key）。比如:

* 分析网站或APP访问流量，统计用户的访问总时长、访问总次数;
* 广告厂商为广告主提供的广告点击总量、展示总量、消费统计等;
* 分析电商的全年的交易数据, 获得某指定季度或者月份的, 各人口分类(geographic)的爆款商品。

适合采用聚合模型来分析的场景具有如下特点：

1. 业务方进行的查询为汇总类查询，比如sum、count、max等类型的查询；
2. 不需要召回原始的明细数据；
3. 老数据不会被频繁更新，只会追加新数据。

### 原理

StarRocks会将指标列按照相同维度列进行聚合。当多条数据具有相同的维度时，StarRocks会把指标进行聚合。从而能够减少查询时所需要的处理的数据量，进而提升查询的效率。

  <br/>

以下面的原始数据为例：

|   Date  |   Country  |   PV  |
| :---: | :---: | :---: |
|  2020.05.01   |  CHN   |  1   |
|  2020.05.01   |  CHN   |  2   |
|  2020.05.01   |  USA   |  3   |
|  2020.05.01   |  USA   |  4   |

在StarRocks聚合模型的表中，存储内容会从四条数据变为两条数据。这样在后续查询处理的时候，处理的数据量就会显著降低：

|   Date  |   Country  |   PV  |
| :---: | :---: | :---: |
|  2020.05.01   |  CHN   |  3   |
|  2020.05.01   |  USA   |  7   |

### 如何使用

在建表时, 只要给指标列的定义指明聚合函数, 就会启用聚合模型; 用户可以使用`AGGREGATE KEY`显式地定义排序键。

以下是一个使用聚合模型创建数据表的例子：

* site\_id, date, city\_code为排序键;
* pv为指标列, 使用聚合函数SUM。

~~~sql
CREATE TABLE IF NOT EXISTS example_db.aggregate_tbl (
    site_id LARGEINT NOT NULL COMMENT "id of site",
    date DATE NOT NULL COMMENT "time of event",
    city_code VARCHAR(20) COMMENT "city_code of user",
    pv BIGINT SUM DEFAULT "0" COMMENT "total page views"
)
DISTRIBUTED BY HASH(site_id) BUCKETS 8;
~~~

### 注意事项

1. 聚合表中数据会分批次多次导入，每次导入会形成一个版本。相同排序键的数据行聚合有三种触发方式:
    1. 数据导入时，数据落盘前的聚合；
    2. 数据落盘后，后台的多版本异步聚合；
    3. 数据查询时，多版本多路归并聚合。
2. 数据查询时，指标列采用先聚合后过滤的方式，把没必有做指标的列存储为维度列。
3. 聚合模型所支持的聚合函数列表请参考 [Create Table 语句说明](../sql-reference/sql-statements/data-definition/CREATE_TABLE.md)。

  <br/>

## 更新模型

### 适用场景

有些分析场景之下，数据会更新, StarRocks采用更新模型来满足这种需求。比如在电商场景中，定单的状态经常会发生变化，每天的订单更新量可突破上亿。在这种量级的更新场景下进行实时数据分析，如果在明细模型下通过delete+insert的方式，是无法满足频繁更新需求的; 因此, 用户需要使用更新模型来满足数据分析需求。如用户需要更加实时/频繁的更新功能，建议使用[主键模型](#主键模型)。

  <br/>

以下是一些适合更新模型的场景特点：

1. 已经写入的数据有大量的更新需求；
2. 需要进行实时数据分析。

### 原理

更新模型中, 排序键满足唯一性约束, 成为主键。

StarRocks存储内部会给每一个批次导入数据分配一个版本号, 同一主键的数据可能有多个版本, 查询时最大(最新)版本的数据胜出。

|  ID   |   value  |   _version  |
| :---: | :---: | :---: |
|  1   |  100   |  1   |
|  1   |  101   |  2   |
|  2   |  100   |  3   |
|  2   |  101   |  4   |
|  2   |  102   |  5   |

具体的示例如上表所示，ID是表的主键，value是表的内容，而\_\_version是StarRocks内部的版本号。其中ID为1的数据有两个导入批次，版本分别为1，2；ID为2的数据有三个批次导入，版本分别为3，4，5。在查询的时候对于ID为1只会返回最新版本2的数据，而对于ID为2只会返回最新版本5的数据，那么用户能够看到的数据如下表所示：

|  ID   |   value  |
| :---: | :---: |
|  1   |  101   |
|  2   |  102   |

通过这种机制，StarRocks可以支持对于频繁更新数据的分析。

### 如何使用

在电商订单分析场景中，经常根据订单状态进行的统计分析。因为订单状态经常改变，而create\_time和order\_id不会改变，并且经常会在查询中作为过滤条件。所以可以将 create\_time和order\_id 两个列作为这个表的主键（即，在建表时用`UNIQUE KEY`关键字定义），这样既能够满足订单状态的更新需求，又能够在查询中进行快速过滤。

  <br/>

以下是一个使用更新模型创建数据表的例子：

* 用`UNIQUE KEY(create_time, order_id)`做主键, 其中create\_time, order\_id为排序列, 其定义在其他列定义之前出现;
* order\_state和total\_price为指标列, 其聚合类型为REPLACE。

~~~sql
CREATE TABLE IF NOT EXISTS detail (
    create_time DATE NOT NULL COMMENT "create time of an order",
    order_id BIGINT NOT NULL COMMENT "id of an order",
    order_state INT COMMENT "state of an order",
    total_price BIGINT COMMENT "price of an order"
)
UNIQUE KEY(create_time, order_id)
DISTRIBUTED BY HASH(order_id) BUCKETS 8
~~~

### 注意事项

1. 导入数据时需要将所有字段补全才能够完成更新操作，即，上述例子中的create\_time、order\_id、order\_state和total\_price四个字段都需必须存在。
2. 对于更新模型的数据读取，需要在查询时完成多版本合并，当版本过多时会导致查询性能降低。所以在向更新模型导入数据时，应该适当降低导入频率，从而提升查询性能。建议在设计导入频率时以满足业务对实时性的要求为准。如果业务对实时性的要求是分钟级别，那么每分钟导入一次更新数据即可，不需要秒级导入。
3. 在查询时，对于value字段的过滤通常在多版本合并之后。将经常过滤字段且不会被修改的字段放在主键上, 能够在合并之前就将数据过滤掉，从而提升查询性能。
4. 因为合并过程需要将所有主键字段进行比较，所以应该避免放置过多的主键字段，以免降低查询性能。如果某个字段只是偶尔会作为查询中的过滤条件存在，不需要放在主键中。

## 主键模型

### 适用场景

相较更新模型，主键模型（Primary Key）可以更好地支持实时/频繁更新的功能。该类型的表要求有唯一的主键，支持对表中的行按主键进行更新和删除操作。

该模型适合需要对数据进行实时更新的场景，特别适合MySQL或其他数据库同步到StarRocks的场景。虽然原有的Unique模型也可以实现对数据的更新，但Merge-on-Read的策略大大限制了查询性能。Primary模型更好地解决了行级别的更新操作，配合Flink-connector-starrocks可以完成MySQL数据库的同步。具体使用方式详见[文档](/loading/Flink-connector-starrocks.md#使用-flink-connector-写入实现-mysql-数据同步)。

需要注意的是：由于存储引擎会为主键建立索引，而在导入数据时会把主键索引加载在内存中，所以主键模型对内存的要求比较高，还不适合主键特别多的场景。目前primary主键存储在内存中，为防止滥用造成内存占满，限制主键字段长度全部加起来编码后不能超过127字节。目前比较适合的两个场景是：

* 数据有冷热特征，即最近几天的热数据才经常被修改，老的冷数据很少被修改。典型的例子如MySQL订单表实时同步到StarRocks中提供分析查询。其中，数据按天分区，对订单的修改集中在最近几天新创建的订单，老的订单完成后就不再更新，因此导入时其主键索引就不会加载，也就不会占用内存，内存中仅会加载最近几天的索引。

![主键1](../assets/3.2-1.png)
>如图所示，数据按天分区，对最近分区的Primary Key相关数据的修改更加频繁。

* 大宽表(数百到数千列)。主键只占整个数据的很小一部分，其内存开销比较低。比如用户状态/画像表，虽然列非常多，但总的用户数不大(千万-亿级别)，主键索引内存占用相对可控。

![主键2](../assets/3.2-2.png)
>如图所示，大宽表中Priamry Key只占一小部分，且数据行数不高。

### 原理

主键模型是由StarRocks全新设计开发的存储引擎支持的，其元数据组织、读取、写入方式和原有的表模型完全不同。

原有的表模型整体上采用了读时合并(Merge-On-Read)的策略，写入时处理简单高效，但是读取(查询)时需要在线合并多版本。由于Merge算子的存在使得谓词无法下推和索引无法使用，严重影响了查询性能。而主键模型通过主键约束，保证同一个主键下仅存在一条记录，这样就完全避免了Merge操作。具体实现步骤：

* StarRocks收到对某记录的更新操作时，会通过主键索引找到该条记录的位置，并对其标记为删除，再插入一条新的记录。相当于把Update改写为Delete+Insert。

* StarRocks收到对某记录的删除操作时，会通过主键索引找到该条记录的位置，对其标记为删除。这样在查询时不影响谓词下推和索引的使用, 保证了查询的高效执行。

可见，相比更新模型，主键模型通过牺牲微小的写入性能和内存占用，极大提升了查询性能。

### 如何使用

#### 建表

和其他数据库类似，在建表时通过`PRIMARY KEY`指定最前的若干列为主键，即可启用主键模型。

例:

以下语句使用主键模型创建一个按天分区的订单表：

~~~sql
create table orders (
    dt date NOT NULL,
    order_id bigint NOT NULL,
    user_id int NOT NULL,
    merchant_id int NOT NULL,
    good_id int NOT NULL,
    good_name string NOT NULL,
    price int NOT NULL,
    cnt int NOT NULL,
    revenue int NOT NULL,
    state tinyint NOT NULL
) PRIMARY KEY (dt, order_id)
PARTITION BY RANGE(`dt`) (
    PARTITION p20210820 VALUES [('2021-08-20'), ('2021-08-21')),
    PARTITION p20210821 VALUES [('2021-08-21'), ('2021-08-22')),
    ...
    PARTITION p20210929 VALUES [('2021-09-29'), ('2021-09-30')),
    PARTITION p20210930 VALUES [('2021-09-30'), ('2021-10-01'))
) DISTRIBUTED BY HASH(order_id) BUCKETS 4
PROPERTIES("replication_num" = "3");
~~~

以下语句只用主键模型创建一个用户状态表:

~~~sql
create table users (
    user_id bigint NOT NULL,
    name string NOT NULL,
    email string NULL,
    address string NULL,
    age tinyint NULL,
    sex tinyint NULL,
    last_active datetime,
    property0 tinyint NOT NULL,
    property1 tinyint NOT NULL,
    property2 tinyint NOT NULL,
    property3 tinyint NOT NULL,
    ....
) PRIMARY KEY (user_id)
DISTRIBUTED BY HASH(user_id) BUCKETS 4
PROPERTIES("replication_num" = "3");
~~~

注意:

1. 主键列仅支持类型: boolean, tinyint, smallint, int, bigint, largeint, string/varchar, date, datetime, 不允许NULL。
2. 分区列(partition)、分桶列(bucket)必须在主键列中。
3. 和更新模型不同，主键模型允许为非主键列创建bitmap等索引，注意需要建表时指定。
4. 由于其列值可能会更新，主键模型目前还不支持rollup index和物化视图。
5. 暂不支持使用`ALTER TABLE`修改列类型。 `ALTER TABLE`的相关语法说明和示例，请参见 [ALTER TABLE](../sql-reference/sql-statements/data-definition/ALTER_TABLE.md)。
6. 在设计表时应尽量减少主键的列数和大小以节约内存，建议使用int/bigint等占用空间少的类型。暂时不建议使用varchar。建议提前根据表的行数和主键列类型来预估内存使用量，避免出现OOM。内存估算举例：  
  a. 假设表的主键为:  `dt date (4byte), id bigint(8byte) = 12byte`  
  b. 假设热数据有1000W行, 存储3副本  
  c. 则内存占用: `(12 + 9(每行固定开销) ) * 1000W * 3 * 1.5(hash表平均额外开销) = 945M`  
7. 目前主键模型只支持整行更新，还不支持部分列更新。

#### 插入/更新/删除操作

插入/更新/删除目前支持使用导入的方式完成，通过SQL语句(`insert`/`update`/`delete`)来操作数据的功能会在未来版本中支持。目前支持的导入方式有stream load、broker load、routine load、Json数据导入。当前Spark load还未支持。

StarRocks目前不会区分`insert`/`upsert`，所有的操作默认都为`upsert`操作，使用原有的stream load/broker load功能导入数据时默认为upsert操作。

为了在导入中同时支持upsert和delete操作，StarRocks在stream load和broker load语法中加入了特殊字段`__op`。该字段用来表示该行的操作类型，其取值为 0时代表`upsert`操作，取值为1时为`delete`操作。在导入的数据中, 可以添加一列, 用来存储`__op` 操作类型, 其值只能是0(表示`upsert`)或者1(表示`delete`)。

#### 使用 Stream Load / Broker Load 导入

Stream load和broker load的操作方式类似，根据导入的数据文件的操作形式有如下几种情况。这里通过一些例子来展示具体的导入操作：

1. 当导入的数据文件只有`upsert`操作时可以不添加`__op`列。可以指定`__op`为`upsert`，也可以不做任何指定，StarRocks会默认导入为`upsert`。例如想要向表t中导入如下内容：

    ~~~text
    # 导入内容
    0,aaaa
    1,bbbb
    2,\N
    4,dddd
    ~~~

    Stream load导入语句：

    ~~~Bash
    #不指定__op
    curl --location-trusted -u root: -H "label:lineorder" -H "column_separator:," -T demo.csv http://localhost:8030/api/demo/demo/_stream_load
    #指定__op
    curl --location-trusted -u root: -H "label:lineorder" -H "column_separator:," -H "columns:__op='upsert'" -T demo.csv http://localhost:8030/api/demo/demo/_stream_load
    ~~~

    Broker load导入语句：

    ~~~Bash
    #不指定__op
    load label demo.demo (
      data infile("hdfs://localhost:9000/demo.csv")
      into table t
      format as "csv"
    ) with broker "broker1";

    #指定__op
    load label demo.demo (
      data infile("hdfs://localhost:9000/demo.csv")
      into table t
      format as "csv"
      set (__op='upsert')
    ) with broker "broker1";
    ~~~

2. 当导入的数据文件只有delete操作时，也可以不添加`__op`列，只需指定`__op`为delete。例如想要删除如下内容：

    ~~~text
    #导入内容
    1,bbbb
    4,dddd
    ~~~

    注意：`delete`虽然只用到primary key列，但同样要提供全部的列，与`upsert`保持一致。

    Stream load导入语句：
  
      ~~~bash
      curl --location-trusted -u root: -H "label:lineorder" -H "column_separator:," -H "columns:__op='delete'" -T demo.csv http://localhost:8030/api/demo/demo/_stream_load
      ~~~
  
    Broker load导入语句：
  
      ~~~bash
      load label demo.ttt3 (
        data infile("hdfs://localhost:9000/demo.csv")
        into table t
        format as "csv"
        set (__op='delete')
      ) with broker "broker1";  
      ~~~

3. 当导入的数据文件中包含upsert和delete混合时，需要指定额外的`__op`来表明操作类型。例如想要导入如下内容：

    ~~~text
    1,bbbb,1
    4,dddd,1
    5,eeee,0
    6,ffff,0
    ~~~

    注意：

    * `delete`虽然只用到primary key列，但同样要提供全部的列，与`upsert`保持一致
    * 上述导入内容表示删除1、4行，添加5、6行

    Stream load导入语句：

      ~~~bash
      curl --location-trusted -u root: -H "label:lineorder" -H "column_separator:," -H "columns:c1,c2,c3,pk=c1,col0=c2,__op=c3" -T demo.csv http://localhost:8030/api/demo/demo/_stream_load
      ~~~

    其中，指定了`__op`为第三列。

    Brokder load导入语句：

      ~~~bash
      load label demo.ttt3 (
          data infile("hdfs://localhost:9000/demo.csv")
          into table t
          format as "csv"
         (c1, c2, c3)
          set (pk = c1, col0 = c2, __op=c3)
      ) with broker "broker1";
      ~~~

    其中，指定了`__op`为第三列。

#### 使用 Routine Load 导入

可以在创建routine load的语句中，在columns最后增加一列，指定为在`__op`。在真实导入中，`__op`为0则表示`upsert`操作，为1则表示`delete`操作。例如导入如下内容：

~~~bash
2020-06-23  2020-06-23 00:00:00 beijing haidian 1   -128    -32768  -2147483648    0
2020-06-23  2020-06-23 00:00:01 beijing haidian 0   -127    -32767  -2147483647    1
2020-06-23  2020-06-23 00:00:02 beijing haidian 1   -126    -32766  -2147483646    0
2020-06-23  2020-06-23 00:00:03 beijing haidian 0   -125    -32765  -2147483645    1
2020-06-23  2020-06-23 00:00:04 beijing haidian 1   -124    -32764  -2147483644    0
~~~

Routine load导入语句：

~~~bash
CREATE ROUTINE LOAD routine_load_basic_types_1631533306858 on primary_table_without_null 
COLUMNS (k1,k2,k3,k4,k5,v1,v2,v3,__op),
COLUMNS TERMINATED BY '\t' 
PROPERTIES (
    "desired_concurrent_number"="1",
    "max_error_number"="1000",
    "max_batch_interval"="5"
) FROM KAFKA (
    "kafka_broker_list"="172.26.92.141:9092",
    "kafka_topic"="data-for-basic-types",
    "kafka_partitions"="0,1,2,3,4",
    "kafka_offsets"="OFFSET_BEGINNING,OFFSET_BEGINNING,OFFSET_BEGINNING,OFFSET_BEGINNING,OFFSET_BEGINNING"
);
~~~
