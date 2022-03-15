# 数组

## 背景

数组，作为数据库的一种扩展类型，在 PG、ClickHouse、Snowflake 等系统中都有相关特性支持，可以广泛的应用于A/B Test对比、用户标签分析、人群画像等场景。StarRocks 当前支持了 多维数组嵌套、数组切片、比较、过滤等特性。

下面简要介绍一些是使用方式，更详细的函数语法请查看 `参考手册 > 函数参考 > 数组函数`。

<br/>

## 数组使用

### 数组定义

下面是在StarRocks中定义数组列的例子

~~~SQL
-- 一维数组
create table t0(
  c0 INT,
  c1 ARRAY<INT>
)
duplicate key(c0)
distributed by hash(c0) buckets 3;  -- 以分3个桶为例。

-- 定义嵌套数组
create table t1(
  c0 INT,
  c1 ARRAY<ARRAY<VARCHAR(10)>>
)
duplicate key(c0)
distributed by hash(c0) buckets 3;
~~~

如上，数组列的定义形式为 `ARRAY`，其中 TYPE 是数组元素类型，默认 nullable，暂时不支持指定元素类型为 NOT NULL，但是可以定义数组本身为 NOT NULL。

~~~SQL
create table t2(
  c0 INT,
  c1 ARRAY<INT> NOT NULL
)
duplicate key(c0)
distributed by hash(c0) buckets 3;
~~~

数组类型有以下限制

* 只能在duplicate table中定义数组列（2.1版本开始支持Primary key和Unique key中使用数组类型）
* 数组列不能作为key列(以后可能支持)
* 数组列不能作为distribution列
* 数组列不能作为partition列

<br/>

### 在SQL中构造数组

可以在SQL中通过中括号（ "[" 和 "]" ）来构造数组常量，每个数组元素通过逗号(",")分割

~~~SQL
select [1, 2, 3] as numbers;
select ["apple", "orange", "pear"] as fruit;
select [true, false] as booleans;
~~~

当数组元素具有不同类型时，StarRocks会自动推导出合适的类型(supertype)

~~~SQL
select [1, 1.2] as floats;
select [12, "100"]; -- 结果是 ["12", "100"]
~~~

可以使用尖括号(`<>`)显示声明数组类型

~~~SQL
select ARRAY<float>[1, 2];
select ARRAY<INT>["12", "100"]; -- 结果是 [12, 100]
~~~

元素中可以包含NULL

~~~SQL
select [1, NULL];
~~~

对于空数组，可以使用尖括号显示声明其类型，也可以直接写\[\]，此时StarRocks会根据上下文推断其类型，如果无法推断则会报错。

~~~SQL
select [];
select ARRAY<VARCHAR(10)>[];
select array_append([], 10);
~~~

<br/>

### 数组导入

目前有三种方式向StarRocks中写入数组值，insert into 适合小规模数据测试。后面两种适合大规模数据导入。

* **INSERT INTO**

  ~~~SQL
  create table t0(c0 INT, c1 ARRAY<INT>)duplicate key(c0);
  INSERT INTO t0 VALUES(1, [1,2,3]);
  ~~~

* **从ORC/Parquet文件导入**

  StarRocks 中的数组类型，与ORC/Parquet格式中的list结构相对应，不需要额外指定，具体请参考StarRocks 企业文档中 `broker load` 导入相关章节。当前ORC的list结构可以直接导入，Parquet格式正在开发中。

* **从CSV文件导入**

  CSV 文件导入数组，默认采用逗号分隔，可以用 stream load / routine load 导入CSV文本文件或 Kafka 中的 CSV 格式数据。

<br/>

### 数组元素访问

使用中括号（ "[" 和 "]" ）加下标形式访问数组中某个元素，下标从 `1` 开始

~~~Plain Text
mysql> select [1,2,3][1];

+------------+
| [1,2,3][1] |
+------------+
|          1 |
+------------+
1 row in set (0.00 sec)
~~~

如果下标为0，或者为负数，**不会报错，会返回NULL**

~~~Plain Text
mysql> select [1,2,3][0];

+------------+
| [1,2,3][0] |
+------------+
|       NULL |
+------------+
1 row in set (0.01 sec)
~~~

如果下标超过数组大小，**也会返回NULL**

~~~Plain Text
mysql> select [1,2,3][4];

+------------+
| [1,2,3][4] |
+------------+
|       NULL |
+------------+
1 row in set (0.01 sec)
~~~

对于多维数组，可以**递归**访问内部元素

~~~Plain Text
mysql(ARRAY)> select [[1,2],[3,4]][2];

+------------------+
| [[1,2],[3,4]][2] |
+------------------+
| [3,4]            |
+------------------+
1 row in set (0.00 sec)

mysql> select [[1,2],[3,4]][2][1];

+---------------------+
| [[1,2],[3,4]][2][1] |
+---------------------+
|                   3 |
+---------------------+
1 row in set (0.01 sec)
~~~
