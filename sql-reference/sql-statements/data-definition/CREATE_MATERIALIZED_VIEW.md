# CREATE MATERIALIZED VIEW

## 功能

该语句用于创建物化视图。

说明:

**异步** 语法，调用成功后仅表示创建物化视图的任务提交成功，用户需要先通过 ` show alter table rollup ` 来查看物化视图的创建进度。

在显示 FINISHED 后既可通过 `desc [table_name] all` 命令来查看物化视图的 schema 了。

删除物化视图参考 [DROP MATERIALIZED](../data-definition/DROP_MATERIALIZED_VIEW.md) 章节。

## 语法

```sql
CREATE MATERIALIZED VIEW [MV name] as [query]
[PROPERTIES ("key" = "value")]
```

## 参数

1. MV name

    物化视图的名称，必填项。

    相同表的物化视图名称不可重复。

2. query

    用于构建物化视图的查询语句，查询语句的结果既物化视图的数据。目前支持的 query 格式为:

    ```sql
    SELECT select_expr[, select_expr ...]
    FROM [Base view name]
    GROUP BY column_name[, column_name ...]
    ORDER BY column_name[, column_name ...];
    ```

    语法和查询语句语法一致。

    **select_expr**: 物化视图的 schema 中所有的列。
    + 仅支持不带表达式计算的单列，聚合列。
    + 其中聚合函数目前仅支持 SUM, MIN, MAX 三种，且聚合函数的参数只能是不带表达式计算的单列。
    + 至少包含一个单列。
    + 所有涉及到的列，均只能出现一次。

    **base view name**: 物化视图的原始表名，必填项。
    + 必须是单表，且非子查询

    **group by**: 物化视图的分组列，选填项。
    + 不填则数据不进行分组。

    **order by**: 物化视图的排序列，选填项。
    + 排序列的声明顺序必须和 select_expr 中列声明顺序一致。
    + 如果不声明 order by，则根据规则自动补充排序列。
    如果物化视图是聚合类型，则所有的分组列自动补充为排序列。
    如果物化视图是非聚合类型，则前 36 个字节自动补充为排序列。如果自动补充的排序个数小于 3 个，则前三个作为排序列。
    + 如果 query 中包含分组列的话，则排序列必须和分组列一致。

3. properties

    声明物化视图的一些配置，选填项。

    ```sql
    PROPERTIES ("key" = "value", "key" = "value" ...)
    ```

    以下几个配置，均可声明在此处：

    ```plain text
    short_key: 排序列的个数。
    timeout: 物化视图构建的超时时间。
    ```

### 聚合函数匹配关系

使用物化视图搜索时，原始查询语句将会被自动改写并用于查询物化视图中保存的中间结果。下表展示了原始查询聚合函数和构建物化视图用到的聚合函数的匹配关系。您可以根据您的业务场景选择对应的聚合函数构建物化视图。

| **原始查询聚合函数**                                   | **物化视图构建聚合函数** |
| ------------------------------------------------------ | ------------------------ |
| sum                                                    | sum                      |
| min                                                    | min                      |
| max                                                    | max                      |
| count                                                  | count                    |
| bitmap_union, bitmap_union_count, count(distinct)      | bitmap_union             |
| hll_raw_agg, hll_union_agg, ndv, approx_count_distinct | hll_union                |

## 注意事项

+ 当前版本暂时不支持同时创建多个物化视图。仅当当前创建任务完成时，方可执行下一个创建任务。
+ 同步物化视图仅支持单列聚合函数，不支持形如 `sum(a+b)` 的查询语句。
+ 同步物化视图仅支持对同一列数据使用一种聚合函数，不支持形如 `select sum(a), min(a) from table` 的查询语句。
+ 同步物化视图中使用聚合函数需要与 GROUP BY 语句一起使用，且 SELECT 的列中至少包含一个分组列。
+ 同步物化视图创建语句不支持 JOIN、WHERE 以及 GROUP BY 的 HAVING 子句。
+ 使用 ALTER TABLE DROP COLUMN 删除基表中特定列时，需要保证该基表所有同步物化视图中不包含被删除列，否则无法进行删除操作。如果必须删除该列，则需要将所有包含该列的同步物化视图删除，然后进行删除列操作。
+ 为一张表创建过多的同步物化视图会影响导入的效率。导入数据时，同步物化视图和基表数据将同步更新，如果一张基表包含 n 个物化视图，向基表导入数据时，其导入效率大约等同于导入 n 张表，数据导入的速度会变慢。

## 示例

Base 表结构为：

```sql
mysql> desc duplicate_table;
+-------+--------+------+------+---------+-------+
| Field | Type   | Null | Key  | Default | Extra |
+-------+--------+------+------+---------+-------+
| k1    | INT    | Yes  | true | N/A     |       |
| k2    | INT    | Yes  | true | N/A     |       |
| k3    | BIGINT | Yes  | true | N/A     |       |
| k4    | BIGINT | Yes  | true | N/A     |       |
+-------+--------+------+------+---------+-------+
```

1. 创建一个仅包含原始表 （k1，k2）列的物化视图。

    ```sql
    create materialized view k1_k2 as
    select k1, k2 from duplicate_table;
    ```

    物化视图的 schema 如下图，物化视图仅包含两列 k1、k2 且不带任何聚合。

    ```sql
    +-----------------+-------+--------+------+------+---------+-------+
    | IndexName       | Field | Type   | Null | Key  | Default | Extra |
    +-----------------+-------+--------+------+------+---------+-------+
    | k1_k2           | k1    | INT    | Yes  | true | N/A     |       |
    |                 | k2    | INT    | Yes  | true | N/A     |       |
    +-----------------+-------+--------+------+------+---------+-------+
    ```

2. 创建一个以 k2 为排序列的物化视图。

    ```sql
    create materialized view k2_order as
    select k2, k1 from duplicate_table order by k2;
    ```

    物化视图的 schema 如下图，物化视图仅包含两列 k2、k1，其中 k2 列为排序列，不带任何聚合。

    ```sql
    +-----------------+-------+--------+------+-------+---------+-------+
    | IndexName       | Field | Type   | Null | Key   | Default | Extra |
    +-----------------+-------+--------+------+-------+---------+-------+
    | k2_order        | k2    | INT    | Yes  | true  | N/A     |       |
    |                 | k1    | INT    | Yes  | false | N/A     | NONE  |
    +-----------------+-------+--------+------+-------+---------+-------+
    ```

3. 创建一个以 k1，k2 分组，k3 列为 SUM 聚合的物化视图。

    ```sql
    create materialized view k1_k2_sumk3 as
    select k1, k2, sum(k3) from duplicate_table group by k1, k2;
    ```

    物化视图的 schema 如下图，物化视图包含两列 k1、k2，sum(k3) 其中 k1、k2 为分组列，sum(k3) 为根据 k1、k2 分组后的 k3 列的求和值。

    由于物化视图没有声明排序列，且物化视图带聚合数据，系统默认补充分组列 k1、k2 为排序列。

    ```sql
    +-----------------+-------+--------+------+-------+---------+-------+
    | IndexName       | Field | Type   | Null | Key   | Default | Extra |
    +-----------------+-------+--------+------+-------+---------+-------+
    | k1_k2_sumk3     | k1    | INT    | Yes  | true  | N/A     |       |
    |                 | k2    | INT    | Yes  | true  | N/A     |       |
    |                 | k3    | BIGINT | Yes  | false | N/A     | SUM   |
    +-----------------+-------+--------+------+-------+---------+-------+
    ```

4. 创建一个去除重复行的物化视图。

    ```sql
    create materialized view deduplicate as
    select k1, k2, k3, k4 from duplicate_table group by k1, k2, k3, k4;
    ```

    物化视图 schema 如下图，物化视图包含 k1、k2、k3、k4 列，且不存在重复行。

    ```sql
    +-----------------+-------+--------+------+-------+---------+-------+
    | IndexName       | Field | Type   | Null | Key   | Default | Extra |
    +-----------------+-------+--------+------+-------+---------+-------+
    | deduplicate     | k1    | INT    | Yes  | true  | N/A     |       |
    |                 | k2    | INT    | Yes  | true  | N/A     |       |
    |                 | k3    | BIGINT | Yes  | true  | N/A     |       |
    |                 | k4    | BIGINT | Yes  | true  | N/A     |       |
    +-----------------+-------+--------+------+-------+---------+-------+

    ```

5. 创建一个不声明排序列的非聚合型物化视图。

    all_type_table 的 schema 如下：

    ```sql
    +-------+--------------+------+-------+---------+-------+
    | Field | Type         | Null | Key   | Default | Extra |
    +-------+--------------+------+-------+---------+-------+
    | k1    | TINYINT      | Yes  | true  | N/A     |       |
    | k2    | SMALLINT     | Yes  | true  | N/A     |       |
    | k3    | INT          | Yes  | true  | N/A     |       |
    | k4    | BIGINT       | Yes  | true  | N/A     |       |
    | k5    | DECIMAL(9,0) | Yes  | true  | N/A     |       |
    | k6    | DOUBLE       | Yes  | false | N/A     | NONE  |
    | k7    | VARCHAR(20)  | Yes  | false | N/A     | NONE  |
    +-------+--------------+------+-------+---------+-------+
    ```

    物化视图包含 k3、k4、k5、k6、k7 列，且不声明排序列，则创建语句如下：

    ```sql
    create materialized view mv_1 as
    select k3, k4, k5, k6, k7 from all_type_table;
    ```

    系统默认补充的排序列为 k3、k4、k5 三列。这三列类型的字节数之和为 4(INT) + 8(BIGINT) + 16(DECIMAL) = 28 < 36。所以补充的是这三列作为排序列。
    物化视图的 schema 如下，可以看到其中 k3、k4、k5 列的 key 字段为 true，也就是排序列。k6、k7 列的 key 字段为 false，也就是非排序列。

    ```sql
    +----------------+-------+--------------+------+-------+---------+-------+
    | IndexName      | Field | Type         | Null | Key   | Default | Extra |
    +----------------+-------+--------------+------+-------+---------+-------+
    | mv_1           | k3    | INT          | Yes  | true  | N/A     |       |
    |                | k4    | BIGINT       | Yes  | true  | N/A     |       |
    |                | k5    | DECIMAL(9,0) | Yes  | true  | N/A     |       |
    |                | k6    | DOUBLE       | Yes  | false | N/A     | NONE  |
    |                | k7    | VARCHAR(20)  | Yes  | false | N/A     | NONE  |
    +----------------+-------+--------------+------+-------+---------+-------+
    ```
