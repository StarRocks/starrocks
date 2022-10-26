# Bitmap 索引

本文介绍了如何创建和管理 bitmap（位图）索引，以及 bitmap 索引的适用场景。

Bitmap 索引是一种特殊的数据库索引技术，其使用 bitmap 进行储存和计算操作，能够提高指定列的查询效率。

## 注意事项

- Bitmap 索引适用于满足以下条件的列：
  - 基数较低，值大量重复，例如 ENUM 类型的列。如列基数较高，推荐使用 [Bloomfilter 索引](/using_starrocks/Bloomfilter_index.md)。
  - 可使用等值条件查询或者可转化为等值条件查询。
- 主键模型和明细模型中所有列都可以创建 bitmap 索引；聚合模型和更新模型中，只有维度列（即 Key 列）支持创建 bitmap 索引。
- 不支持为 FLOAT、DOUBLE、BOOLEAN 和 DECIMAL 类型的列创建 bitmap 索引。

## 创建索引

- 建表时创建 bitmap 索引。

    ```SQL
    CREATE TABLE d0.table_hash
    (
        k1 TINYINT,
        k2 DECIMAL(10, 2) DEFAULT "10.5",
        v1 CHAR(10) REPLACE,
        v2 INT SUM,
        INDEX index_name (column_name) USING BITMAP COMMENT ''
    )
    ENGINE = olap
    AGGREGATE KEY(k1, k2)
    COMMENT "my first starrocks table"
    DISTRIBUTED BY HASH(k1) BUCKETS 32
    PROPERTIES ("storage_type" = "column");
    ```

    其中有关索引部分参数说明如下：

    | **参数**    | **必选** | **说明**                                                     |
    | ----------- | -------- | ------------------------------------------------------------ |
    | index_name  | 是       | bitmap 索引名称。                                            |
    | column_name | 是       | 创建 bitmap 索引的列名，您可以指定多个列名，即在建表时可同时为多个列创建 bitmap 索引。 |
    | COMMENT     | 否       | 索引备注。                                                   |

    关于建表的其他参数说明，参见 [CREATE TABLE](/sql-reference/sql-statements/data-definition/CREATE%20TABLE.md)。

- 建表后使用 CREATE INDEX 语句创建 bitmap 索引。详细参数说明和示例，参见 [CREATE INDEX](/sql-reference/sql-statements/data-definition/CREATE%20INDEX.md)。

    ```SQL
    CREATE INDEX index_name ON table_name (column_name) [USING BITMAP] [COMMENT ''];
    ```

## 查看索引

查看指定表的所有 bitmap 索引。详细参数和返回结果说明，参见 [SHOW INDEX](/sql-reference/sql-statements/Administration/SHOW%20INDEX.md)。

```SQL
SHOW INDEX[ES] FROM [db_name.]table_name [FROM db_name];
```

或

```SQL
SHOW KEY[S] FROM [db_name.]table_name [FROM db_name];
```

> 说明：创建 bitmap 索引为异步过程，使用如上语句只能查看到已经创建完成的索引。

## 删除索引

删除指定表的某个 bitmap 索引。详细参数说明和示例，参见 [DROP INDEX](/sql-reference/sql-statements/data-definition/DROP%20INDEX.md)。

```SQL
DROP INDEX index_name ON [db_name.]table_name;
```

## 适用场景

- **单个非前缀索引列查询**：如果一个查询条件命中前缀索引列，StarRocks 即可使用[前缀索引](/table_design/Sort_key.md)提高查询效率，快速返回查询结果。但是前缀索引的长度有限，如果想要提高一个非前缀索引列的查询效率，即可以为这一列创建 bitmap 索引。
- **多个非前缀索引列查询**：bitmap 索引使用位运算，速度较快，所以在多列查询的场景中，可以通过为每列创建 bitmap 索引来提高查询速度。需要注意的是创建 bitmap 索引会消耗额外的存储空间。

> 说明：如要了解一个查询是否命中了 bitmap 索引，可查看该查询的 Profile。关于如何查看 Profile，参见[分析查询](/administration/Query_planning.md)。

### **单个非前缀索引列查询**

假设表`table1`中有一列名为`Platform`。如下图所示，该列的取值情况有 2 种：`Android`和`Ios`。`Platform`列不是前缀索引列，如果想要提高该列的查询效率即可为该列创建 bitmap 索引。

![figure1](/assets/3.6.1-2.png)

执行如下命令为`Platform` 列创建 bitmap 索引。

```SQL
CREATE INDEX index1 ON table1 (Platform) USING BITMAP COMMENT 'index1';
```

执行命令后，bitmap 索引生成的过程如下：

1. 构建字典：StarRocks 根据 `Platform` 列的取值构建一个字典，将 `Android` 和 `Ios` 分别映射为 INT 类型的编码值：`0` 和 `1`。
2. 生成索引：StarRocks 根据字典的编码值生成 bitmap。因为`Android`出现在第 1 行、第 2 行和第 3 行，所以`Android`的 bitmap 是`1110`；`Ios`出现在第 4 行，所以`Ios`的 bitmap 是`0001`。

如果执行一个 SQL 查询`select xxx from table where Platform = Ios`，那么 StarRocks 会先查找字典，得到`Ios`的编码值是`1`，然后再去查找 bitmap，得到`Ios`对应的 bitmap 是 `0001`，也就是说只有第 4 行数据符合查询条件。那么 StarRocks 就会跳过前 3 行，只读取第 4 行数据。

### **多个非前缀索引列查询**

假设表`table1`中除了`Platform`列还有`Producer`列。如下图所示：

- `Platform`列取值情况有 2 种：`Android`和`Ios`。
- `Producer`列取值情况有 3 种：`P1`、 `P2`和`P3`。

这两列均不是前缀索引列，如果想要提高这两列的查询效率即可为每一列创建 bitmap 索引。

![figure2](/assets/3.6.1-3.png)

执行如下命令为`Platform`列创建 bitmap 索引。

```SQL
CREATE INDEX index1 ON table1 (Platform) USING BITMAP COMMENT 'index1';
```

执行如下命令为`Producer`列创建 bitmap 索引。

```SQL
CREATE INDEX index2 ON table1 (Producer) USING BITMAP COMMENT 'index2';
```

以上两个命令执行后，StarRocks 会为`Platform`和`Producer`列分别构建一个字典，然后再根据字典生成 bitmap。

- `Platform`列：`Android`的 bitmap 为 `1110`；`Ios`的 bitmap 为 `0001`。
- `Producer`列： `P1`的 bitmap 为 `1010`；`P2`的 bitmap 为`0100`；`P3`的 bitmap 为 `0001`。

如果执行一个 SQL 查询 `select xxx from table where Platform = Android and Producer = P1`，那么：

1. StarRocks 会同时查找`Platform` 和`Producer`的字典，得到`Android`的编码值为`0`，对应的 bitmap 为`1110`；`P1`的编码值为`0`，对应的 bitmap 为`1010`。
2. 因为`Platform = Android`和`Producer = P1` 这两个查询条件是 and 关系，所以 StarRocks 会对两个 bitmap 进行位运算 `1110 & 1010`，得到最终结果`1010`。
3. 根据最终结果，StarRocks 只读取第 1 行和第 3 行数据，不会读取所有数据。
