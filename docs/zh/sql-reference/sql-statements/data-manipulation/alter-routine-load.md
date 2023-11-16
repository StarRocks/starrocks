# ALTER ROUTINE LOAD

## 功能

该语法用于修改已经创建的例行导入作业，且只能修改处于 **PAUSED** 状态的作业。通过 [PAUSE](../data-manipulation/PAUSE_ROUTINE_LOAD.md) 命令可以暂停导入的任务，进行 `ALTER ROUTINE LOAD` 操作。

## 语法

```sql
ALTER ROUTINE LOAD FOR [db.]job_name
[load_properties]
[job_properties]
FROM data_source
[data_source_properties]
```

1. **[db.] job_name**

    指定要修改的作业名称。

2. **load_properties**

    用于描述导入数据。语法：

    ```sql
    [COLUMNS TERMINATED BY '<terminator>'],
    [COLUMNS ([<column_name> [, ...] ] [, column_assignment [, ...] ] )],
    [WHERE <expr>],
    [PARTITION ([ <partition_name> [, ...] ])]

    column_assignment:
    <column_name> = column_expression
    ```

    1. 设置列分隔符

        对于 csv 格式的数据，可以指定列分隔符，例如，将列分隔符指定为逗号 ","。

        ```sql
        COLUMNS TERMINATED BY ","
        ```

        默认为：\t。

    2. 指定列映射关系

        指定源数据中列的映射关系，以及定义衍生列的生成方式。

        1. 映射列：

            按顺序指定，源数据中各个列，对应目的表中的哪些列。对于希望跳过的列，可以指定一个不存在的列名。
            假设目的表有三列 k1, k2, v1。源数据有 4 列，其中第 1、2、4 列分别对应 k2, k1, v1。则书写如下：

            ```SQL
            COLUMNS (k2, k1, xxx, v1)
            ```

            其中 xxx 为不存在的一列，用于跳过源数据中的第三列。

        2. 衍生列：

            以 col_name = expr 的形式表示的列，我们称为衍生列。即支持通过 expr 计算得出目的表中对应列的值。
            衍生列通常排列在映射列之后，虽然这不是强制的规定，但是 StarRocks 总是先解析映射列，再解析衍生列。
            接上一个示例，假设目的表还有第 4 列 v2，v2 由 k1 和 k2 的和产生。则可以书写如下：

            ```plain text
            COLUMNS (k2, k1, xxx, v1, v2 = k1 + k2);
            ```

        对于 csv 格式的数据，COLUMNS 中的映射列的个数必须要与数据中的列个数一致。

    3. 指定过滤条件

        用于指定过滤条件，以过滤掉不需要的列。过滤列可以是映射列或衍生列。
        例如我们只希望导入 k1 大于 100 并且 k2 等于 1000 的列，则书写如下：

        ```plain text
        WHERE k1 > 100 and k2 = 1000
        ```

    4. 指定导入分区

        指定 **导入目的表** 的哪些 partition 中。如果不指定，则会自动导入到对应的 partition 中。
        示例：

        ```plain text
        PARTITION(p1, p2, p3)
        ```

3. **job_properties**

    指定需要修改的作业参数。目前仅支持以下参数的修改：

    1. `desired_concurrent_number`
    2. `max_error_number`
    3. `max_batch_interval`
    4. `max_batch_rows`
    5. `max_batch_size`
    6. `jsonpaths`
    7. `json_root`
    8. `strip_outer_array`
    9. `strict_mode`
    10. `timezone`

4. **data_source**

    数据源的类型。当前支持：

    KAFKA

5. **data_source_properties**

    数据源的相关属性。目前仅支持：
    1. `kafka_partitions`
    2. `kafka_offsets`
    3. 自定义 property，如 `property.group.id`

注意：

`kafka_partitions` 和 `kafka_offsets` 用于修改待消费的 kafka partition 的 offset，仅能修改当前已经消费的 partition。不能新增 partition。

## 示例

1. 将 `desired_concurrent_number` 修改为 1，调整该参数可调整消费 kafka 并行度，详细调节方式请参考: [通过调整并行度增加 Routine load 消费 kafka 数据速率](https://forum.starrocks.com/t/topic/1675)。

    ```sql
    ALTER ROUTINE LOAD FOR db1.label1
    PROPERTIES
    (
        "desired_concurrent_number" = "1"
    );
    ```

2. 将 `desired_concurrent_number` 修改为 10，修改 partition 的 offset，修改 group id。

    ```sql
    ALTER ROUTINE LOAD FOR db1.label1
    PROPERTIES
    (
        "desired_concurrent_number" = "10"
    )
    FROM kafka
    (
        "kafka_partitions" = "0, 1, 2",
        "kafka_offsets" = "100, 200, 100",
        "property.group.id" = "new_group"
    );
    ```

3. 更改导入的过滤条件为 `a > 0`，并且指定导入的 partition 为 `p1`。

    ```sql
    ALTER ROUTINE LOAD FOR db1.label1
    WHERE a > 0
    PARTITION (p1)
    ```
