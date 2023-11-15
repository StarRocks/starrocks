# SHOW EXPORT

## 功能

查询符合指定条件的导出作业的执行情况。

## 语法

```SQL
SHOW EXPORT
[ FROM <db_name> ]
[
WHERE
    [ QUERYID = <query_id> ]
    [ STATE = { "PENDING" | "EXPORTING" | "FINISHED" | "CANCELLED" } ]
]
[ ORDER BY <field_name> [ ASC | DESC ] [, ... ] ]
[ LIMIT <count> ]
```

## 参数说明

该语句包含四个可选子句：

- FROM

  指定要查询的数据库的名称。如果不指定该子句，则默认查询当前数据库。

- WHERE

  指定过滤条件。只有符合过滤条件的导入作业记录才会作为查询结果返回。

  | **参数** | **是否必选** | **说明**                                                     |
  | -------- | ------------ | ------------------------------------------------------------ |
  | QUERYID  | 否           | 导出作业的 ID。该参数用于查询单个导出作业的执行情况。        |
  | STATE    | 否           | 导出作业的状态。取值范围：<br /><ul><li>`PENDING`：表示查询待调度的导出作业。</li><li>`EXPORING`：表示查询正在执行中的导出作业。</li><li>`FINISHED`：表示查询成功完成的导出作业。</li><li>`CANCELLED`：表示查询失败的导出作业。</li></ul> |

- ORDER BY

  指定用于对返回的导出作业记录进行排序的字段的名称。支持指定多个字段，多个字段之间必须用逗号 (,) 分隔。另外，您还可以在字段后通过添加 `ASC` 或 `DESC` 关键字来指定按该字段对返回的导出作业记录进行升序或降序排序。

- LIMIT

  指定允许返回的导出作业记录最大数量。取值范围：正整数。如果不指定该子句，则默认返回符合指定条件的所有导出作业。

## 返回结果说明

例如查看一个 ID 为 `edee47f0-abe1-11ec-b9d1-00163e1e238f` 的导出作业的执行情况：

```SQL
SHOW EXPORT
WHERE QUERYID = "edee47f0-abe1-11ec-b9d1-00163e1e238f";
```

返回结果如下：

```SQL
     JobId: 14008
   QueryId: edee47f0-abe1-11ec-b9d1-00163e1e238f
     State: FINISHED
  Progress: 100%
  TaskInfo: {"partitions":["*"],"column separator":"\t","columns":["*"],"tablet num":10,"broker":"","coord num":1,"db":"db0","tbl":"tbl_simple","row delimiter":"\n","mem limit":2147483648}
      Path: hdfs://127.0.0.1:9000/users/230320/
CreateTime: 2023-03-20 11:16:14
 StartTime: 2023-03-20 11:16:17
FinishTime: 2023-03-20 11:16:26
   Timeout: 7200
```

返回结果中的参数说明如下：

- `JobId`：导出作业的 ID。
- `QueryId`：当前查询的 ID。
- `State`：导出作业的状态，包括：
  - `PENDING`：导出作业待调度。
  - `EXPORTING`：导出作业正在执行中。
  - `FINISHED`：导出作业成功。
  - `CANCELLED`：导出作业失败。
- `Progress`：导出作业的进度。该进度以查询计划为单位。假设导出作业一共分为 10 个查询计划，当前已完成 3 个，则进度为 30%。有关查询计划的原理，参见[“使用 EXPORT 导出数据 > 导出流程”](../../../unloading/Export.md#导出流程)。
- `TaskInfo`：导出作业的信息，格式为 JSON，包括：
  - `partitions`：导出数据所在的分区。通配符 (`*`) 表示导出了所有分区的数据。
  - `column separator`：导出文件的列分隔符。
  - `columns`：导出的列的名称。
  - `tablet num`：导出的 Tablet 总数量。
  - `broker`：在 v2.4 及以前版本，该字段用于返回导出作业使用的 Broker 的名称。自 v2.5 起，该字段返回一个空字符串。具体原因请参见[“使用 EXPORT 导出数据 > 背景信息”](../../../unloading/Export.md#背景信息)。
  - `coord num`：导出作业拆分的查询计划的个数。
  - `db`：导出数据所在的数据库的名称。
  - `tbl`：导出数据所在的表的名称。
  - `row delimiter`：导出文件的行分隔符。
  - `mem limit`：导出作业的内存使用限制。单位是字节。
- `Path`：远端存储上的导出路径。
- `CreateTime`：导出作业的创建时间。
- `StartTime`：导出作业开始调度的时间。
- `FinishTime`：导出作业的结束时间。
- `Timeout`：导出作业的超时时间。单位是秒。该时间从 `CreateTime` 开始计算。
- `ErrorMsg`：导出作业的错误原因。该字段仅当导出作业出现错误时才会返回。

## 示例

- 查询当前数据库下所有导出作业：

  ```SQL
  SHOW EXPORT;
  ```

- 查询指定数据库 `example_db` 下 ID 为 `921d8f80-7c9d-11eb-9342-acde48001122` 的导出作业：

  ```SQL
  SHOW EXPORT FROM example_db
  WHERE queryid = "921d8f80-7c9d-11eb-9342-acde48001122";
  ```

- 查询指定数据库 `example_db` 下状态为 `EXPORTING` 的导出作业，并按 `StartTime` 对返回的导出作业记录进行升序排序：

  ```SQL
  SHOW EXPORT FROM example_db
  WHERE STATE = "exporting"
  ORDER BY StartTime ASC;
  ```

- 查询指定数据库 `example_db` 下所有导出作业，并按 `StartTime` 对返回的导出作业记录进行降序排序：

  ```SQL
  SHOW EXPORT FROM example_db
  ORDER BY StartTime DESC;
  ```
