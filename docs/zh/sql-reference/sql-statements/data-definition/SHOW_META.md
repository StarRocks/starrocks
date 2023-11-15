# SHOW META

## 功能

查看统计信息元数据。该命令支持查看基础统计信息和直方图统计信息的元数据。该语句从 2.4 版本开始支持。

更多 CBO 统计信息采集的内容，参见 [CBO 统计信息](../../../using_starrocks/Cost_based_optimizer.md)。

### 基础统计信息元数据

#### 语法

```SQL
SHOW STATS META [WHERE predicate]
```

该语句返回如下列。

| **列名**   | **说明**                                            |
| ---------- | --------------------------------------------------- |
| Database   | 数据库名。                                          |
| Table      | 表名。                                              |
| Columns    | 列名。                                              |
| Type       | 统计信息的类型，`FULL` 表示全量，`SAMPLE` 表示抽样。 |
| UpdateTime | 当前表的最新统计信息更新时间。                      |
| Properties | 自定义参数信息。                                    |
| Healthy    | 统计信息健康度。                                    |

### 直方图统计信息元数据

#### 语法

```SQL
SHOW HISTOGRAM META [WHERE predicate];
```

该语句返回如下列。

| **列名**   | **说明**                                  |
| ---------- | ----------------------------------------- |
| Database   | 数据库名。                                |
| Table      | 表名。                                    |
| Column     | 列名。                                    |
| Type       | 统计信息的类型，直方图固定为 `HISTOGRAM`。 |
| UpdateTime | 当前表的最新统计信息更新时间。            |
| Properties | 自定义参数信息。                          |

## 相关文档

想了解更多 CBO 统计信息采集的内容，参见 [CBO 统计信息](../../../using_starrocks/Cost_based_optimizer.md)。
