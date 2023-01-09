# ALTER LOAD

## 功能

修改 Broker Load 作业的优先级。该命令目前可以用于修改处于 **QUEUEING** 状态或者 **LOADING** 状态的 Broker Load 作业的优先级。

> **说明**
>
> 修改处于 **LOADING** 状态的 Broker Load 作业的优先级不会对作业产生任何影响。

## 语法

```SQL
ALTER LOAD FOR <label_name>
properties
(
    'priority'='{LOWEST | LOW | NORMAL | HIGH | HIGHEST}'
)
```

## 参数说明

| **参数**   | **是否必选** | **说明**                                                     |
| ---------- | ------------ | ------------------------------------------------------------ |
| label_name | 是           | 指定导入作业的标签。格式：`[<database_name>.]<label_name>`。参见 [BROKER LOAD](../data-manipulation/BROKER%20LOAD.md#label)。 |
| priority   | 是           | 指定导入作业的优先级。取值范围：`LOWEST`、`LOW`、`NORMAL`、`HIGH` 和 `HIGHEST`。参见 [BROKER LOAD](../data-manipulation/BROKER%20LOAD.md#opt_properties)。 |

## 示例

假设您有一个标签为 `test_db.label1` 的 Broker Load 作业，且作业当前处于 **QUEUEING** 状态或者 **LOADING** 状态。如果您想尽快执行该作业，可以通过如下命令，把该作业的优先级改为 `HIGHEST`：

```SQL
ALTER LOAD FOR test_db.label1
properties
(
    'priority'='HIGHEST'
);
```
