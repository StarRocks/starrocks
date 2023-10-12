# SHOW RESOURCES

## 功能

用于展示用户有使用权限 (USAGE) 的资源。

> **注意**
>
> 普通用户可以看到自己有 USAGE 权限的资源，`db_admin` 角色可以看到全局资源。

## 语法

```sql
SHOW RESOURCES
```

返回字段

| 字段     | 说明                                              |
| -------- | ----------------------------------|
| Name    | 资源名称。                                          |
| ResourceType    | 资源类型，比如 spark。                       |
| Key     |  创建资源时在 PROPERTIES 里指定的 key。               |
| Value   |    key 对应的 value。                               |

## 示例

```plain
MySQL [(none)]> show resources;
+-------------------+--------------+---------------------+-----------------------+
| Name              | ResourceType | Key                 | Value                 |
+-------------------+--------------+---------------------+-----------------------+
| spark_resource_16ce91e4_e20a_11ed_a756_00163e0e489a | spark | spark.master | yarn |
```

## Keywords

show resource, show resources
