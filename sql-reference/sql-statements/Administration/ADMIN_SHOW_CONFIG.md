# ADMIN SHOW CONFIG

## 功能

该语句用于展示当前集群的配置（当前仅支持展示 FE 的配置项）。

关于每个配置项的含义，参见[FE 配置项](../../../administration/Configuration.md#fe-配置项)。

如果要动态设置或修改集群的配置项，参见 [ADMIN SET CONFIG](ADMIN_SET_CONFIG.md)。

## 语法

```sql
ADMIN SHOW FRONTEND CONFIG [LIKE "pattern"]
```

说明：

结果中的各列含义如下：

```plain text
1. Key         配置项名称
2. AliasNames  配置项别名
2. Value       配置项取值
3. Type        配置项数据类型
4. IsMutable   是否可以通过 ADMIN SET CONFIG 命令动态设置
5. Comment     配置项说明
```

## 示例

1. 查看当前 FE 节点的配置。

    ```sql
    ADMIN SHOW FRONTEND CONFIG;
    ```

2. 使用 like 谓词搜索当前 FE 节点的配置。

    ```plain text
    mysql> ADMIN SHOW FRONTEND CONFIG LIKE '%check_java_version%';
    +--------------------+------------+-------+---------+-----------+---------+
    | Key                | AliasNames | Value | Type    | IsMutable | Comment |
    +--------------------+------------+-------+---------+-----------+---------+
    | check_java_version | []         | true  | boolean | false     |         |
    +--------------------+------------+-------+---------+-----------+---------+
    1 row in set (0.00 sec)

    ```
