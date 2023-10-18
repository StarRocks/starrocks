# 部署后设置

本文描述了您在部署 StarRocks 之后需要执行的任务。

在将新的 StarRocks 集群投入生产之前，您必须管理初始帐户并设置必要的变量和属性以使集群正常运行。

## 管理初始帐户

创建 StarRocks 集群后，系统会自动生成集群的初始 `root` 用户。`root` 用户拥有 `root` 权限，即集群内所有权限的集合。我们建议您修改 `root` 用户密码并避免在生产中使用该用户，以避免误用。

1. 使用用户名 `root` 和空密码通过 MySQL 客户端连接到 StarRocks。

   ```Bash
   # 将 <fe_address> 替换为您连接的 FE 节点的 IP 地址（priority_networks）
   # 或 FQDN，将 <query_port> 替换为您在 fe.conf 中指定的 query_port（默认：9030）。
   mysql -h <fe_address> -P<query_port> -uroot
   ```

2. 执行以下 SQL 重置 `root` 用户密码：

   ```SQL
   -- 将 <password> 替换为您要为 root 用户设置的密码。
   SET PASSWORD = PASSWORD('<password>')
   ```

> **说明**
>
> - 重置密码后请务必妥善保管。如果您忘记了密码，请参阅 [重置丢失的 root 密码](../administration/User_privilege.md#修改-root-用户密码) 了解详细说明。
> - 完成部署后设置后，您可以创建新用户和角色来管理团队内的权限。有关详细说明，请参阅 [管理用户权限](../administration/User_privilege.md)。

## 设置必要的系统变量

为使您的 StarRocks 集群在生产环境中正常工作，您需要设置以下系统变量：

| **变量名**                          | **StarRocks 版本** | **推荐值**                                                   | **说明**                                                     |
| ----------------------------------- | ------------------ | ------------------------------------------------------------ | ------------------------------------------------------------ |
| is_report_success                   | v2.4 或更早        | false                                                        | 是否发送查询 Profile 以供分析。默认值为 `false`，即不发送。将此变量设置为 `true` 会影响 StarRocks 的并发性能。 |
| enable_profile                      | v2.5 或以后        | false                                                        | 是否发送查询 Profile 以供分析。默认值为 `false`，即不发送。将此变量设置为 `true` 会影响 StarRocks 的并发性能。 |
| enable_pipeline_engine              | v2.3 或以后        | true                                                         | 是否启用 Pipeline Engine。`true` 表示启用，`false` 表示禁用。默认值为 `true`. |
| parallel_fragment_exec_instance_num | v2.3 或以后        | 如果您启用了 Pipeline Engine，您可以将此变量设置为`1`。如果您未启用 Pipeline Engine，您可以将此变量设置为 CPU 核数的一半。 | 每个 BE 上用于扫描节点的实例数。默认值为 `1`。               |
| pipeline_dop                        | v2.3、v2.4 及 v2.5 | 0                                                            | Pipeline 实例的并行度，用于调整查询并发度。默认值：0，表示系统自动调整每个 Pipeline 实例的并行度。<br />自 v3.0 起，StarRocks 根据查询并行度自适应调整该参数。 |

- 全局设置 `is_report_success` 为 `false`：

  ```SQL
  SET GLOBAL is_report_success = false;
  ```

- 全局设置 `enable_profile` 为 `false`：

  ```SQL
  SET GLOBAL enable_profile = false;
  ```

- 全局设置 `enable_pipeline_engine` 为 `true`：

  ```SQL
  SET GLOBAL enable_pipeline_engine = true;
  ```

- 全局设置 `parallel_fragment_exec_instance_num` 为 `1`：

  ```SQL
  SET GLOBAL parallel_fragment_exec_instance_num = 1;
  ```

- 全局设置 `pipeline_dop` 为 `0`：

  ```SQL
  SET GLOBAL pipeline_dop = 0;
  ```

有关系统变量的更多信息，请参阅 [系统变量](../reference/System_variable.md)。

## 设置用户属性

如果您在集群中创建了新用户，则需要增加新用户的最大连接数（例如至 `1000`）：

```SQL
-- 将 <username> 替换为需要增加最大连接数的用户名。
SET PROPERTY FOR '<username>' 'max_user_connections' = '1000';
```

## 下一步

成功部署和设置 StarRocks 集群后，您可以开始着手设计最适合您的业务场景场景的表。有关表设计的详细说明，请参阅 [理解表设计](../table_design/StarRocks_table_design.md)。
