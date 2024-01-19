---

displayed_sidebar: "Chinese"
---

# 使用 Apache Ranger 管理权限

[Apache Ranger](https://ranger.apache.org/) 提供了一个集中式的安全管理框架，用户可以通过可视化的 Web 页面来定制各种访问策略，决定哪些角色能访问哪些数据，对 Hadoop 生态的各个组件和服务进行细粒度的数据访问控制，确保数据的安全性和合规性。

Apache Ranger 提供以下核心模块：

- Ranger Admin：Ranger 的核心模块，内置了一个 Web 界面，用户可以通过界面或者 REST 接口来创建和更新安全策略。Hadoop 生态各个组件的 Plugin 定期对这些策略进行轮询和拉取。
- Agent Plugin：嵌入到 Hadoop 生态圈组件的 Plugin，定期从 Ranger Admin 拉取安全策略，存储在本地文件中。当用户访问组件时，Plugin 会根据安全策略对请求进行安全评估，将结果反馈给相应组件。
- User Sync：用于拉取用户和用户组的信息，将用户和用户组的权限数据同步到 Ranger 的数据库中。

除了原生的 RBAC 权限系统，StarRocks 3.1 及后续版本还支持通过 Apache Ranger 来进行访问控制，提供更高层次的数据安全保障。

本文介绍 StarRocks 与 Apache Ranger 集成后的权限控制方式以及集成过程。关于如何在 Ranger 上创建权限策略来管理数据安全，参见 [Apache Ranger 官网](https://ranger.apache.org/)。

## 权限控制方式

StarRocks 集成 Apache Ranger 后可以实现以下权限控制方式：

- 在 Ranger 中创建 StarRocks Service 实现权限控制。用户访问 StarRocks 内表、外表或其他对象时，会根据 StarRocks Service 中配置的访问策略来进行访问控制。
- 对于 External Catalog，可以复用 Ranger 上的外部 Service（如 Hive Service）实现访问控制。StarRocks 支持对于不同的 Catalog 匹配不同的 Ranger service。用户访问外部数据源时，会直接根据数据源对应的 Service 来进行访问控制。

通过集成 Apache Ranger，您可以实现以下访问控制模式：

- 全部使用 Ranger 进行权限管理，在 StarRocks Service 内统一管理内表、外表及所有对象。
- 全部使用 Ranger 进行权限管理。对于内表及内部对象，在 StarRocks Service 内管理；对于 External Catalog，无需额外创建，直接复用对应外部数据源对应的 Ranger Service。
- External Catalog 使用 Ranger 进行权限管理，复用外部数据源对应的 Ranger Service；内部对象及内部表在 StarRocks 内部进行授权。

目前 StarRocks 在能力上支持：

- 通过 Ranger 创建 Access policy、Masking policy、Row-level filter policy。
- 支持 Ranger 审计日志。
- 暂不支持 Kerberos 认证的 Ranger Server。

**权限管理流程：**

- 对于用户认证，您也可以选择通过 LDAP 来完成。Ranger 可以同步 LDAP 用户，并对其进行权限规则配置。StarRocks 也可以通过 LDAP 完成用户登录认证。
- 在用户发起查询时，StarRocks 会对查询语句进行解析，向 Ranger 传递用户信息及所需权限；Ranger 则会在对应 Service 中根据创建的访问策略来判断用户是否有访问权限，并向 StarRocks 返回鉴权结果。如果用户有访问权限，StarRocks 会返回查询数据；如果用户无访问权限，StarRocks 会返回报错。

## 前提条件

- 已经部署安装 Apache Ranger 2.0.0 及以上版本。详细的部署步骤，参见 [快速开始](https://ranger.apache.org/quick_start_guide.html)。

- 确保 StarRocks 所有 FE 机器都能够访问 Ranger。您可以在 FE 节点的机器上执行以下语句来判断:

  ```SQL
  telnet <ranger-ip> <ranger-host>
  ```

  如果显示 `Connected to <ip>`，则表示连接成功。

## 集成过程

### 安装 ranger-starrocks-plugin

1. 在 Ranger Admin 的 `ews/webapp/WEB-INF/classes/ranger-plugins` 目录下创建 `starrocks` 文件夹。

   ```SQL
   mkdir {path-to-ranger}/ews/webapp/WEB-INF/classes/ranger-plugins/starrocks
   ```

2. 下载 [`plugin-starrocks/target/ranger-starrocks-plugin-3.0.0-SNAPSHOT.jar`](https://www.starrocks.io/download/community) 和 [mysql-connector-j](https://dev.mysql.com/downloads/connector/j/)，并放入 `starrocks` 文件夹内。

3. 重启 Ranger Admin。

   ```SQL
   ranger-admin restart
   ```

### 在 Ranger Admin 上配置 StarRocks Service

1. 拷贝 [`ranger-servicedef-starrocks.json](https://github.com/StarRocks/ranger/blob/master/agents-common/src/main/resources/service-defs/ranger-servicedef-starrocks.json) 至 StarRocks FE 机器或 Ranger 集群机器上的任意目录。

   ```SQL
   wget https://raw.githubusercontent.com/StarRocks/ranger/master/agents-common/src/main/resources/service-defs/ranger-servicedef-starrocks.json
   ```

2. 使用 Ranger 的管理员账户运行以下命令，添加 StarRocks Service。

   ```SQL
   curl -u <ranger_adminuser>:<ranger_adminpwd> \
   -X POST -H "Accept: application/json" \
   -H "Content-Type: application/json" http://<ranger-ip>:<ranger-port>/service/plugins/definitions -d@ranger-servicedef-starrocks.json
   ```

3. 登录 Ranger 界面 `http://<ranger-ip>:<ranger-host>/login.jsp`。可以看到界面上出现了 STARROCKS 服务。

   ![home](../assets/ranger_home.png)

4. 点击 **STARROKCS** 后的加号 (`+`) 配置 StarRocks Service 信息。

   ![service config](../assets/ranger_service_details.png)

   ![property](../assets/ranger_properties.png)

   - `Service Name`: 服务名称，必填。
   - `Display Name`: 要显示在 STARROCKS 下的服务名称。如果不指定，则显示 `Service Name`。
   - `Username` 和 `Password`：FE 的账号和密码。用于后续创建 Policy 时对象名的自动补全，不影响 StarRocks 与 Ranger 的连通性。如果您希望使用自动补全功能，请至少配置一个默认激活 `db_admin` 角色的用户。
   - `jdbc.url`：填写 StarRocks 集群 FE 的 IP 及端口。

   下图展示了一个填写示例。

   ![example](../assets/ranger_show_config.png)

   下图展示了页面上配置好的 service。

   ![service](../assets/ranger_added_service.png)

5. 点击 **Test connection** 测试连通性，连通成功后保存。

6. 在 StarRocks 集群的每一台 FE 机器上，在 `fe/conf` 文件夹内创建 [`ranger-starrocks-security.xml`](https://github.com/StarRocks/ranger/blob/master/plugin-starrocks/conf/ranger-starrocks-security.xml)，并将内容拷贝，必须修改两处内容并保存：

   - `ranger.plugin.starrocks.service.name` 改为刚刚创建的 StarRocks Service 的名称。
   - `ranger.plugin.starrocks.policy.rest.url` 改为 Ranger Admin 的地址。

   如需修改其他配置也可根据 Ranger 官方文档进行对应修改。比如可以修改 `ranger.plugin.starrocks.policy.pollIntervalM` 来更改拉取权限变更的时间。

   ```SQL
   vim ranger-starrocks-security.xml
   
   ...
       <property>
           <name>ranger.plugin.starrocks.service.name</name>
           <value>starrocks</value> --改为 StarRocks Service 的名称。
           <description>
               Name of the Ranger service containing policies for this StarRocks instance
           </description>
       </property>
   ...

   ...
       <property>
           <name>ranger.plugin.starrocks.policy.rest.url</name>
           <value>http://localhost:6080</value> --改为 Ranger admin 的地址。
           <description>
               URL to Ranger Admin
           </description>
       </property>
   ...
   ```

7. 在 `fe/conf` 文件夹内拷贝 [`ranger-starrocks-audit.xml`](https://github.com/StarRocks/ranger/blob/master/plugin-starrocks/conf/ranger-starrocks-audit.xml) 和 [`ranger-hive-audit.xml`](https://github.com/StarRocks/ranger/blob/master/hive-agent/conf/ranger-hive-audit.xml) 文件。

8. 修改所有 FE 的配置文件，添加 `access_control=ranger`。

   ```SQL
   vim fe.conf
   access_control=ranger 
   ```

9. 重启所有 FE。

   ```SQL
   -- 回到 FE 文件夹内
   cd..
   
   bin/stop_fe.sh
   bin/start_fe.sh
   ```

## 复用其他 Service 来为外表鉴权

对于 External Catalog，可以复用外部 Service（如 Hive Service）实现访问控制。StarRocks  支持对于不同的 Catalog 匹配不同的 Ranger service。用户访问外表时，会直接根据对应外表的 Service 来进行访问控制。用户权限与 Ranger 同名用户一致。

1. 将 Hive 的 Ranger 相关配置文件 (`ranger-hive-security.xml` 和 `ranger-hive-audit.xml`) 拷贝至所有 FE 机器的 `fe/conf` 文件下。

3. 重启所有 FE。

4. 配置 Catalog。

   创建 External Catalog 时，添加 PROPERTIES `"ranger.plugin.hive.service.name"`.

    ```SQL
      CREATE EXTERNAL CATALOG hive_catalog_1
      PROPERTIES (
          "type" = "hive",
          "hive.metastore.type" = "hive",
          "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
          "ranger.plugin.hive.service.name" = "<ranger_hive_service_name>"
      )
    ```

   也可以对已有的 External Catalog 添加该属性。将已有的 Catalog 转换为通过 Ranger 鉴权。

    ```SQL
      ALTER CATALOG hive_catalog_1
      SET ("ranger.plugin.hive.service.name" = "<ranger_hive_service_name>");
    ```

## 后续步骤

添加完 StarRocks Service 后，您可以点击该服务，为服务创建权限策略，给不同的用户或用户组分配不同的权限。后续用户在访问 StarRocks 数据时，会根据这些策略进行访问控制。
