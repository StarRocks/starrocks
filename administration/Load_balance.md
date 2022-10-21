# 负载均衡

本文介绍如何通过在多个 FE 节点之上部署负载均衡层以实现 StarRocks 的高可用。

## 通过代码均衡负载

您可以在应用层代码进行重试和负载均衡。当特定连接宕机，代码应控制系统自动在其他连接上进行重试。使用该方式，您需要配置多个 StarRocks 前端节点地址。

## 通过 JDBC Connector 均衡负载

如果您使用 MySQL JDBC Connector 连接 StarRocks，可以通过 JDBC 的自动重试机制进行重试和负载均衡。

```sql
jdbc:mysql://[host:port],[host:port].../[database][?propertyName1][=propertyValue1][&propertyName2][=propertyValue2]...
```

## 通过 ProxySQL 均衡负载

ProxySQL 是一个灵活强大的 MySQL 代理层，可以实现读写分离，支持 Query 路由、SQL Cache，动态加载配置、故障切换和 SQL 过滤等功能。

StarRocks 的 FE 进程负责接收用户连接和查询请求，其本身是可以横向扩展且可以部署为高可用集群。您需要在多个 FE 节点上架设一层 Proxy 以实现自动的连接负载均衡。

1. 安装相关依赖。

    ```shell
    yum install -y gnutls perl-DBD-MySQL perl-DBI perl-devel
    ```

2. 下载并解压 ProxySQL 安装包。

    ```shell
    wget https://github.com/sysown/proxysql/releases/download/v2.0.14/proxysql-2.0.14-1-centos7.x86\_64.rpm
    rpm2cpio proxysql-2.0.14-1-centos7.x86_64.rpm | cpio -ivdm
    ```

    > 说明：您可以自行选择下载其他版本的 ProxySQL。

3. 修改配置文件 **/etc/proxysql.cnf**。

    修改以下配置项为您有访问权限的目录（绝对路径）。

    ```plain text
    datadir = "/var/lib/proxysql"
    errorlog = "/var/lib/proxysql/proxysql.log"
    ```

4. 启动 ProxySQL。

    ```shell
    ./usr/bin/proxysql -c ./etc/proxysql.cnf --no-monitor
    ```

5. 登录 StarRocks。

    ```shell
    mysql -u admin -padmin -h 127.0.0.1 -P6032
    ```

    > 注意
    >
    > ProxySQL 默认包含两个端口，其中 `6032` 是 ProxySQL 的管理端口，`6033` 是 ProxySQL 的流量转发端口，即对外提供服务的端口。

6. 配置全局日志。

    ```sql
    SET mysql-eventslog_filename='proxysql_queries.log';
    SET mysql-eventslog_default_log=1;
    SET mysql-eventslog_format=2;
    LOAD MYSQL VARIABLES TO RUNTIME;
    SAVE MYSQL VARIABLES TO DISK;
    ```

7. 插入主节点以及 Observer 节点并读取配置。

    ```sql
    insert into mysql_servers(hostgroup_id, hostname, port) values(1, '172.26.92.139', 9030);
    insert into mysql_servers(hostgroup_id, hostname, port) values(1, '172.26.34.139', 9030);
    insert into mysql_servers(hostgroup_id, hostname, port) values(1, '172.26.34.140', 9030);
    load mysql servers to runtime;
    save mysql servers to disk;
    ```

8. 配置用户名和密码并读取配置。

    ```sql
    insert into mysql_users(username, password, active, default_hostgroup, backend, frontend) 
    values('root', '*FAAFFE644E901CFAFAEC7562415E5FAEC243B8B2', 1, 1, 1, 1);
    load mysql users to runtime; 
    save mysql users to disk;
    ```

    > 注意：这里 `password` 输入值为密文。例如，root 用户密码为 `root123`，则 `password` 输入为 `*FAAFFE644E901CFAFAEC7562415E5FAEC243B8B2`。您可以通过 `password()`函数获取具体输入的加密值。
    >
    > 示例：
    >
    > ```plain text
    > mysql> select password('root123');
    > +---------------------------------------------+
    > | '*FAAFFE644E901CFAFAEC7562415E5FAEC243B8B2' |
    > +---------------------------------------------+
    > | *FAAFFE644E901CFAFAEC7562415E5FAEC243B8B2   |
    > +---------------------------------------------+
    > 1 row in set (0.01 sec)
    > ```

9. 写入代理规则并读取配置。

    ```sql
    insert into mysql_query_rules(rule_id, active, match_digest, destination_hostgroup, mirror_hostgroup, apply) values(1, 1, '.', 1, 2, 1);
    load mysql query rules to runtime; 
    save mysql query rules to disk;
    ```

完成以上步骤后，您可以通过 ProxySQL 经由 `6033` 端口对数据库进行操作。

```shell
mysql -u admin -padmin -P6033 -h 127.0.0.1 -e"select * from db_name.table_name"
```

如果结果正常返回，则表示您已成功通过 ProxySQL 连接 StarRocks。
