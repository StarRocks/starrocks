# 负载均衡

当部署多个 FE 节点时，用户可以在多个 FE 之上部署负载均衡层来实现 StarRocks 的高可用。

以下提供一些高可用的方案：

## 代码方式

自己在应用层代码进行重试和负载均衡。比如发现一个连接挂掉，就自动在其他连接上进行重试。应用层代码重试需要应用自己配置多个StarRocks前端节点地址。

## JDBC Connector

如果使用 mysql jdbc connector 来连接StarRocks，可以使用 jdbc 的自动重试机制:

~~~sql
jdbc:mysql://[host:port],[host:port].../[database][?propertyName1][=propertyValue1][&propertyName2][=propertyValue2]...
~~~

## ProxySQL

ProxySQL 是一个灵活强大的 MySQL 代理层, 可以实现读写分离，支持 Query 路由、SQL Cache，动态加载配置、故障切换和 SQL 过滤等功能。

StarRocks 的 FE 进程负责接收用户连接和查询请求，其本身是可以横向扩展且高可用的，但是需要用户在多个 FE 上架设一层 proxy，来实现自动的连接负载均衡。

### 1. 安装相关依赖

~~~shell
yum install -y gnutls perl-DBD-MySQL perl-DBI perl-devel
~~~

### 2. 下载安装包

~~~shell
wget https://github.com/sysown/proxysql/releases/download/v2.0.14/proxysql-2.0.14-1-centos7.x86\_64.rpm
~~~

### 3. 解压到当前目录

~~~shell
rpm2cpio proxysql-2.0.14-1-centos7.x86_64.rpm | cpio -ivdm
~~~

### 4. 修改配置文件

~~~shell
vim ./etc/proxysql.cnf 
~~~

修改为用户有权限访问的目录（绝对路径）

~~~vim
datadir="/var/lib/proxysql"
errorlog="/var/lib/proxysql/proxysql.log"
~~~

### 5. 启动

~~~shell
./usr/bin/proxysql -c ./etc/proxysql.cnf --no-monitor
~~~

### 6. 登录

~~~shell
mysql -u admin -padmin -h 127.0.0.1 -P6032
~~~

### 7. 配置全局日志

~~~shell
SET mysql-eventslog_filename='proxysql_queries.log';
SET mysql-eventslog_default_log=1;
SET mysql-eventslog_format=2;
LOAD MYSQL VARIABLES TO RUNTIME;
SAVE MYSQL VARIABLES TO DISK;
~~~

### 8. 插入主节点

~~~sql
insert into mysql_servers(hostgroup_id, hostname, port) values(1, '172.26.92.139', 8533);
~~~

### 9. 插入Observer节点

~~~sql
insert into mysql_servers(hostgroup_id, hostname, port) values(2, '172.26.34.139', 9931);
insert into mysql_servers(hostgroup_id, hostname, port) values(2, '172.26.34.140', 9931);
~~~

### 10. load 配置

~~~sql
load mysql servers to runtime;
save mysql servers to disk;
~~~

### 11. 配置用户名和密码

~~~sql
insert into mysql_users(username, password, active, default_hostgroup, backend, frontend) values('root', '*94BDCEBE19083CE2A1F959FD02F964C7AF4CFC29', 1, 1, 1, 1);
~~~

### 12. load 配置

~~~sql
load mysql users to runtime; 
save mysql users to disk;
~~~

### 13. 写入代理规则

~~~sql
insert into mysql_query_rules(rule_id, active, match_digest, destination_hostgroup, mirror_hostgroup, apply) values(1, 1, '.', 1, 2, 1);
~~~

### 14. load 配置

~~~sql
load mysql query rules to runtime; 
save mysql query rules to disk;
~~~
