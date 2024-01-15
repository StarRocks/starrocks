---
displayed_sidebar: "English"
---

# Load Balancing

When deploying multiple FE nodes, users can deploy a load balancing layer on top of the  FEs to achieve high availability.

The following are some high availability options:

## Code approach

One way is to implement code at the application layer to perform retry and load balancing . For example, if a connection is broken, it will automatically retry on other connections. This approach requires users to configure multiple FE node addresses.

## JDBC Connector

JDBC connector supports automatic retry:

~~~sql
jdbc:mysql:loadbalance://[host1][:port],[host2][:port][,[host3][:port]]...[/[database]][?propertyName1=propertyValue1[&propertyName2=propertyValue2]...]
~~~

## ProxySQL

ProxySQL is a MySQL proxy layer that supports read/write separation, query routing, SQL caching, dynamic load configuration, failover, and SQL filtering.

StarRocks FE is responsible for receiving connection and query requests, and it’s horizontally scalable and highly available. However FE requires users to set up a proxy layer on top of it to achieve automatic load balancing. See the following steps for setup:

### 1. Install relevant dependencies

~~~shell
yum install -y gnutls perl-DBD-MySQL perl-DBI perl-devel
~~~

### 2. Download the installation package

~~~shell
wget https://github.com/sysown/proxysql/releases/download/v2.0.14/proxysql-2.0.14-1-centos7.x86_64.rpm
~~~

### 3. Decompress to the current directory

~~~shell
rpm2cpio proxysql-2.0.14-1-centos7.x86_64.rpm | cpio -ivdm
~~~

### 4. Modify the configuration file

~~~shell
vim ./etc/proxysql.cnf 
~~~

Direct to a directory that the user has privilege to access (absolute path):

~~~vim
datadir="/var/lib/proxysql"
errorlog="/var/lib/proxysql/proxysql.log"
~~~

### 5. Start

~~~shell
./usr/bin/proxysql -c ./etc/proxysql.cnf --no-monitor
~~~

### 6. Log in

~~~shell
mysql -u admin -padmin -h 127.0.0.1 -P6032
~~~

### 7. Configure the global log

~~~shell
SET mysql-eventslog_filename='proxysql_queries.log';
SET mysql-eventslog_default_log=1;
SET mysql-eventslog_format=2;
LOAD MYSQL VARIABLES TO RUNTIME;
SAVE MYSQL VARIABLES TO DISK;
~~~

### 8. Insert into the leader node

~~~sql
insert into mysql_servers(hostgroup_id, hostname, port) values(1, '172.26.92.139', 8533);
~~~

### 9. Insert the observer nodes

~~~sql
insert into mysql_servers(hostgroup_id, hostname, port) values(2, '172.26.34.139', 9931);
insert into mysql_servers(hostgroup_id, hostname, port) values(2, '172.26.34.140', 9931);
~~~

### 10. Load the configuration

~~~sql
load mysql servers to runtime;
save mysql servers to disk;
~~~

### 11. Configure the username and password

~~~sql
insert into mysql_users(username, password, active, default_hostgroup, backend, frontend) values('root', '*94BDCEBE19083CE2A1F959FD02F964C7AF4CFC29', 1, 1, 1, 1);
~~~

### 12. Load the configuration

~~~sql
load mysql users to runtime; 
save mysql users to disk;
~~~

### 13. Write to the proxy rules

~~~sql
insert into mysql_query_rules(rule_id, active, match_digest, destination_hostgroup, mirror_hostgroup, apply) values(1, 1, '.', 1, 2, 1);
~~~

### 14. Load the configuration

~~~sql
load mysql query rules to runtime; 
save mysql query rules to disk;
~~~
