# ALTER SYSTEM

## 功能

该语句用于操作一个系统内的节点。（仅管理员使用！）

## 语法

### 增加节点(不使用多租户功能则按照此方法添加)

```sql
ALTER SYSTEM ADD BACKEND "host:heartbeat_service_port"[,"host:heartbeat_service_port"...];
```

BE 节点增加成功后可通过 [Show Backends](../Administration/SHOW%20BACKENDS.md) 章节描述命令查看。

### 增加空闲节点(即添加不属于任何 cluster 的 BACKEND)

```sql
ALTER SYSTEM ADD FREE BACKEND "host:heartbeat_service_port"[,"host:heartbeat_service_port"...];
```

### 增加节点到某个 cluster

```sql
ALTER SYSTEM ADD BACKEND TO cluster_name "host:heartbeat_service_port"[,"host:heartbeat_service_port"...];
```

### 删除节点

```sql
ALTER SYSTEM DROP BACKEND "host:heartbeat_service_port"[,"host:heartbeat_service_port"...];
```

### 节点下线

```sql
ALTER SYSTEM DECOMMISSION BACKEND "host:heartbeat_service_port"[,"host:heartbeat_service_port"...];
```

### 增加 Broker

```sql
ALTER SYSTEM ADD BROKER broker_name "host:port"[,"host:port"...];
```

Broker 增加成功后可通过 [Show Backends](../Administration/SHOW%20BROKER.md) 章节描述命令查看。

### 减少 Broker

```sql
ALTER SYSTEM DROP BROKER broker_name "host:port"[,"host:port"...];
```

### 删除所有 Broker

```sql
ALTER SYSTEM DROP ALL BROKER broker_name
```

### 设置一个 Load error hub，用于集中展示导入时的错误信息

```sql
ALTER SYSTEM SET LOAD ERRORS HUB PROPERTIES ("key" = "value"[, ...]);
```

说明：

1. host 可以是主机名或者 ip 地址。
2. heartbeat_service_port 为该节点的 **心跳端口**，默认 9050。
3. 增加和删除节点为 **同步** 操作。这两种操作不考虑节点上已有的数据，节点直接从元数据中删除，请谨慎使用。
4. 节点下线操作用于安全下线节点。该操作为 **异步** 操作。如果成功，节点最终会从元数据中删除。如果失败，则不会完成下线。
5. 可以手动取消节点下线操作。详见 [CANCEL DECOMMISSION](../Administration/CANCEL%20DECOMMISSION.md)。
6. Load error hub:

    当前支持两种类型的 Hub：Mysql 和 Broker。需在 PROPERTIES 中指定 "type" = "mysql" 或 "type" = "broker"。
    如果需要删除当前的 load error hub，可以将 type 设为 null。

    1. 当使用 Mysql 类型时，导入时产生的错误信息将会插入到指定的 mysql 库表中，之后可以通过 `show load warnings` 语句直接查看错误信息。

        Mysql 类型的 Hub 需指定以下参数：

        ```plain text
        host：mysql host
        port：mysql port
        user：mysql user
        password：mysql password
        database：mysql database
        table：mysql table
        ```

    2. 当使用 Broker 类型时，导入时产生的错误信息会形成一个文件，通过 broker，写入到指定的远端存储系统中。须确保已经部署对应的 broker。

        Broker 类型的 Hub 需指定以下参数：

        ```plain text
        broker: broker 的名称
        path: 远端存储路径
        other properties: 其他访问远端存储所必须的信息，比如认证信息等。
        ```

## 示例

1. 增加一个节点。

    ```sql
    ALTER SYSTEM ADD BACKEND "host:port";
    ```

2. 增加一个空闲节点。

    ```sql
    ALTER SYSTEM ADD FREE BACKEND "host:port";
    ```

3. 删除两个节点。

    ```sql
    ALTER SYSTEM DROP BACKEND "host1:port", "host2:port";
    ```

4. 下线两个节点。

    ```sql
    ALTER SYSTEM DECOMMISSION BACKEND "host1:port", "host2:port";
    ```

5. 增加两个 HDFS Broker。

    ```sql
    ALTER SYSTEM ADD BROKER hdfs "host1:port", "host2:port";
    ```

6. 添加一个 Mysql 类型的 load error hub。

    ```sql
    ALTER SYSTEM SET LOAD ERRORS HUB PROPERTIES
    ("type"= "mysql",
    "host" = "192.168.1.17"
    "port" = "3306",
    "user" = "my_name",
    "password" = "my_passwd",
    "database" = "starrocks_load",
    "table" = "load_errors"
    );
    ```

7. 添加一个 Broker 类型的 load error hub。

    ```sql
    ALTER SYSTEM SET LOAD ERRORS HUB PROPERTIES
    ("type"= "broker",
    "name" = "oss",
    "path" = "oss://backup-cmy/logs",
    "fs.oss.accessKeyId" = "xxx",
    "fs.oss.accessKeySecret" = "yyy",
    "fs.oss.endpoint" = "oss-cn-beijing.aliyuncs.com"
    );
    ```

8. 删除当前的 load error hub。

    ```sql
    ALTER SYSTEM SET LOAD ERRORS HUB PROPERTIES
    ("type"= "null");
    ```
