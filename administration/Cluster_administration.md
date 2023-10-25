# 集群管理

## 集群启停

StarRocks集群部署完成之后，紧接着就是启动集群，提供服务。启动服务之前需要保证配置文件满足要求。

### FE启动

FE启动前需要重点关注的配置项有：元数据放置目录(meta_dir)、通信端口。

***meta_dir***：描述FE存储元数据的目录，需要提前创建好相应目录，并在配置文件中写明。由于FE的元数据是整个系统的元数据，十分关键，建议不要和其他进程混布。

FE配置的通信端口有四个:

|端口名称|默认端口|作用|
|---|---|---|
|http_port|8030|FE 上的 http server 端口|
|rpc_port|9020|FE 上的 thrift server 端口|
|query_port|9030|FE 上的 mysql server 端口|
|edit_log_port|9010|FE Group(Master, Follower, Observer)之间通信用的端口|

FE进程的启动方式十分简单：

* 进入FE进程的部署目录
  
* 运行`sh bin/start_fe.sh --daemon`启动服务

 FE为了保证高可用，会部署多个节点。线上典型的部署方式是3个FE(1 Master + 2 Follower)。

多节点启动的时候，建议先逐台启动Follower，然后启动Master（如果Follower出错可以提前发现）。

任何一台FE的启动，都建议进行验证，可以通过发送查询的方式予以验证。

### BE启动

BE启动前需要重点关注的配置项有：数据放置目录(storage_root_path)、通信端口。

***storage_root_path***描述BE放置存储文件的地方，需要事先创建好相应目录，建议每个磁盘创建一个目录。

BE配置的通信端口有三个:

|端口名称|默认端口|作用|
|---|---|---|
|be_port|9060|BE 上 thrift server 的端口，用于接收来自 FE 的请求|
|webserver_port|8040|BE 上的 http server 的端口|
|heartbeat_service_port|9050|BE 上心跳服务端口（thrift），用于接收来自 FE 的心跳|

### 确认集群健康状态

BE和FE启动完成之后，需要检查进程状态，以确定服务正常启动。

* 运行 `http://be_host:be_http_port/api/health`  确认BE启动状态

```shell
http://<be_host>:<be_http_port>/api/health
```

* 运行 `http://fe_host:fe_http_port/api/bootstrap` 确认FE启动状态。

  * 返回 `{"status":"OK","msg":"Success"}` 表示启动正常。

```shell
http://<fe_host>:<fe_http_port>/api/bootstrap
```

* 进入FE目录 运行`sh bin/stop_fe.sh`

* 进入BE目录 运行`sh bin/stop_be.sh`

## 集群升级

StarRocks可以通过滚动升级的方式，平滑进行升级。**升级顺序是先升级BE，再升级FE**。StarRocks保证BE后向兼容FE。升级的过程可以分为：测试升级的正确性，滚动升级，观察服务。

### 常规升级

#### 升级准备

* 在完成数据正确性验证后，将 BE 和 FE 新版本的二进制文件分发到各自目录下。

* 小版本升级，BE 只需升级 starrocks_be；FE 只需升级 starrocks-fe.jar。

* 大版本升级，则可能需要升级其他文件（包括但不限于 bin/ lib/ 等）；如果不确定是否需要替换其他文件，全部替换即可。

#### 升级

* 确认新版本的文件替换完成。

* 逐台重启 BE 后，再逐台重启 FE。
  
* 确认前一个实例启动成功后，再重启下一个实例。

##### BE 升级

```shell
cd be_work_dir 
mv lib lib.bak 
cp -r /tmp/StarRocks-SE-x.x.x/be/lib  .   
sh bin/stop_be.sh
sh bin/start_be.sh --daemon
ps aux | grep starrocks_be
```

##### FE 升级

```shell
cd fe_work_dir 
mv lib lib.bak 
cp -r /tmp/StarRocks-SE-x.x.x/fe/lib  .   
sh bin/stop_fe.sh
sh bin/start_fe.sh --daemon
ps aux | grep StarRocksFe
```

特别注意：

BE、FE启动顺序不能颠倒。因为如果升级导致新旧 FE、BE 不兼容，从新 FE 发出的命令可能会导致旧的 BE 挂掉。但是因为已经部署了新的 BE 文件，BE 通过守护进程自动重启后，即已经是新的 BE 了。

##### StarRocks 2.0 升级到StarRocks 2.1 的注意事项

StarRocks如果从2.0升级到2.1版本如果是灰度升级的方式需要确保下面的一些配置项:

1. 确保所有be的配置项vector_chunk_size是4096(默认配置)
2. 确保 FE session 变量 batch_size 小于 4096 (默认配置为1024)

```sql
mysql> show variables like '%batch_size%';
+---------------+-------+
| Variable_name | Value |
+---------------+-------+
| batch_size    | 1024  |
+---------------+-------+
1 row in set (0.00 sec)
-- 设置 batch_size
mysql> set global batch_size = 4096;
```

#### 测试BE升级的正确性

* 任意选择一个BE节点，部署最新的starrocks_be二进制文件。

* 重启该BE节点，通过BE日志be.INFO查看是否启动成功。
  
* 如果启动失败，可以先排查原因。如果错误不可恢复，可以直接通过 `DROP BACKEND` 删除该 BE、清理数据后，使用上一个版本的 starrocks_be 重新启动 BE。然后重新 `ADD BACKEND`。（**该方法会导致丢失一个数据副本，请务必确保3副本完整的情况下，执行这个操作！！！**）
  
#### 测试FE升级的正确性

* 单独使用新版本部署一个测试用的 FE 进程（比如自己本地的开发机）

* 修改测试用的 FE 的配置文件 fe.conf，将**所有端口设置为与线上不同**。
  
* 在fe.conf添加配置：cluster_id=123456
  
* 在fe.conf添加配置：metadata_failure_recovery=true
  
* 拷贝线上环境Master FE的元数据目录meta到测试环境
  
* 将拷贝到测试环境中的 meta/image/VERSION 文件中的 cluster_id 修改为 123456（即与第3步中相同）
* 在测试环境中，运行 `sh bin/start_fe.sh` 启动 FE
  
* 通过FE日志fe.log观察是否启动成功。
  
* 如果启动成功，运行 `sh bin/stop_fe.sh` 停止测试环境的 FE 进程。

**以上 2-6 步的目的是防止测试环境的FE启动后，错误连接到线上环境中。**

**因为FE元数据十分关键，升级出现异常可能导致所有数据丢失，请特别谨慎，尤其是大版本的升级。**

### 标准版DorisDB-x升级到社区版StarRocks-x

>注意：StarRocks可以做到前向兼容，所以在升级的时候可以做到灰度升级，但是一定是先升级BE再升级FE。(标准版安装包命名为DorisDB*,社区版安装包命名格式为StarRocks*)

#### 准备升级环境

>注意：请留意环境变量配置准确

1. 创建操作目录，准备环境
  
    ```bash
    # 创建升级操作目录用来保存备份数据，并进入当前目录
    mkdir DorisDB-upgrade-$(date +"%Y-%m-%d") && cd DorisDB-upgrade-$(date +"%Y-%m-%d")
    # 设置当前目录路径作为环境变量，方便后续编写命令
    export DSDB_UPGRADE_HOME=`pwd`
    # 将您当前已经安装的标准版（DorisDB）路径设置为环境变量
    export DSDB_HOME=标准版的安装路径，形如“xxx/DorisDB-SE-1.17.6”
    ```

2. 下载社区版（StarRocks）安装包，并解压，解压文件根据您下载版本进行重命名
  
    ```bash
    wget "https://xxx.StarRocks-SE-1.18.4.tar.gz" -O StarRocks-1.18.4.tar.gz
    tar -zxvf StarRocks-1.18.4.tar.gz
    # 将社区版的解压文件路径设置为环境变量
    export STARROCKS_HOME= 社区版的解压安装路径，形如“xxx/StarRocks-1.18.4”
    ```

#### 升级BE

逐台升级BE，确保每台升级成功，保证对数据没有影响

1. 先将标准版的BE停止

    ```bash
    # 停止BE
    cd ${DSDB_HOME}/be && ./bin/stop_be.sh
    ```

2. 当前标准版BE的lib和bin移动到备份目录下

    ```bash
    # 在DSDB_UPGRADE_HOME 目录下创建be目录，用于备份当前标准版be的lib
    cd ${DSDB_UPGRADE_HOME} && mkdir -p DorisDB-backups/be
    mv  ${DSDB_HOME}/be/lib DorisDB-backups/be/
    mv  ${DSDB_HOME}/be/bin DorisDB-backups/be/
    ```

3. 拷贝社区版（StarRocks）的BE文件到当前标准版(DorisDB)的BE的目录下

    ```bash
    cp -rf ${STARROCKS_HOME}/be/lib/ ${DSDB_HOME}/be/lib
    cp -rf ${STARROCKS_HOME}/be/bin/ ${DSDB_HOME}/be/bin
    ```

4. 重新启动BE

    ```bash
    # 启动BE
    cd ${DSDB_HOME}/be && ./bin/start_be.sh --daemon
    ```

5. 验证BE是否正确运行  

    a. 在MySQL客户端执行show backends/show proc '/backends' \G 查看BE是否成功启动并且版本是否正确  

    b. 观察be/log/be.INFO的日志是否正常  

    c. 观察be/log/be.WARNING 是否有异常日志  

6. 第一台观察10分钟后，按照上述流程执行其他BE节点

#### 升级FE

> 注意：
>
> * 逐台升级FE，先升级Observer，在升级Follower，最后升级Master
>
> * 请将没有启动FE的节点的FE同步升级，以防后续这些节点启动FE出现FE版本不一致的情况

1. 先将当前节点FE停止

    ```bash
    cd ${DSDB_HOME}/fe && ./bin/stop_fe.sh
    ```

2. 创建FE目录备份标准版的FE的lib和bin

    ```bash
    cd ${DSDB_UPGRADE_HOME} && mkdir -p DorisDB-backups/fe
    mv ${DSDB_HOME}/fe/bin DorisDB-backups/fe/
    mv ${DSDB_HOME}/fe/lib DorisDB-backups/fe/
    ```

3. 拷贝社区版FE的lib和bin文件到标准版FE的目录下  

    ```bash
    cp -r ${STARROCKS_HOME}/fe/lib/ ${DSDB_HOME}/fe
    cp -r ${STARROCKS_HOME}/fe/bin/ ${DSDB_HOME}/fe
    ```

4. 备份meta_dir元数据信息  
  
    a. 将标准版的/fe/conf/fe.conf中设置的meta_dir目录进行备份，如未更改过配置文件中meta_dir属性，默认为`${DSDB_HOME}/doris-meta”`；下例中设置为`“${DSDB_HOME}/meta”`,  
  
    b. 注意保证“命令中元数据目录”和“元数据实际目录”还有“配置文件”中一致，如您原目录名为“doris-meta”，建议您将目录重命名，同步需要更改配置文件  
  
    ```bash
    mv doris-meta/ meta
    ```
  
    c. 配置文件示例：
  
    ```bash
    # store metadata, create it if it is not exist.
    # Default value is ${DORIS_HOME}/doris-meta
    # meta_dir = ${DORIS_HOME}/doris-meta
    meta_dir=xxxxx/DorisDB-SE-1.17.6/meta
    ```
  
    其中，meta_dir设置为创建的元数据存储目录路径

    ```bash
    cd “配置文件中设置的meta_dir目录,到meta的上级目录”
    # 拷贝meta文件，meta为本例中设置路径，请根据您配置文件中设置进行更改
    cp -r meta meta-$(date +"%Y-%m-%d")
    ```
  
5. 启动FE服务

    ```bash
    cd ${DSDB_HOME}/fe && ./bin/start_fe.sh --daemon
    ```

6. 验证FE是否正确运行  

    a. 在MySQL客户端执行show frontends /show proc '/frontends' \G查看FE是否成功启动并且版本是否正确  

    b. 观察fe/log/fe.INFO的日志是否正常  

    c. 观察fe/log/fe.WARNING 是否有异常日志  

7. 第一台观察10分钟后，按照上述流程执行其他FE节点

#### 升级brokers

1. 停止当前节点的broker

    ```bash
    cd ${DSDB_HOME}/apache_hdfs_broker && ./bin/stop_broker.sh
    ```

2. 备份当前标准版的Broker运行的lib和bin

    ```bash
    cd ${DSDB_UPGRADE_HOME} && mkdir -p DorisDB-backups/apache_hdfs_broker
    mv ${DSDB_HOME}/apache_hdfs_broker/lib DorisDB-backups/apache_hdfs_broker/
    mv ${DSDB_HOME}/apache_hdfs_broker/bin DorisDB-backups/apache_hdfs_broker/
    ```

3. 拷贝社区版本Broker的lib和bin文件到执行Broker的目录下

    ```bash
    cp -rf ${STARROCKS_HOME}/apache_hdfs_broker/lib ${DSDB_HOME}/apache_hdfs_broker
    cp -rf ${STARROCKS_HOME}/apache_hdfs_broker/bin ${DSDB_HOME}/apache_hdfs_broker
    ```

4. 启动当前Broker

    ```bash
    cd ${DSDB_HOME}/apache_hdfs_broker && ./bin/start_broker.sh --daemon
    ```

5. 验证Broker是否正确运行

    a. 在MySQL客户端执行show broker 查看Broker是否成功启动并且版本是否正确

    b. 观察broker/log/broker.INFO的日志是否正常  

    c. 观察broker/log/broker.WARNING 是否有异常日志  

6. 按照上述流程执行其他Broker节点

#### 回滚方案

>注意：从标准版DorisDB升级至社区版StarRocks，暂时不支持回滚，建议先在测试环境验证测试没问题后再升级线上。如有遇到问题无法解决，可以添加下面企业微信寻求帮助。

![二维码](../assets/8.3.1.png)

### 从 ApacheDoris 升级为 DorisDB 标准版操作手册

#### 升级环境

> 注意：当前只支持从apache doris的0.13.15（不包括）版本以前升级。0.13.15版本在升级fe时需要修改源码处理，具体方法见升级Fe的部分。0.14及以后的版本暂时不支持升级。

1. 获取原有集群信息

    如果原有集群 FE/BE/broker 信息未给出，可以通过 MySQL 连接到 FE 的方式，并使用以下 SQL 命令查看并确认清楚：

    ```SQL
    show frontends;
    show backends;
    show broker;
    ```

    重点关注：

    a. FE、BE 的 数量/IP/版本 等信息；
  
    b. FE 的 Leader、Follower、Observer 情况；  

2. 假设

    这里假设原Apache Doris目录为 `/home/doris/doris/`, 手工安装的 DorisDB 新目录为 `/home/doris/dorisdb/`，为了减少操作失误，后续步骤采用全路径方式。如有具体升级中个，存在路径差异，建议统一修改文档中对应路径，然后严格按照操作步骤执行。

    有些`/home/doris`是软链接，可能会使用`/disk1/doris`等，具体情况下得注意

3. 检查 BE 配置文件

    ```bash
    # 检查BE 配置文件的一些字段
    vim /home/doris/doris/be/conf/be.conf
    ```

    重点是，检查default_rowset_type=BETA配置项，确认是否是 BETA 类型：

    a. 如果为 BETA，说明已经开始使用 segmentV2 格式，但还有可能有部分tablet或 rowset还是 segmentV1 格式，也需要检查和转换。  

    b. 如果是 ALPHA，说明全部数据都是 segmentV1 格式，则需要修改配置为 BETA，并做后续检查和转换。  

4. 测试SQL
    可以测试下，看看当前数据的情况。

    ```SQL
    show databases;
    use {one_db};
    show tables;
    show data;
    select count(*) from {one_table};
    ```

#### 升级步骤

1. 检查文件格式

    a. 下载文件格式检测工具

    ```bash
    # git clone 或直接从其他地方下载包
    http://dorisdb-public.oss-cn-zhangjiakou.aliyuncs.com/show_segment_status.tar.gz
    ```

    b. 解压`show_segment_status.tar.gz`包

    ```bash
    tar -zxvf show_segment_status.tar.gz
    ```

    c. 修改`cong`文件

    ```bash
    vim conf

    进行修改文件

    [cluster]
    fe_host =10.0.2.170
    query_port =9030
    user = root
    query_pwd = ****

    # Following confs are optional
    # select one database
    db_names =  数据库名
    # select one table
    table_names =  表明
    # select one be. when value is 0 means all bes
    be_id = 0
    ```

    d. 设置完成后，运行检测脚本，检测是否已经转换为了segmentV2。

    ```bash
    python show_segment_status.py
    ```

    e. 检查工具输出信息：rowset_count的两个值是否相等，数目不相等时，就说明存在这种segmentv1的表，需要进行转换。

2. 寻找segmentv1的表，进行转换
    针对每一个有 segmentV1 格式数据的表，进行格式转换：

    ```SQL
    -- 修改格式
    ALTER TABLE table_name SET ("storage_format" = "v2");

    -- 等待任务完成。如果 status 为 FINISHED 即可
    SHOW ALTER TABLE column;
    ```

    并再次重复运行python`show_segment_status.py`语句来检查：

    如果已经显示成功设置 storage_format 为 V2 了，但还是有数据是 v1 格式的，则可以通过以下方式进一步检查：

    a. 逐个寻找所有表，通过`show tablet from table_name`获取表的元数据链接；

    b. 通过MetaUrl，类似`wget http://172.26.92.139:8640/api/meta/header/11010/691984191获取tablet`的元数据；

    c. 这时候本地会出现一个`691984191`的JSON文件，查看其中的`rowset_type`看看内容是不是`ALPHA_ROWSET/BETA_ROWSET`；

    d. 如果是ALPHA_ROWSET，就表明是segmentV1的数据，需要进行转换到segmentV2。

    如果直接修改 storage_format为 v2 的方法执行后，还是有数据为 v1 版本，则需要再使用如下方法处理（但一般不会有问题，这个方法也比较麻烦）：

    ```SQL
    -- 方法2:参考SQL 通过重新导入数据到临时分区，然后分区替换的方式来处理SegmentV2的转化
    alter table dwd_user_tradetype_d
    ADD TEMPORARY PARTITION p09
    VALUES [('2020-09-01'), ('2020-10-01'))
    ("replication_num" = "3")
    DISTRIBUTED BY HASH(`dt`, `c`, `city`, `trade_hour`) BUCKETS 16;

    insert into dwd_user_tradetype_d TEMPORARY partition(p09)
    select * from dwd_user_tradetype_d partition(p202009);

    ALTER TABLE dwd_user_tradetype_d
    REPLACE PARTITION (p202009) WITH TEMPORARY PARTITION (p09);
    ```

3. 升级BE

    > 注意：BE升级采用逐台升级的方式，确保机器升级无误后，隔点时间/隔一天再升级其他机器

    准备工作：解压缩DorisDB，并重命名为dorisdb；

    ```bash
    cd ~
    tar xzf DorisDB-EE-1.14.6/file/DorisDB-1.14.6.tar.gz
    mv DorisDB-1.14.6/ dorisdb
    ```

    * 比较并拷贝原有 conf/be.conf的内容到新的 BE 中的conf/be.conf中；

    ```bash
    # 比较并修改和拷贝
    vimdiff /home/doris/dorisdb/be/conf/be.conf /home/doris/doris/be/conf/be.conf

    # 重要关注下面（拷贝此行配置到新 BE 中，一般建议继续使用原数据目录）
    storage_root_path = {data_path}
    ```

    * 检查是否是用 supervisor 启动的 BE：
      * 如果是supervisor启动的，就需要通过supervisor发送命令重启BE
      * 如果没有部署supervisor, 则需要手动重启BE

    ```bash
    # check 原有进程（原来的是 palo_be)
    ps aux | grep palo_be
    # 检查是否有 supervisor(只需要看 doris 账户下的进程）
    ps aux | grep supervisor

    ## 下面 a/b 是二选一
    # a、如果没有supervisor，则直接关闭原有be 进程
    sh /home/doris/doris/be/bin/stop_be.sh
    # b、如果有 supervisor，用 control.sh脚本关闭 be
    cd /home/doris/doris/be && ./control.sh stop && cd -

    # !!！检查： mysql中确保 Alive 为 false，以及 LastStartTime 为最新时间（见下图）
    mysql> show backends;
    # !!!   并且进程不在了（palo_be)
    ps aux | grep be
    ps aux | grep supervisor

    # 启动新 BE
    sh /home/doris/dorisdb/be/bin/start_be.sh --daemon
    # 检查：以及进程存在（新进程名为 dorisdb_be)
    ps aux | grep dorisdb_be
    # 检查：mysql 中 alive 为 true（见下图）
    mysql> show backends;
    ```

    * 观察升级结果：
        * 观察 be.out，查看是否有异常日志。
        * 观察 be.INFO，查看heartbeat是否正常。
        * 观察 be.WARN, 查看有什么异常。
        * 登录集群，发送show backends, 查看是否Alive这一栏是否为true。
    * 升级 2 个 BE 后，show frontends 下，看ReplayedJournalId是否在增长，以说明导入是否没问题。

4. 升级FE

    > 注意：FE升级采用先升级Observer，再升级Follower，最后升级Master的逻辑。
    > 如果是从apache doris 0.13.15版本升级，先要修改starrocks的fe模块的源码，并重新编译fe模块。如果没有编译过fe模块，可以找官方技术支持提供帮助。

    * 修改fe源码(如果不是从apache doris 0.13.15版本升级，跳过此步骤)
        * 下载源码patch

        ```bash
        wget "http://starrocks-public.oss-cn-zhangjiakou.aliyuncs.com/upgrade_from_apache_0.13.15.patch"
        ```

        * git命令合入patch

        ```bash
        git apply --reject upgrade_from_apache_0.13.15.patch
        ```

        * 如果本地代码没有在git环境中，也可以根据patch的内容手动合入。

        * 编译fe模块

        ```bash
        ./build.sh --fe --clean
        ```

    * 登录集群，确定Master和Follower，如果IsMaster为true，代表是Master。其他的都是Follower/Observer。
    * 升级Follower或者Master之前确保备份元数据，这一步非常重要，因为要确保没有问题。
        * cp doris-meta doris-meta.20210313 用升级的日期做备份时间即可。
    * 比较并拷贝原有 conf/fe.conf的内容到新的 FE 中的conf/fe.conf中；

    ```bash
    # 比较并修改和拷贝 
    vimdiff /home/doris/dorisdb/fe/conf/fe.conf /home/doris/doris/fe/conf/fe.conf

    # 重要关注下面（修改此行配置到新 FE 中，在原 doris 目录，新的 meta 文件（后面会拷贝）)
    meta_dir = /home/doris/doris/fe/doris-meta
    # 维持原有java堆大小等信息
    JAVA_OPTS="-Xmx8192m
    ```

    ```bash
    # check 原有进程
    ps aux | grep fe
    # 检查是否有 supervisor(只需要看 doris 账户下的进程）
    ps aux | grep supervisor

    ## 下面 a/b 是二选一
    # a、如果没有supervisor，则直接关闭原有fe 进程
    sh /home/doris/doris/fe/bin/stop_fe.sh
    # b、如果有 supervisor，用 control.sh脚本关闭 fe
    cd /home/doris/doris/fe && ./control.sh stop && cd -

    # !!！并检查： mysql中确保 Alive 为 false
    mysql> show frontends;
    # !!!   并且进程不在了
    ps aux | grep fe
    ps aux | grep supervisor

    # !!! 如果更改 meta 目录，则需要先停止后，再复制 meta
    cp -r /home/doris/doris/fe/palo-meta /home/doris/doris/fe/doris-meta
    
    # 启动新 FE 
    sh /home/doris/dorisdb/fe/bin/start_fe.sh --daemon
    # 检查：进程是否已经存在
    ps aux | grep dorisdb/fe
    # 检查：用当前 FE 登录 mysql，并且其中alive 为 true
    #  ，ReplayedJournalId 在同步甚至增长，以及进程存在
    mysql> show frontends;
    ```

    * 观察升级结果：
        * 观察 fe.out/fe.log 查看是否有错误信息。
        * 如果fe.log始终是UNKNOWN状态， 没有变成Follower、Observer，说明有问题。
        * fe.out报各种Exception，也有问题。
    （注意要先升级 Observer ，再升级Follower ）

    * 如果是修改源码升级的，需要等元数据产生新image之后(meta/image目录下有image.xxx的新文件产生)，将fe的lib包替换回发布包。

#### FAQs

1. 大量表有segmentV1的格式，需要转换，改怎么操作？

    segmentV1转segmentV2需要费时间来完成，这个可能需要一定时间，建议用户平时就可以进行这个操作。
2. FE/BE升级有没有顺序？

    需要先升级 BE、再升级 FE，因为DorisDB的标准版中BE是兼容FE的。升级BE的过程中，需要进行灰度升级，先升级一台BE，过一天观察无误，再升级其他FE。
3. 升级出问题是否能进行回滚？

    暂时不支持回滚，建议先在测试环境验证测试没问题后再升级线上。如有遇到问题无法解决，可以官网联系寻求帮助。
