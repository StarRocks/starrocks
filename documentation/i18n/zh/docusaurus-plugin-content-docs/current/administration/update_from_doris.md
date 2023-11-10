# 升级 Apache Doris 至 StarRocks

本文介绍如何将 Apache Doris 升级为 StarRocks。

当前仅支持从 Apache Doris 0.13.15（不包含）以前版本升级至 StarRocks。如果您需要升级 Apache Doris 0.13.15 至 StarRocks，请联系官方人员协助修改源码。当前暂不支持 Apache Doris 0.14 及以后版本升级至 StarRocks。

> 注意：
>
> * 因为 StarRocks 支持 BE 后向兼容 FE，所以请务必**先升级 BE 节点，再升级 FE 节点**。错误的升级顺序可能导致新旧 FE、BE 节点不兼容，进而导致 BE 节点停止服务。
> * 请谨慎跨版本升级。如需跨版本升级，建议您在测试环境验证无误后再升级生产环境。Apache Doris 升级至 Starrocks-2.x 版本时需要前置开启 CBO，因此您需要先将 Apache Doris 升级至 StarRocks 1.19.x 版本。您可以在[官网](https://www.mirrorship.cn/zh-CN/download)获取 1.19.x 版本的安装包。

## 检查升级环境

1. 通过 MySQL 客户端查看原有集群信息。

    ```SQL
    SHOW FRONTENDS;
    SHOW BACKENDS;
    SHOW BROKER;
    ```

    记录 FE、BE 的 数量、IP 地址、版本等信息，以及 FE 的 Leader、Follower、Observer 信息。

2. 确认安装相关路径。

    以下示例假设原 Apache Doris 目录为 `/home/doris/doris/`，Starrocks 新目录为 `/home/starrocks/starrocks/`。为减少误操作，后续步骤将采用全路径方式。请根据您的实际情况修改示例中的路径信息。

    > 注意：在某些情况下，Apache Doris 目录可能为 `/disk1/doris`。

3. 查看 BE 配置文件。

    检查 **be.conf** 文件中 `default_rowset_type` 配置项的值：

    * 如果该配置项的值为 `ALPHA`，则说明全部数据都使用 segmentV1 格式，需要修改配置项为 BETA，并做后续检查和转换。
    * 如果该配置项的值为 `BETA`，则说明数据已使用 segmentV2 格式，但可能仍有部分 Tablet 或 Rowset 使用 segmentV1 格式，需要做后续检查和转换。  

4. 测试 SQL 情况。

    ```SQL
    show databases;
    use {one_db};
    show tables;
    show data;
    select count(*) from {one_table};
    ```

## 转化数据格式

转换过程将花费较长时间。如果数据中存在大量 segmentV1 格式数据，建议您提前进行转化操作。

1. 检查文件格式。

    a. 下载并安装文件格式检测工具。

    ```bash
    # 您也可以通过 git clone 或直接从当前地址直接下载。
    wget http://Starrocks-public.oss-cn-zhangjiakou.aliyuncs.com/show_segment_status.tar.gz
    tar -zxvf show_segment_status.tar.gz
    ```

    b. 修改安装路径下的 **conf** 文件，根据实际情况配置以下配置项。

    ```conf
    [cluster]
    fe_host =10.0.2.170
    query_port =9030
    user = root
    query_pwd = ****

    # Following confs are optional
    # select one database
    db_names =              // 填写数据库名
    # select one table
    table_names =           // 填写表名
    # select one be. when value is 0 means all bes
    be_id = 0
    ```

    c. 设置完成后，运行检测脚本，检测数据格式。

    ```bash
    python show_segment_status.py
    ```

    d. 检查以上工具输出信息 `rowset_count` 的两个值是否相等。如果数目不相等，说明数据中存在 segmentv1 格式的表，需要进行对该部分数据转换。

2. 转换数据格式。

    a. 转换数据格式为 segmentV1 的表。

    ```SQL
    ALTER TABLE table_name SET ("storage_format" = "v2");
    ```

    b. 查看转化状态。当 `status` 字段值为 `FINISHED` 时，表明格式转换完成。

    ```sql
    SHOW ALTER TABLE column;
    ```

    c. 重复运行数据格式检查工具查看数据格式状态。如果已经显示成功设置 `storage_format` 为 `V2` 了，但仍有数据为 segmentV1 格式，您可以通过以下方式进一步检查并转换：

      i.   逐个查询所有表，获取表的元数据链接。

    ```sql
    SHOW TABLET FROM table_name;
    ```

      ii.  通过元数据链接获取 Tablet 元数据。

    示例：

    ```shell
    wget http://172.26.92.139:8640/api/meta/header/11010/691984191
    ```

      iii. 查看本地的元数据 JSON 文件，确认其中的 `rowset_type` 的值。如果为 `ALPHA_ROWSET`，则表明该数据为 segmentV1 格式，需要进行转换。

      iv.  如果仍有数据为 segmentV1 格式，您需要通过以下示例中的方法转换。

      示例：

    ```SQL
    ALTER TABLE dwd_user_tradetype_d
    ADD TEMPORARY PARTITION p09
    VALUES [('2020-09-01'), ('2020-10-01')]
    ("replication_num" = "3")
    DISTRIBUTED BY HASH(`dt`, `c`, `city`, `trade_hour`);

    INSERT INTO dwd_user_tradetype_d TEMPORARY partition(p09)
    select * from dwd_user_tradetype_d partition(p202009);

    ALTER TABLE dwd_user_tradetype_d
    REPLACE PARTITION (p202009) WITH TEMPORARY PARTITION (p09);
    ```

## 升级 BE 节点

升级遵循**先升级 BE，后升级 FE**的顺序。

> 注意：BE 升级采用逐台升级的方式。完成当前机器升级后，须间隔一定时间（推荐时间间隔为一天）确保当前机器升级无误后，再升级其他机器。

1. 下载并解压缩 StarRocks 安装包，并重命名安装路径为 **Starrocks**。

    ```bash
    cd ~
    tar xzf Starrocks-EE-1.19.6/file/Starrocks-1.19.6.tar.gz
    mv Starrocks-1.19.6/ Starrocks
    ```

2. 比较并拷贝原有 **conf/be.conf** 的内容到新的 BE **conf/be.conf** 中。

    ```bash
    # 比较并修改和拷贝
    vimdiff /home/doris/Starrocks/be/conf/be.conf /home/doris/doris/be/conf/be.conf

    # 拷贝以下配置到新 BE 配置文件中，建议您使用原有数据目录。
    storage_root_path =     // 原有数据目录
    ```

3. 检查系统是否使用 Supervisor 启动 BE，并关闭 BE。

    ```bash
    # 检查原有进程（palo_be)。
    ps aux | grep palo_be
    # 检查 doris 账户下是否有 Supervisor 进程。
    ps aux | grep supervisor
    ```

    * 如果已部署并使用 Supervisor 启动 BE，您需要通过 Supervisor 发送命令关闭 BE。

    ```bash
    cd /home/doris/doris/be && ./control.sh stop && cd -
    ```

    * 如果没有部署 Supervisor，您需要手动关闭 BE。

    ```bash
    sh /home/doris/doris/be/bin/stop_be.sh
    ```

4. 查看 BE 进程状态，确认节点 `Alive` 为 `false`，并且 `LastStartTime` 为最新时间。

    ```sql
    SHOW BACKENDS;
    ```

    确认 BE 进程已不存在。

    ```bash
    ps aux | grep be
    ps aux | grep supervisor
    ```

5. 启动新的 BE 节点。

    ```bash
    sh /home/doris/Starrocks/be/bin/start_be.sh --daemon
    ```

6. 检查升级结果。

    * 查看新 BE 进程状态。

    ```bash
    ps aux | grep be
    ps aux | grep supervisor
    ```

    * 通过 MySQL 客户端查看节点 `Alive` 状态。

    ```sql
    SHOW BACKENDS;
    ```

    * 查看 **be.out**，检查是否有异常日志。
    * 查看 **be.INFO**，检查 heartbeat 是否正常。
    * 查看 **be.WARN**， 检查是否有异常日志。

> 注意：成功升级 2 个 BE 节点后，您可以通过 `SHOW FRONTENDS;` 查看 `ReplayedJournalId` 是否在增长，以检测导入是否存在问题。

## 升级 FE 节点

成功升级 BE 节点后，您可以继续升级 FE 节点。

> 注意：升级 FE 遵循**先升级 Observer，再升级 Follower，最后升级 Leader** 的顺序。

1. 修改 FE 源码（可选，如果您无需从 Apache Doris 0.13.15 版本升级，可以跳过此步骤）。

    a. 下载源码 patch。

    ```bash
    wget "http://starrocks-public.oss-cn-zhangjiakou.aliyuncs.com/upgrade_from_apache_0.13.15.patch"
    ```

    b. 合入源码 patch。

    * 通过 Git 合入。

    ```bash
    git apply --reject upgrade_from_apache_0.13.15.patch
    ```

    * 如果本地代码不在 Git 环境中，您也可以根据 patch 的内容手动合入。

    c. 编译 FE 模块。

    ```bash
    ./build.sh --fe --clean
    ```

2. 通过 MySQL 客户端确定各 FE 节点的 Leader 和 Follower 身份。

    ```sql
    SHOW FRONTENDS;
    ```

    如果 `IsMaster` 为 `true`，代表该节点为 Leader，否则为 Follower 或 Observer。

3. 检查系统是否使用 Supervisor 启动的 FE，并关闭 FE。

    ```bash
    # 查看原有进程。
    ps aux | grep fe
    # 检查 Doris 账户下是否有 Supervisor 进程
    ps aux | grep supervisor
    ```

    * 如果已部署并使用 Supervisor 启动 FE，您需要通过 Supervisor 发送命令关闭 FE。

    ```bash
    cd /home/doris/doris/fe && ./control.sh stop && cd -
    ```

    * 如果没有部署 Supervisor，您需要手动关闭 BE。

    ```bash
    sh /home/doris/doris/fe/bin/stop_fe.sh
    ```

4. 查看 FE 进程状态，确认节点 `Alive` 为 `false`。

    ```sql
    SHOW FRONTENDS;
    ```

    确认 BE 进程已不存在。

    ```bash
    ps aux | grep fe
    ps aux | grep supervisor
    ```

5. 升级 Follower 或者 Leader 之前，务必确保备份元数据。您可以使用升级的日期做备份时间。

    ```bash
    cp -r /home/doris/doris/fe/palo-meta /home/doris/doris/fe/doris-meta
    ```

6. 比较并拷贝原有 **conf/fe.conf** 的内容到新 FE 的 **conf/fe.conf** 中。

    ```bash
    # 比较并修改和拷贝。
    vimdiff /home/doris/Starrocks/fe/conf/fe.conf /home/doris/doris/fe/conf/fe.conf

    # 修改此行配置到新 FE 中，在原 doris 目录，新的 meta 文件（后面会拷贝）。
    meta_dir = /home/doris/doris/fe/doris-meta
    # 维持原有java堆大小等信息。
    JAVA_OPTS="-Xmx8192m
    ```

7. 启动新的 FE 节点。

    ```bash
    sh /home/doris/Starrocks/fe/bin/start_fe.sh --daemon
    ```

8. 检查升级结果。

    * 查看新 FE 进程状态。

    ```bash
    ps aux | grep be
    ps aux | grep supervisor
    ```

    * 通过 MySQL 客户端查看节点 `Alive` 状态。

    ```sql
    SHOW BACKENDS;
    ```

    * 查看 **fe.out** 或 **fe.log**，检查是否有异常日志。
    * 如果 **fe.log** 始终是 UNKNOWN 状态，没有变成 Follower、Observer，说明升级出现问题。

> 注意：如果您通过修改源码升级，需要等元数据产生新 image 之后（既 **meta/image** 目录下有新的 **image.xxx** 文件生成），再将 **fe/lib** 路径替换回新的安装路径。

## 回滚方案

从 Apache Doris 升级的 StarRocks 暂时不支持回滚。建议您在测试环境验证测试成功后再升级至生产环境。

如果遇到无法解决的问题，您可以添加下方企业微信寻求帮助。

![二维码](../assets/8.3.1.png)
