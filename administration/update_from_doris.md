# 从 ApacheDoris 升级为 Starrocks 标准版操作手册

## 升级环境

> 注意：当前只支持从 apache doris 的 0.13.15（不包括）之前的版本升级。0.13.15 版本在升级 fe 时需要修改源码处理，该版本可以联系官方人员协助升级。0.14 及以后的版本暂时不支持升级。

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

    这里假设原 Apache Doris 目录为 `/home/doris/doris/`, 手工安装的 Starrocks 新目录为 `/home/starrocks/starrocks/`，为了减少操作失误，后续步骤采用全路径方式。如有具体升级中个，存在路径差异，建议统一修改文档中对应路径，然后严格按照操作步骤执行。

    有些 `/home/doris` 是软链接，可能会使用 `/disk1/doris` 等，具体情况下得注意

3. 检查 BE 配置文件

    ```bash
    # 检查BE 配置文件的一些字段
    vim /home/doris/doris/be/conf/be.conf
    ```

    重点是，检查 default_rowset_type = BETA 配置项，确认是否是 BETA 类型：

    a. 如果为 BETA，说明已经开始使用 segmentV2 格式，但还有可能有部分 tablet 或 rowset 还是 segmentV1 格式，也需要检查和转换。  

    b. 如果是 ALPHA，说明全部数据都是 segmentV1 格式，则需要修改配置为 BETA，并做后续检查和转换。  

4. 测试 SQL
    可以测试下，看看当前数据的情况。

    ```SQL
    show databases;
    use {one_db};
    show tables;
    show data;
    select count(*) from {one_table};
    ```

## 升级准备

1. 检查文件格式

    a. 下载文件格式检测工具

    ```bash
    # git clone 或直接从其他地方下载包
    http://Starrocks-public.oss-cn-zhangjiakou.aliyuncs.com/show_segment_status.tar.gz
    ```

    b. 解压 `show_segment_status.tar.gz` 包

    ```bash
    tar -zxvf show_segment_status.tar.gz
    ```

    c. 修改 `conf` 文件

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
    table_names =  表名
    # select one be. when value is 0 means all bes
    be_id = 0
    ```

    d. 设置完成后，运行检测脚本，检测是否已经转换为了 segmentV2。

    ```bash
    python show_segment_status.py
    ```

    e. 检查工具输出信息：rowset_count 的两个值是否相等，数目不相等时，就说明存在这种 segmentv1 的表，需要进行转换。

2. 寻找 segmentv1 的表，进行转换
    针对每一个有 segmentV1 格式数据的表，进行格式转换：

    ```SQL
    -- 修改格式
    ALTER TABLE table_name SET ("storage_format" = "v2");

    -- 等待任务完成。 status 字段值为 FINISHED 即可
    SHOW ALTER TABLE column;
    ```

    并再次重复运行 python `show_segment_status.py` 语句来检查：

    如果已经显示成功设置 storage_format 为 V2 了，但还是有数据是 v1 格式的，则可以通过以下方式进一步检查：

    a. 逐个寻找所有表，通过 `show tablet from table_name` 获取表的元数据链接；

    b. 通过 MetaUrl，类似 `wget http://172.26.92.139:8640/api/meta/header/11010/691984191获取tablet` 的元数据；

    c. 这时候本地会出现一个 `691984191` 的 JSON 文件，查看其中的 `rowset_type` 看看内容是不是 `ALPHA_ROWSET/BETA_ROWSET`；

    d. 如果是 ALPHA_ROWSET，就表明是 segmentV1 的数据，需要进行转换到 segmentV2。

    如果直接修改 storage_format 为 v2 的方法执行后，还是有数据为 v1 版本，则需要再使用如下方法处理（但一般不会有问题，这个方法也比较麻烦）：

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

## 升级 BE

> 注意：BE 升级采用逐台升级的方式，确保机器升级无误后，隔点时间/隔一天再升级其他机器

准备工作：解压缩 Starrocks，并重命名为 Starrocks；

```bash
cd ~
tar xzf Starrocks-EE-1.19.6/file/Starrocks-1.19.6.tar.gz
mv Starrocks-1.19.6/ Starrocks
```

* 比较并拷贝原有 conf/be.conf 的内容到新的 BE 中的 conf/be.conf 中；

```bash
# 比较并修改和拷贝
vimdiff /home/doris/Starrocks/be/conf/be.conf /home/doris/doris/be/conf/be.conf

# 重要关注下面（拷贝此行配置到新 BE 中，一般建议继续使用原数据目录）
storage_root_path = {data_path}
```

* 检查是否是用 supervisor 启动的 BE：
    *如果是 supervisor 启动的，就需要通过 supervisor 发送命令重启 BE
    *如果没有部署 supervisor, 则需要手动重启 BE

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
sh /home/doris/Starrocks/be/bin/start_be.sh --daemon
# 检查：以及进程存在（新进程名为 Starrocks_be)
ps aux | grep starrocks_be
# 检查：mysql 中 alive 为 true（见下图）
mysql> show backends;
```

* 观察升级结果：
    *观察 be.out，查看是否有异常日志。
    *观察 be.INFO，查看 heartbeat 是否正常。
    *观察 be.WARN, 查看有什么异常。
    *登录集群，发送 show backends, 查看是否 Alive 这一栏是否为 true。
* 升级 2 个 BE 后，show frontends 下，看 ReplayedJournalId 是否在增长，以说明导入是否没问题。

## 升级 FE

> 注意：FE 升级采用先升级 Observer，再升级 Follower，最后升级 Master 的逻辑。
> 如果是从 apache doris 0.13.15 版本升级，先要修改 starrocks 的 fe 模块的源码，并重新编译 fe 模块。如果没有编译过 fe 模块，可以找官方技术支持提供帮助。

* 修改 fe 源码(如果不是从 apache doris 0.13.15 版本升级，跳过此步骤)
    *下载源码 patch

    ```bash
    wget "http://starrocks-public.oss-cn-zhangjiakou.aliyuncs.com/upgrade_from_apache_0.13.15.patch"
    ```

    *git 命令合入 patch

    ```bash
    git apply --reject upgrade_from_apache_0.13.15.patch
    ```

    *如果本地代码没有在 git 环境中，也可以根据 patch 的内容手动合入。

    *编译 fe 模块

    ```bash
    ./build.sh --fe --clean
    ```

* 登录集群，确定 Master 和 Follower，如果 IsMaster 为 true，代表是 Master。其他的都是 Follower/Observer。
* 升级 Follower 或者 Master 之前确保备份元数据，这一步非常重要，因为要确保没有问题。
    *cp doris-meta doris-meta.20210313 用升级的日期做备份时间即可。
* 比较并拷贝原有 conf/fe.conf 的内容到新的 FE 中的 conf/fe.conf 中；

```bash
# 比较并修改和拷贝 
vimdiff /home/doris/Starrocks/fe/conf/fe.conf /home/doris/doris/fe/conf/fe.conf

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
sh /home/doris/Starrocks/fe/bin/start_fe.sh --daemon
# 检查：进程是否已经存在
ps aux | grep StarRocksFE
# 检查：用当前 FE 登录 mysql，并且其中alive 为 true
#  ，ReplayedJournalId 在同步甚至增长，以及进程存在
mysql> show frontends;
```

* 观察升级结果：
    *观察 fe.out/fe.log 查看是否有错误信息。
    *如果 fe.log 始终是 UNKNOWN 状态， 没有变成 Follower、Observer，说明有问题。
    *fe.out 报各种 Exception，也有问题。
（注意要先升级 Observer ，再升级 Follower ）

* 如果是修改源码升级的，需要等元数据产生新 image 之后(meta/image 目录下有 image.xxx 的新文件产生)，将 fe 的 lib 包替换回发布包。

## 回滚方案

> 注意：从 ApacheDoris 升级为 Starrocks，暂时不支持回滚，建议先在测试环境验证测试没问题后再升级线上。如有遇到问题无法解决，可以添加下面企业微信寻求帮助。

![二维码](../assets/8.3.1.png)

## 注意事项

1. segmentV1 转 segmentV2 需要费时间来完成，这个可能需要一定时间，如果存在大量表数据格式为 V1 的话，建议用户平时就可以进行这个操作转换数据格式。

2. 需要先升级 BE、再升级 FE，因为 Starrocks 的标准版中 BE 是兼容 FE 的。升级 BE 的过程中，需要进行灰度升级，先升级一台 BE，过一天观察无误，再升级其他 FE。

3. 跨版本升级时需要谨慎，测试环境验证后再操作，且升级 Starrocks-2.x 版本时需要前置开启 CBO，需要升级至 2.x 版本的用户需先升级 1.19.x 版本。可在官网获取最新版本的 1.19.x 安装包（1.19.7）。
