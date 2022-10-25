# 升级标准版 DorisDB 至社区版 StarRocks

本文介绍如何将标准版 DorisDB 升级至社区版 StarRocks。标准版 DorisDB 包括安装包命名格式为 DorisDB-x 的版本，社区版 StarRocks 包含安装包命名格式为 StarRocks-x 的版本。

StarRocks 支持前向兼容，因此您可以灰度升级。

> 注意：
>
> * 因为 StarRocks 支持 BE 后向兼容 FE，所以请务必**先升级 BE 节点，再升级 FE 节点**。错误的升级顺序可能导致新旧 FE、BE 节点不兼容，进而导致 BE 节点停止服务。
> * 请谨慎跨版本升级。如需跨版本升级，建议您在测试环境验证无误后再升级生产环境。DorisDB 升级至 Starrocks-2.x 版本时需要前置开启 CBO，因此您需要先将 DorisDB 升级至 StarRocks 1.19.x 版本。您可以在[官网](https://www.starrocks.com/zh-CN/download)获取 1.19.x 版本的安装包。

## 准备升级环境

> 注意：请务必确保环境变量配置准确。

1. 创建操作目录，设定 DorisDB 相关环境变量。

    ```bash
    # 创建升级操作目录用来保存备份数据，并进入当前目录。
    mkdir DorisDB-upgrade-$(date +"%Y-%m-%d") && cd DorisDB-upgrade-$(date +"%Y-%m-%d")
    # 设置当前目录路径作为环境变量，方便后续编写命令。
    export DSDB_UPGRADE_HOME=`pwd`
    # 将您当前已经安装的标准版 DorisDB 路径设置为环境变量。
    export DSDB_HOME=/path/to/DorisDB-x
    ```

    > 说明：将以上 `/path/to/DorisDB-x` 修改为标准版 DorisDB 的本地安装路径，例如，`xxx/DorisDB-SE-1.17.6`。

2. 下载并解压社区版 StarRocks 安装包，根据下载版本重命名解压后的路径，设定 StarRocks 相关环境变量。

    ```bash
    wget "https://xxx.StarRocks-SE-x.tar.gz" -O StarRocks-x.tar.gz
    tar -zxvf StarRocks-x.tar.gz
    # 将社区版 StarRocks 的解压文件路径设置为环境变量。
    export STARROCKS_HOME=/path/to/StarRocks-x
    ```

    > 说明：将以上 `https://xxx.StarRocks-SE-x.tar.gz` 修改为社区版 StarRocks 的下载路径，并将 `/path/to/StarRocks-x` 修改为社区版 StarRocks 的本地安装路径，例如，`xxx/StarRocks-1.18.4`。

## 升级 BE 节点

在升级 BE 节点时，你需要逐台升级 BE，以确保每台升级成功。

1. 停止标准版的 BE 停止

    ```bash
    # 停止BE
    cd ${DSDB_HOME}/be && ./bin/stop_be.sh
    ```

2. 将当前标准版 DorisDB 的 BE 节点安装路径下 **lib** 和 **bin** 目录移动至备份目录下。

    ```bash
    # 在 DSDB_UPGRADE_HOME 目录下创建 be 目录，用于备份当前标准版 BE 节点的 lib。
    cd ${DSDB_UPGRADE_HOME} && mkdir -p DorisDB-backups/be
    mv  ${DSDB_HOME}/be/lib DorisDB-backups/be/
    mv  ${DSDB_HOME}/be/bin DorisDB-backups/be/
    ```

3. 拷贝社区版 StarRocks 的 BE 相关目录到当前标准版 DorisDB 的 BE 的目录下。

    ```bash
    cp -rf ${STARROCKS_HOME}/be/lib/ ${DSDB_HOME}/be/lib
    cp -rf ${STARROCKS_HOME}/be/bin/ ${DSDB_HOME}/be/bin
    ```

4. 重新启动 BE。

    ```bash
    # 启动BE
    cd ${DSDB_HOME}/be && ./bin/start_be.sh --daemon
    ```

5. 验证 BE 运行状态。

    您可以：

    * 通过 MySQL 客户端查看 BE 运行状态以及版本信息。

    ```sql
    # 查看所有 BE 节点。
    SHOW BACKENDS;
    # 查看 BE 节点进程。
    SHOW PROC '/backends'\G;
    ```

    * 通过查看 **be/log/be.INFO** 中的日志输出是否正常判断 BE 运行状态。
    * 通过查看 **be/log/be.WARNING** 中是否有异常日志判断 BE 运行状态。

6. 确认当前 BE 节点运行正常后（推荐时间跨度为 10 分钟），按照上述流程执行其他 BE 节点。

## 升级 FE 节点

在升级 FE 节点时，你需要逐台升级 FE，，以确保每台升级成功。

> 注意：
>
> * FE 节点升级应按照先升级 Observer，再升级 Follower，最后升级 Leader 的顺序。
> * 请确保同步升级集群中未启动的 FE 节点，以防后续这些节点启动出现 FE 节点版本不一致的情况。

1. 停止当前 FE 节点。

    ```bash
    cd ${DSDB_HOME}/fe && ./bin/stop_fe.sh
    ```

2. 创建 FE 目录以备份标准版 DorisDB 的 FE 节点路径下的 **lib** 和 **bin** 目录。

    ```bash
    cd ${DSDB_UPGRADE_HOME} && mkdir -p DorisDB-backups/fe
    mv ${DSDB_HOME}/fe/bin DorisDB-backups/fe/
    mv ${DSDB_HOME}/fe/lib DorisDB-backups/fe/
    ```

3. 拷贝社区版 StarRocks 的 FE 相关目录到当前标准版 DorisDB 的 FE 的目录下。

    ```bash
    cp -r ${STARROCKS_HOME}/fe/lib/ ${DSDB_HOME}/fe
    cp -r ${STARROCKS_HOME}/fe/bin/ ${DSDB_HOME}/fe
    ```

4. 备份 `meta_dir` 下元数据信息。

    备份标准版 DorisDB 的 **/fe/conf/fe.conf** 中设置项 `meta_dir` 下的目录。默认为 `${DSDB_HOME}/doris-meta`。如果原目录名为 `doris-meta`，建议您将目录重命名，并更改配置文件。以下示例中设置为 `${DSDB_HOME}/meta`。

    > 注意：请确保 **命令中元数据目录** 和 **元数据实际目录** 以及 **配置项 `meta_dir`** 中的内容一致。

    a. 重命名目录。

    ```bash
    mv doris-meta/ meta
    ```

    b. 修改配置文件。

    ```bash
    # store metadata, create it if it is not exist.
    # Default value is ${DORIS_HOME}/doris-meta
    # meta_dir = ${DORIS_HOME}/doris-meta
    meta_dir=/path/to/DorisDB-x/meta
    ```

    > 说明：将以上 `/path/to/DorisDB-x/` 修改为标准版 DorisDB 的本地安装路径，例如，`xxx/DorisDB-SE-1.17.6`。

    c. 备份 **meta** 目录下的文件。

    ```bash
    cd /path/to/DorisDB-x
    cp -r meta meta-$(date +"%Y-%m-%d")
    ```

    > 说明：将以上 `/path/to/DorisDB-x` 修改为 **meta** 的上层路径，例如，`xxx/DorisDB-SE-1.17.6`。

5. 启动 FE 节点。

    ```bash
    cd ${DSDB_HOME}/fe && ./bin/start_fe.sh --daemon
    ```

6. 验证 FE 运行状态。

    您可以：

    * 通过 MySQL 客户端查看 FE 运行状态以及版本信息。

    ```sql
    SHOW FRONTENDS\G;
    ```

    * 通过查看 **fe/log/fe.INFO** 中的日志输出是否正常判断 BE 运行状态。
    * 通过查看 **fe/log/fe.WARNING** 中是否有异常日志判断 BE 运行状态。

7. 确认当前 FE 节点运行正常后（推荐时间跨度为 10 分钟），按照上述流程执行其他 FE 节点。

## 升级 Broker

1. 停止当前 Broker。

    ```bash
    cd ${DSDB_HOME}/apache_hdfs_broker && ./bin/stop_broker.sh
    ```

2. 备份当前标准版 DorisDB 的 Broker 安装目录下的 **lib** 和 **bin**。

    ```bash
    cd ${DSDB_UPGRADE_HOME} && mkdir -p DorisDB-backups/apache_hdfs_broker
    mv ${DSDB_HOME}/apache_hdfs_broker/lib DorisDB-backups/apache_hdfs_broker/
    mv ${DSDB_HOME}/apache_hdfs_broker/bin DorisDB-backups/apache_hdfs_broker/
    ```

3. 拷贝社区版 StarRocks Broker 安装目录下的 **lib** 和 **bin** 到当前 Broker 的目录下。

    ```bash
    cp -rf ${STARROCKS_HOME}/apache_hdfs_broker/lib ${DSDB_HOME}/apache_hdfs_broker
    cp -rf ${STARROCKS_HOME}/apache_hdfs_broker/bin ${DSDB_HOME}/apache_hdfs_broker
    ```

4. 启动当前 Broker。

    ```bash
    cd ${DSDB_HOME}/apache_hdfs_broker && ./bin/start_broker.sh --daemon
    ```

5. 验证 Broker 运行状态。

    您可以：

    * 通过 MySQL 客户端查看 Broker 运行状态以及版本信息。

    ```sql
    SHOW BROKER;
    ```

    * 通过查看 **apache_hdfs_broker/log/broker.INFO** 中的日志输出是否正常判断 BE 运行状态。
    * 通过查看 **apache_hdfs_broker/log/broker.WARNING** 中是否有异常日志判断 BE 运行状态。

6. 确认当前 Broker 运行正常后（推荐时间跨度为 10 分钟），按照上述流程执行其他 Broker。

## 回滚方案

从标准版 DorisDB 升级的社区版 StarRocks 暂时不支持回滚。建议您在测试环境验证测试成功后再升级至生产环境。

如果遇到无法解决的问题，您可以添加下方企业微信寻求帮助。

![二维码](../assets/8.3.1.png)
