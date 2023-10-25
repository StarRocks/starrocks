# 标准版 DorisDB-x 升级到社区版 StarRocks-x

> 注意：StarRocks 可以做到前向兼容，所以在升级的时候可以做到灰度升级，但是一定是先升级 BE 再升级 FE。(标准版安装包命名为 DorisDB *, 社区版安装包命名格式为 StarRocks*)

## 准备升级环境

> 注意：请留意环境变量配置准确

1. 创建操作目录，准备环境
  
    ```bash
    # 创建升级操作目录用来保存备份数据，并进入当前目录
    mkdir DorisDB-upgrade-$(date +"%Y-%m-%d") && cd DorisDB-upgrade-$(date +"%Y-%m-%d")
    # 设置当前目录路径作为环境变量，方便后续编写命令
    export DSDB_UPGRADE_HOME=`pwd`
    # 将您当前已经安装的标准版（DorisDB）路径设置为环境变量
    export DSDB_HOME=标准版的安装路径，形如"xxx/DorisDB-SE-1.17.6"
    ```

2. 下载社区版（StarRocks）安装包，并解压，解压文件根据您下载版本进行重命名
  
    ```bash
    wget "https://xxx.StarRocks-SE-1.18.4.tar.gz" -O StarRocks-1.18.4.tar.gz
    tar -zxvf StarRocks-1.18.4.tar.gz
    # 将社区版的解压文件路径设置为环境变量
    export STARROCKS_HOME= 社区版的解压安装路径，形如"xxx/StarRocks-1.18.4"
    ```

## 升级 BE

逐台升级 BE，确保每台升级成功，保证对数据没有影响

1. 先将标准版的 BE 停止

    ```bash
    # 停止BE
    cd ${DSDB_HOME}/be && ./bin/stop_be.sh
    ```

2. 当前标准版 BE 的 lib 和 bin 移动到备份目录下

    ```bash
    # 在DSDB_UPGRADE_HOME 目录下创建be目录，用于备份当前标准版be的lib
    cd ${DSDB_UPGRADE_HOME} && mkdir -p DorisDB-backups/be
    mv  ${DSDB_HOME}/be/lib DorisDB-backups/be/
    mv  ${DSDB_HOME}/be/bin DorisDB-backups/be/
    ```

3. 拷贝社区版（StarRocks）的 BE 文件到当前标准版(DorisDB)的 BE 的目录下

    ```bash
    cp -rf ${STARROCKS_HOME}/be/lib/ ${DSDB_HOME}/be/lib
    cp -rf ${STARROCKS_HOME}/be/bin/ ${DSDB_HOME}/be/bin
    ```

4. 重新启动 BE

    ```bash
    # 启动BE
    cd ${DSDB_HOME}/be && ./bin/start_be.sh --daemon
    ```

5. 验证 BE 是否正确运行  

    a. 在 MySQL 客户端执行 show backends/show proc '/backends' \G 查看 BE 是否成功启动并且版本是否正确  

    b. 观察 be/log/be.INFO 的日志是否正常  

    c. 观察 be/log/be.WARNING 是否有异常日志  

6. 第一台观察 10 分钟后，按照上述流程执行其他 BE 节点

## 升级 FE

> 注意：
>
> * 逐台升级 FE，先升级 Observer，再升级 Follower，最后升级 Master
>
> * 请将没有启动 FE 的节点的 FE 同步升级，以防后续这些节点启动 FE 出现 FE 版本不一致的情况

1. 先将当前节点 FE 停止

    ```bash
    cd ${DSDB_HOME}/fe && ./bin/stop_fe.sh
    ```

2. 创建 FE 目录备份标准版的 FE 的 lib 和 bin

    ```bash
    cd ${DSDB_UPGRADE_HOME} && mkdir -p DorisDB-backups/fe
    mv ${DSDB_HOME}/fe/bin DorisDB-backups/fe/
    mv ${DSDB_HOME}/fe/lib DorisDB-backups/fe/
    ```

3. 拷贝社区版 FE 的 lib 和 bin 文件到标准版 FE 的目录下  

    ```bash
    cp -r ${STARROCKS_HOME}/fe/lib/ ${DSDB_HOME}/fe
    cp -r ${STARROCKS_HOME}/fe/bin/ ${DSDB_HOME}/fe
    ```

4. 备份 meta_dir 元数据信息  
  
    a. 将标准版的/fe/conf/fe.conf 中设置的 meta_dir 目录进行备份，如未更改过配置文件中 meta_dir 属性，默认为 `\${DSDB_HOME}/doris-meta`；下例中设置为 `\${DSDB_HOME}/meta`,  
  
    b. 注意保证 "命令中元数据目录" 和 "元数据实际目录" 还有 "配置文件" 中一致，如您原目录名为 "doris-meta"，建议您将目录重命名，同步需要更改配置文件  
  
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
  
    其中，meta_dir 设置为创建的元数据存储目录路径

    ```bash
    cd "配置文件中设置的meta_dir目录,到meta的上级目录"
    # 拷贝meta文件，meta为本例中设置路径，请根据您配置文件中设置进行更改
    cp -r meta meta-$(date +"%Y-%m-%d")
    ```
  
5. 启动 FE 服务

    ```bash
    cd ${DSDB_HOME}/fe && ./bin/start_fe.sh --daemon
    ```

6. 验证 FE 是否正确运行  

    a. 在 MySQL 客户端执行 `show frontends\G` 查看 FE 是否成功启动并且版本是否正确  

    b. 观察 fe/log/fe.INFO 的日志是否正常  

    c. 观察 fe/log/fe.WARNING 是否有异常日志  

7. 第一台观察 10 分钟后，按照上述流程执行其他 FE 节点

## 升级 brokers

1. 停止当前节点的 broker

    ```bash
    cd ${DSDB_HOME}/apache_hdfs_broker && ./bin/stop_broker.sh
    ```

2. 备份当前标准版的 Broker 运行的 lib 和 bin

    ```bash
    cd ${DSDB_UPGRADE_HOME} && mkdir -p DorisDB-backups/apache_hdfs_broker
    mv ${DSDB_HOME}/apache_hdfs_broker/lib DorisDB-backups/apache_hdfs_broker/
    mv ${DSDB_HOME}/apache_hdfs_broker/bin DorisDB-backups/apache_hdfs_broker/
    ```

3. 拷贝社区版本 Broker 的 lib 和 bin 文件到执行 Broker 的目录下

    ```bash
    cp -rf ${STARROCKS_HOME}/apache_hdfs_broker/lib ${DSDB_HOME}/apache_hdfs_broker
    cp -rf ${STARROCKS_HOME}/apache_hdfs_broker/bin ${DSDB_HOME}/apache_hdfs_broker
    ```

4. 启动当前 Broker

    ```bash
    cd ${DSDB_HOME}/apache_hdfs_broker && ./bin/start_broker.sh --daemon
    ```

5. 验证 Broker 是否正确运行

    a. 在 MySQL 客户端执行 show broker 查看 Broker 是否成功启动并且版本是否正确

    b. 观察 broker/log/broker.INFO 的日志是否正常  

    c. 观察 broker/log/broker.WARNING 是否有异常日志  

6. 按照上述流程执行其他 Broker 节点

## 回滚方案

> 注意：从标准版 DorisDB 升级至社区版 StarRocks，暂时不支持回滚，建议先在测试环境验证测试没问题后再升级线上。如有遇到问题无法解决，可以添加下面企业微信寻求帮助。

![二维码](../assets/8.3.1.png)

## 注意事项

1. 需要先升级 BE、再升级 FE，因为 Starrocks 的标准版中 BE 是兼容 FE 的。升级 BE 的过程中，需要进行灰度升级，先升级一台 BE，过一天观察无误，再升级其他 FE。

2. 跨版本升级时需要谨慎，测试环境验证后再操作，且升级 Starrocks-2.x 版本时需要前置开启 CBO，需要升级至 2.x 版本的用户需先升级 1.19.x 版本。可在官网获取最新版本的 1.19.x 安装包（1.19.7）。
