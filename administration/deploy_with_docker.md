# 使用 Docker 部署

本文介绍如何以 Docker 镜像的方式手动部署 StarRocks。

## 前提条件

|分类|描述|
|---------|--------|
|硬件要求|<ul><li>CPU 需支持 AVX2 指令集</li><li>建议配置 8 核 或以上 CPU，16GB 或以上内存。</li></ul>|
|操作系统|CentOS（7 或以上）|
|软件要求|<ul><li>Docker</li><li>MySQL 客户端（5.5 或以上）</li></ul>|

## 创建 Dockerfile

创建以下 Dockerfile。

```shell
FROM centos:centos7

# Prepare StarRocks Installer.
RUN yum -y install wget
RUN mkdir -p /data/deploy/ 
RUN wget -SO /data/deploy/StarRocks-x.x.x.tar.gz <url_to_download_specific_ver_of_starrocks>
RUN cd /data/deploy/ && tar zxf StarRocks-x.x.x.tar.gz

# Install Java JDK.
RUN yum -y install java-1.8.0-openjdk-devel.x86_64
RUN rpm -ql java-1.8.0-openjdk-devel.x86_64 | grep bin$
RUN /usr/lib/jvm/java-1.8.0-openjdk-1.8.0.342.b07-1.el7_9.x86_64/bin/java -version

# Create directory for FE meta and BE storage in StarRocks.
RUN mkdir -p /data/deploy/StarRocks-x.x.x/fe/meta
RUN jps
RUN mkdir -p /data/deploy/StarRocks-x.x.x/be/storage

# Install relevant tools.
RUN yum -y install mysql net-tools telnet

# Run Setup script.
COPY run_script.sh /data/deploy/run_script.sh
RUN chmod +x /data/deploy/run_script.sh
CMD /data/deploy/run_script.sh
```

> 注意：将以上 `<url_to_download_specific_ver_of_starrocks>` 替换为实际[下载地址](https://www.starrocks.com/zh-CN/download)，并将 `StarRocks-x.x.x` 替换为实际安装版本。

## 创建脚本文件

构建脚本文件 `run_script.sh` 以配置并启动 StarRocks。

```shell

#!/bin/bash

# Set JAVA_HOME.
export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.342.b07-1.el7_9.x86_64

# Start FE.
cd /data/deploy/StarRocks-x.x.x/fe/bin/
./start_fe.sh --daemon

# Start BE.
cd /data/deploy/StarRocks-x.x.x/be/bin/
./start_be.sh --daemon

# Sleep until the cluster starts.
sleep 30;

# Set BE server IP.
IP=$(ifconfig eth0 | grep 'inet' | cut -d: -f2 | awk '{print $2}')
mysql -uroot -h${IP} -P 9030 -e "alter system add backend '${IP}:9050';"

# Loop to detect the process.
while sleep 60; do
  ps aux | grep starrocks | grep -q -v grep
  PROCESS_STATUS=$?

  if [ PROCESS_STATUS -ne 0 ]; then
    echo "one of the starrocks process already exit."
    exit 1;
  fi
done
```

> 注意：将以上 `StarRocks-x.x.x` 替换为实际安装版本。

## 搭建 Docker 镜像

运行以下命令搭建 Docker 镜像。

```shell
docker build --no-cache --progress=plain -t starrocks:1.0 .
```

## 启动 Docker 容器

运行以下命令启动 Docker 容器。

```shell
docker run -p 9030:9030 -p 8030:8030 -p 8040:8040 --privileged=true -itd --name starrocks-test starrocks:1.0
```

## 连接 StarRocks

当 Docker 容器成功启动后，运行以下命令连接 StarRocks。

```shell
mysql -uroot -h127.0.0.1 -P 9030
```

## 确认部署成功

您可以运行以下 SQL 确认 StarRocks 是否部署成功。

```sql
CREATE DATABASE TEST;

USE TEST;

CREATE TABLE `sr_on_mac` (
 `c0` int(11) NULL COMMENT "",
 `c1` date NULL COMMENT "",
 `c2` datetime NULL COMMENT "",
 `c3` varchar(65533) NULL COMMENT ""
) ENGINE=OLAP 
DUPLICATE KEY(`c0`)
PARTITION BY RANGE (c1) (
  START ("2022-02-01") END ("2022-02-10") EVERY (INTERVAL 1 DAY)
)
DISTRIBUTED BY HASH(`c0`) BUCKETS 1 
PROPERTIES (
"replication_num" = "1",
"in_memory" = "false",
"storage_format" = "DEFAULT"
);


insert into sr_on_mac values (1, '2022-02-01', '2022-02-01 10:47:57', '111');
insert into sr_on_mac values (2, '2022-02-02', '2022-02-02 10:47:57', '222');
insert into sr_on_mac values (3, '2022-02-03', '2022-02-03 10:47:57', '333');


select * from sr_on_mac where c1 >= '2022-02-02';
```

如果无错误返回，则表明您已成功在 Docker 环境中部署 StarRocks。
