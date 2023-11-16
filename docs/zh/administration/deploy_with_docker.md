---
displayed_sidebar: "Chinese"
---

# 使用 Docker 部署

本文介绍如何以 Docker 镜像的方式手动部署 StarRocks。

## 前提条件

|分类|描述|
|---------|--------|
|硬件要求|<ul><li>CPU 需支持 AVX2 指令集</li><li>建议配置 8 核 或以上 CPU，16GB 或以上内存。</li></ul>|
|操作系统|Linux kernel  3.10 以上。|
|软件要求|<ul><li>Docker</li><li>MySQL 客户端（5.5 或以上）</li></ul>|

## 创建 Dockerfile

创建以下 Dockerfile。

```shell
FROM centos:centos7

# Prepare StarRocks Installer. Replace the "2.4.0" below with the StarRocks version that you want to deploy.
ENV StarRocks_version=2.4.0

# Create directory for deployment.
ENV StarRocks_home=/data/deploy

# Replace the "<url_to_download_specific_ver_of_starrocks>" below with the download path of the StarRocks that you want to deploy.
ENV StarRocks_url=<url_to_download_specific_ver_of_starrocks>

# Install StarRocks.
RUN yum -y install wget
RUN mkdir -p $StarRocks_home
RUN wget -SO $StarRocks_home/StarRocks-${StarRocks_version}.tar.gz  $StarRocks_url
RUN cd $StarRocks_home && tar zxf StarRocks-${StarRocks_version}.tar.gz

# Install Java JDK.
RUN yum -y install java-1.8.0-openjdk-devel.x86_64
RUN rpm -ql java-1.8.0-openjdk-devel.x86_64 | grep bin$

# Create directory for FE meta and BE storage in StarRocks.
RUN mkdir -p $StarRocks_home/StarRocks-${StarRocks_version}/fe/meta
RUN mkdir -p $StarRocks_home/StarRocks-${StarRocks_version}/be/storage

# Install relevant tools.
RUN yum -y install mysql net-tools telnet

# Run Setup script.
COPY run_script.sh $StarRocks_home/run_script.sh
RUN chmod +x $StarRocks_home/run_script.sh
CMD $StarRocks_home/run_script.sh
```

> 注意：将以上 `<url_to_download_specific_ver_of_starrocks>` 替换为实际[下载地址](https://www.mirrorship.cn/zh-CN/download/community)。

## 创建脚本文件

构建脚本文件 `run_script.sh` 以配置并启动 StarRocks。

```shell
#!/bin/bash


# Set JAVA_HOME.

JAVA_INSTALL_DIR=/usr/lib/jvm/$(rpm -aq | grep java-1.8.0-openjdk-1.8.0)
export JAVA_HOME=$JAVA_INSTALL_DIR

# Start FE.
cd $StarRocks_home/StarRocks-$StarRocks_version/fe/bin/
./start_fe.sh --daemon

# Start BE.
cd $StarRocks_home/StarRocks-$StarRocks_version/be/bin/
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

  if [ ${PROCESS_STATUS} -ne 0 ]; then
    echo "one of the starrocks process already exit."
    exit 1;
  fi
done
```

## 搭建 Docker 镜像

运行以下命令搭建 Docker 镜像。

```shell
docker build --no-cache --progress=plain -t starrocks:1.0 .
```

## 启动 Docker 容器

运行以下命令启动 Docker 容器。

```shell
# 1、端口映射启动方式
docker run -p 9030:9030 -p 8030:8030 -p 8040:8040 --privileged=true -itd --name starrocks-test starrocks:1.0

# 2、同宿主机网络环境启动方式（避免使用stream load导入数据等找不到be节点ip地址）
docker run  --network host  --privileged=true -itd --name starrocks-test starrocks:1.0
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
"replication_num" = "3",
"storage_format" = "DEFAULT"
);


insert into sr_on_mac values (1, '2022-02-01', '2022-02-01 10:47:57', '111');
insert into sr_on_mac values (2, '2022-02-02', '2022-02-02 10:47:57', '222');
insert into sr_on_mac values (3, '2022-02-03', '2022-02-03 10:47:57', '333');


select * from sr_on_mac where c1 >= '2022-02-02';
```

如果无错误返回，则表明您已成功在 Docker 环境中部署 StarRocks。
