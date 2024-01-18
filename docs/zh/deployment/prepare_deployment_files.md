---
displayed_sidebar: "Chinese"
---

# 准备部署文件

本文介绍如何准备 StarRocks 部署文件。

目前 [镜舟官网](https://www.mirrorship.cn/zh-CN/download/community)提供的 StarRocks 软件包仅支持在 x86 架构 CPU 的 CentOS 7.9 平台上部署。如需在 ARM 架构 CPU 或 Ubuntu 22.04 操作系统上部署 StarRocks，您需要通过 StarRocks Docker 镜像获取部署文件。

## 为 x86 架构 CentOS 7.9 平台准备部署文件

StarRocks 二进制包的名称格式为 **StarRocks-version.tar.gz**，其中 **version** 是一个数字（例如 **2.5.2**），表示二进制包的版本信息。请确保您选择了正确版本的二进制包。

### 步骤

1. 您可选择从 [下载 StarRocks](https://www.starrocks.io/download/community) 页面直接下载 StarRocks 二进制包，或在终端中运行以下命令获取：

   ```Bash
   # 将 <version> 替换为您想要下载的 StarRocks 版本，例如 2.5.4。
   wget https://releases.starrocks.io/starrocks/StarRocks-<version>.tar.gz
   ```

2. 解压二进制包。

   ```Bash
   # 将 <version> 替换为您已下载的 StarRocks 版本，例如 2.5.4。
   tar -xzvf StarRocks-<version>.tar.gz
   ```

   二进制包中包含以下路径及文件：

   | **路径/文件**          | **说明**                                                     |
   | ---------------------- | ------------------------------------------------------------ |
   | **apache_hdfs_broker** | Broker 节点的部署路径。自 StarRocks 2.5 起，您无需在一般场景中部署 Broker 节点。如果您确实需要在 StarRocks 集群中部署 Broker 节点，请参阅 [部署 Broker 节点](../deployment/deploy_broker.md) 了解详细说明。 |
   | **fe**                 | FE 节点的部署路径。                                          |
   | **be**                 | BE 节点的部署路径。                                          |
   | **LICENSE.txt**        | StarRocks license 文件。                                     |
   | **NOTICE.txt**         | StarRocks notice 文件。                                      |

3. 将路径 **fe** 分发至所有 FE 实例，将路径 **be** 分发至所有 BE 或 CN 实例以用于[手动部署](../deployment/deploy_manually.md)。

## 为 ARM 架构 CPU 或 Ubuntu 22.04 平台准备部署文件

### 前提条件

您需要在计算机上安装 [Docker Engine](https://docs.docker.com/engine/install/) (17.06.0 以上)。

### 步骤

1. 从 [StarRocks Docker Hub](https://hub.docker.com/r/starrocks/artifacts-ubuntu/tags) 下载 StarRocks Docker 镜像。 您可以根据 Tag 选择特定版本的镜像。

   - 如果您使用 Ubuntu 22.04 平台：

     ```Bash
     # 将 <image_tag> 替换为您要下载的镜像的 Tag，例如 2.5.4。
     docker pull starrocks/artifacts-ubuntu:<image_tag>
     ```

   - 如果您使用 ARM 架构 CentOS 7.9 平台：

     ```Bash
     # 将 <image_tag> 替换为您要下载的镜像的 Tag，例如 2.5.4。
     docker pull starrocks/artifacts-centos7:<image_tag>
     ```

2. 运行以下命令将 StarRocks 部署文件从 Docker 镜像复制到您的主机：

   - 如果您使用 Ubuntu 22.04 平台：

     ```Bash
     # 将 <image_tag> 替换为您下载的镜像的 Tag，例如 2.5.4。
     docker run --rm starrocks/artifacts-ubuntu:<image_tag> \
         tar -cf - -C /release . | tar -xvf -
     ```

   - 如果您使用 ARM 架构 CentOS 7.9 平台：

     ```Bash
     # 将 <image_tag> 替换为您下载的镜像的 Tag，例如 2.5.4。
     docker run --rm starrocks/artifacts-centos7:<image_tag> \
         tar -cf - -C /release . | tar -xvf -
     ```

   部署文件包括以下路径：

   | **路径**             | **说明**                                                     |
   | -------------------- | ------------------------------------------------------------ |
   | **be_artifacts**     | 该路径下包含 BE 或 CN 节点的部署路径 **be**、StarRocks License 文件 **LICENSE.txt** 以及 StarRocks notice 文件 **NOTICE.txt。** |
   | **broker_artifacts** | 该路径下包含 Broker 节点的部署路径 **apache_hdfs_broker**。  |
   | **fe_artifacts**     | 该路径下包含 FE 节点的部署路径 **fe**、StarRocks License 文件 **LICENSE.txt** 以及 StarRocks notice 文件 **NOTICE.txt。** |

3. 将路径 **fe** 分发至所有 FE 实例，将路径 **be** 分发至所有 BE 或 CN 实例以用于[手动部署](../deployment/deploy_manually.md)。
