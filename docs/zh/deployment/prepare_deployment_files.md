---
displayed_sidebar: docs
---

# 准备部署文件

本文介绍如何准备 StarRocks 部署文件。

目前 [镜舟官网](https://www.mirrorship.cn/zh-CN/download/community)提供的 StarRocks 软件包仅支持在 x86 架构的 CPU 上部署。如需在 ARM 架构 CPU 上部署 StarRocks，您需要通过 StarRocks Docker 镜像获取部署文件。

## 为 x86 架构 CPU 准备部署文件

自 v3.1.14、v3.2.10 和 v3.3.3 版本起，StarRocks 的二进制包的名称格式为 `StarRocks-{Version}-{OS}-{ARCH}.tar.gz`，其中 `Version` 表示版本号（例如 `3.3.3`），`OS` 表示操作系统（包括 `centos` 和 `ubuntu`），`ARCH` 表示 CPU 架构（目前仅支持 `amd64`，相当于 x86_64）。请确保您选择了正确版本的发行包。

:::note

在 v3.1.14、v3.2.10 和 v3.3.3 版本之前，StarRocks 二进制包的名称格式为 `StarRocks-version.tar.gz`。

:::

### 步骤

1. 您可选择从 [下载 StarRocks](https://www.starrocks.io/download/community) 页面直接下载 StarRocks 二进制包，或在终端中运行以下命令获取：

   ```Bash
   # 将 <version> 替换为您想要下载的 StarRocks 版本，例如 3.3.3，
   # 并将 <OS> 替换为 centos 或 ubuntu。
   wget https://releases.starrocks.io/starrocks/StarRocks-<version>-<OS>-amd64.tar.gz
   ```

2. 解压二进制包。

   ```Bash
   # 将 <version> 替换为您想要下载的 StarRocks 版本，例如 3.3.3，
   # 并将 <OS> 替换为 centos 或 ubuntu。
   tar -xzvf StarRocks-<version>-<OS>-amd64.tar.gz
   ```

   二进制包中包含以下路径及文件：

   | **路径/文件**          | **说明**                 |
   | ---------------------- | ------------------------ |
   | **apache_hdfs_broker** | Broker 节点的部署路径。  |
   | **fe**                 | FE 节点的部署路径。      |
   | **be**                 | BE 节点的部署路径。      |
   | **LICENSE.txt**        | StarRocks license 文件。 |
   | **NOTICE.txt**         | StarRocks notice 文件。  |

3. 将路径 **fe** 分发至所有 FE 实例，将路径 **be** 分发至所有 BE 或 CN 实例以用于[手动部署](../deployment/deploy_manually.md)。

## 为 ARM 架构 CPU 准备部署文件

### 前提条件

您需要在计算机上安装 [Docker Engine](https://docs.docker.com/engine/install/) (17.06.0 以上)。

### 步骤

从 v3.1.14、v3.2.10 和 v3.3.3 版本开始，StarRocks 提供的 Docker 镜像命名格式为 `starrocks/{Component}-{OS}:{Version}`，其中 `Component` 表示镜像的组件（包括 `fe`、`be`和 `cn`），`OS` 表示操作系统（包括 `centos` 和 `ubuntu`），`Version` 表示版本号（例如 `3.3.3`）。Docker 将自动识别您的 CPU 架构并拉取相应的镜像。请确保您选择了正确版本的镜像。

:::note

在 v3.1.14、v3.2.10 和 v3.3.3 版本之前，StarRocks 提供的 Docker 镜像位于 `starrocks/artifacts-ubuntu` 和 `starrocks/artifacts-centos7` 仓库中。

:::

1. 从 [StarRocks Docker Hub](https://hub.docker.com/r/starrocks/artifacts-ubuntu/tags) 下载 StarRocks Docker 镜像。 您可以根据 Tag 选择特定版本的镜像。

   ```Bash
   # 将 <component> 替换为您需要下载组建，例如 fe，
   # 将 <version> 替换为您想要下载的 StarRocks 版本，例如 3.3.3，
   # 并将 <OS> 替换为 centos 或 ubuntu。
   docker pull starrocks/<Component>-<OS>:<version>
   ```

2. 运行以下命令将 StarRocks 部署文件从 Docker 镜像复制到您的主机：

   ```Bash
   # 将 <component> 替换为您需要下载组建，例如 fe，
   # 将 <version> 替换为您想要下载的 StarRocks 版本，例如 3.3.3，
   # 并将 <OS> 替换为 centos 或 ubuntu。
   docker run --rm starrocks/<Component>-<OS>:<version> \
       tar -cf - -C /release . | tar -xvf -
   ```

3. 将部署文件分发至所有实例以用于[手动部署](../deployment/deploy_manually.md)。
