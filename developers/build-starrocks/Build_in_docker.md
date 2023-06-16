# 使用 Docker 编译 StarRocks

本文介绍如何使用 Docker 编译 StarRocks。

## 概述

StarRocks 提供 Ubuntu 22.04 和 CentOS 7.9 的开发环境镜像。通过该镜像，您可以在 Docker 容器中编译 StarRocks。

### StarRocks 版本和开发环境镜像

StarRocks 的不同版本对应 [StarRocks Docker Hub](https://hub.docker.com/u/starrocks) 上提供的不同开发环境镜像。

- Ubuntu 22.04:

  | **分支名** | **镜像名**                          |
  | ---------- | ----------------------------------- |
  | main       | starrocks/dev-env-ubuntu:latest     |
  | branch-3.0 | starrocks/dev-env-ubuntu:3.0-latest |
  | branch-2.5 | starrocks/dev-env-ubuntu:2.5-latest |

- For CentOS 7.9:

  | **分支名** | **镜像名**                           |
  | ---------- | ------------------------------------ |
  | main       | starrocks/dev-env-centos7:latest     |
  | branch-3.0 | starrocks/dev-env-centos7:3.0-latest |
  | branch-2.5 | starrocks/dev-env-centos7:2.5-latest |

## 前提条件

在编译 StarRocks 前，请确保满足以下要求：

- **硬件**

  机器必须有 8 GB 以上内存

- **软件**

  - 机器必须运行 Ubuntu 22.04 或 CentOS 7.9
  - 机器必须安装 Docker

## 第一步：下载镜像

运行以下命令下载开发环境镜像：

```Bash
# 将 <image_name> 替换为您要下载的镜像的名称，例如 `starrocks/dev-env-ubuntu:latest`。
# 请确保为您的操作系统选择正确的镜像。
docker pull <image_name>
```

Docker 会自动识别机器的 CPU 架构，并下载对应的镜像。 其中 `linux/amd64` 镜像适用于基于 x86 架构的 CPU，而 `linux/arm64` 镜像适用于基于 ARM 架构的 CPU。

## 第二步：在 Docker 容器中编译 StarRocks

您可以在启动开发环境 Docker 容器时选择是否挂载本机路径。建议您选择挂载本地主机路径，从而避免下次编译时重新下载 Java 依赖，也无需手动将容器中的二进制文件复制到本机。

- **启动 Docker 容器并挂载本机路径：**

  1. 将 StarRocks 源码克隆至本机。

     ```Bash
     git clone https://github.com/StarRocks/starrocks.git
     ```

  2. 启动容器。

     ```Bash
     # 将 <code_dir> 替换为 StarRocks 源代码目录的上级目录。
     # 将 <branch_name> 替换为镜像名称对应的分支名称。
     # 将 <image_name> 替换为您下载的镜像的名称。
     docker run -it -v <code_dir>/.m2:/root/.m2 \
         -v <code_dir>/starrocks:/root/starrocks \
         --name <branch_name> -d <image_name>
     ```

  3. 在容器内启动 bash shell。

     ```Bash
     # 将 <branch_name> 替换为镜像名称对应的分支名称。
     docker exec -it <branch_name> /bin/bash
     ```

  4. 在容器内编译 StarRocks。

     ```Bash
     cd /root/starrocks && ./build.sh
     ```

- **启动 Docker 容器，不挂载本机路径：**

  1. 启动容器

     ```Bash
     # 将 <branch_name> 替换为镜像名称对应的分支名称。
     # 将 <image_name> 替换为您下载的镜像的名称。
     docker run -it --name <branch_name> -d <image_name>
     ```

  2. 在容器内启动 bash shell。

     ```Bash
     # 将 <branch_name> 替换为镜像名称对应的分支名称。
     docker exec -it <branch_name> /bin/bash
     ```

  3. 将 StarRocks 源码克隆至容器。

     ```Bash
     git clone https://github.com/StarRocks/starrocks.git
     ```

  4. 在容器内编译 StarRocks。

     ```Bash
     cd starrocks && ./build.sh
     ```

## 故障排除

Q：StarRocks BE 编译失败，返回如下错误信息：

```Plain
g++: fatal error: Killed signal terminated program cc1plus
compilation terminated.
```

我该如何处理？

A：此错误消息表示 Docker 容器内存不足。您需要为容器分配至少 8 GB 的内存资源。
