---
displayed_sidebar: "Chinese"
---

# 通过源码编译 StarRocks

本文介绍如何通过 Docker 镜像编译 StarRocks。

## 前提条件

在编译 StarRocks 之前，请确保您已安装 [Docker](https://www.docker.com/get-started/)。

## 下载镜像

从 Docker Hub 下载开发环境的镜像文件。该镜像中集成了 LLVM 及 Clang 作为第三方工具。

```shell
docker pull starrocks/dev-env:{branch-name}
```

> 说明：请使用下表中相应的镜像 Tag 替换命令中的 `{branch-name}`。

StarRocks 版本分支与开发环境镜像版本的对应关系如下所示：

| StarRocks 版本    | 镜像 Tag                      |
| ---------------- | ------------------------------|
| main             | starrocks/dev-env:main        |
| StarRocks-2.4.*  | starrocks/dev-env:branch-2.4  |
| StarRocks-2.3.*  | starrocks/dev-env:branch-2.3  |
| StarRocks-2.2.*  | starrocks/dev-env:branch-2.2  |
| StarRocks-2.1.*  | starrocks/dev-env:branch-2.1  |
| StarRocks-2.0.*  | starrocks/dev-env:branch-2.0  |
| StarRocks-1.19.* | starrocks/dev-env:branch-1.19 |

## 编译 StarRocks

您可以通过挂载本地存储（推荐）或拷贝 GitHub 代码库的方式编译 StarRocks。

- 挂载本地存储编译 StarRocks。

  ```shell
  mkdir {local-path}
  cd {local-path}

  git clone https://github.com/StarRocks/starrocks.git
  cd starrocks
  git checkout {branch-name}

  docker run -it -v {local-path}/.m2:/root/.m2 -v {local-path}/starrocks:/root/starrocks --name {branch-name} -d starrocks/dev-env:{branch-name}

  docker exec -it {branch-name} /root/starrocks/build.sh
  ```

  > 说明：该方式可避免在 Docker 容器中重复下载 **.m2** 内的 Java 依赖，且无需从 Docker 容器中复制 **starrocks/output** 内已编译好的二进制包。

- 不使用本地存储编译 StarRocks。

  ```shell
  docker run -it --name {branch-name} -d starrocks/dev-env:{branch-name}
  docker exec -it {branch-name} /bin/bash
  
  # 下载 StarRocks 代码。
  git clone https://github.com/StarRocks/starrocks.git
  
  cd starrocks
  sh build.sh
  ```
