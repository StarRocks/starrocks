---
displayed_sidebar: docs
---

# 基于 Ubuntu 编译 StarRocks

本文介绍如何基于 Ubuntu 操作系统编译 StarRocks。StarRocks 支持基于 x86_64 和 AArch64 架构编译。

## 前提条件

### 安装依赖

运行以下命令安装依赖：

```bash
sudo apt-get update
```

```bash
sudo apt-get install automake binutils-dev bison byacc ccache flex libiberty-dev libtool maven zip python3 python-is-python3 -y
```

### 安装编译器

如果您使用的 Ubuntu 为 22.04 版本及以上，运行以下命令安装编译器：

```bash
sudo apt-get install cmake gcc g++ default-jdk -y
```

如果您使用的 Ubuntu 版本为 22.04 以下，请运行以下命令检查工具和编译器的版本：

1. 检查 GCC/G++ 版本：

   ```bash
   gcc --version
   g++ --version
   ```

   GCC/G++ 的版本必须为 10.3 或以上。如果您使用的版本较低，[请点此安装 GCC/G++](https://gcc.gnu.org/releases.html)。

2. 检查 JDK 版本：

   ```bash
   java --version
   ```

   OpenJDK 的版本必须为 8 或以上。如果您使用的版本较低，[请点此安装 OpenJDK](https://openjdk.org/install)。

3. 检查 CMake 版本：

   ```bash
   cmake --version
   ```

   CMake 的版本必须为 3.20.1 或以上。如果您使用的版本较低，[请点此安装 CMake](https://cmake.org/download)。

## 编译 StarRocks

运行以下命令开始编译：

```bash
./build.sh
```

默认的编译并行度等于 **CPU 核心数/4**。假设您有 32 个 CPU 核心，则默认并行度为 8。

如果您想调整并行度，可以在命令行中通过 `-j` 指定编译使用的核心数。

以下示例指定系统使用 24 个核心进行编译。

```bash
./build.sh -j 24
```

## 常见问题解答

Q: 在 Ubuntu 20.04 中构建 `aws_cpp_sdk` 失败，并返回错误 "Error: undefined reference to pthread_create"。我应该如何解决？

A: 该错误的原因为 CMake 版本较低，请将 CMake 版本升级至 3.20.1 以上。
