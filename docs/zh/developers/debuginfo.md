---
displayed_sidebar: "Chinese"
---

# 使用 debuginfo 文件进行调试

为了降低 StarRocks 二进制包的空间占用，自 2.5 版本开始，我们将 StarRocks BE 二进制中的调试信息 debuginfo 文件单独分离并打包提供下载。

对于普通用户，当前改动对日常使用没有任何影响，您依旧可以只下载二进制包来进行部署和升级操作。debuginfo 包仅适用于开发者使用 GDB 对程序进行调试时使用。您可以在[StarRocks 官网](https://www.starrocks.io/download/community)下载对应版本的 debuginfo 包。

![debuginfo](../assets/debug_info.png)

## 注意事项

推荐使用 GDB 12.1 及以上版本进行调试。

## 使用方法

1. 下载并解压 debuginfo 包。

    ```SQL
    wget https://releases.starrocks.io/starrocks/StarRocks-<sr_ver>.debuginfo.tar.gz

    tar -xzvf StarRocks-<sr_ver>.debuginfo.tar.gz
    ```

    > **说明**
    >
    > 请使用 StarRocks 对应版本号替换以上命令行中的 `<sr_ver>`。

2. 在进行 GDB debug 时导入 debug info。

    - 方法一

    ```Shell
    objcopy --add-gnu-debuglink=starrocks_be.debug starrocks_be
    ```

    以上操作会自动把 debug info 和执行文件关联起来，GDB debug 时会自动关联并加载。

    - 方法二

    ```Shell
    gdb -s starrocks_be.debug -e starrocks_be -c `core_file`
    ```

对于 perf 和 pstack，您无需额外操作即可直接使用。
