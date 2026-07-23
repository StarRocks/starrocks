---
displayed_sidebar: docs
description: "自 v3.4.0 起，StarRocks 支持创建 Python UDF，目前仅支持标量 UDF。"
sidebar_position: 0.91
---

import Experimental from '../../_assets/commonMarkdown/_experimental.mdx'

# Python UDF

<Experimental />

本文介绍如何使用 Python 开发用户自定义函数（UDF）。

自 v3.4.0 起，StarRocks 支持创建 Python UDF。

目前，StarRocks 仅支持 Python 中的标量 UDF。

## 前提条件

在继续之前，请确保满足以下要求：

- 安装 [Python 3.8](https://www.python.org/downloads/release/python-380/) 或更高版本。
- 在 StarRocks 中通过在 FE 配置文件 **fe/conf/fe.conf** 中设置 `enable_udf` 为 `true` 来启用 UDF，然后重启 FE 节点以使配置生效。更多信息请参见 [FE configuration - enable_udf](../../administration/management/FE_configuration.md#enable_udf)。
- Python UDF 需要以下包： pyarrow
- 使用环境变量在 BE 实例中设置 Python 解释器环境的位置。添加变量项 `python_envs` 并将其设置为 Python 解释器安装的位置，例如 `/opt/Python-3.8/`。

## 开发和使用 Python UDF

语法：

```SQL
CREATE [GLOBAL] FUNCTION function_name(arg_type [, ...])
RETURNS return_type
{PROPERTIES ("key" = "value" [, ...]) | key="value" [...] }
[AS $$ $$]
```

| **参数**        | **必需** | **描述**                                                                 |
| ------------- | -------- | ------------------------------------------------------------------------ |
| GLOBAL        | 否       | 是否创建全局 UDF。                                                        |
| function_name | 是       | 要创建的函数名称。您可以在此参数中包含数据库名称，例如 `db1.my_func`。如果 `function_name` 包含数据库名称，则在该数据库中创建 UDF。否则，在当前数据库中创建 UDF。新函数的名称及其参数不能与目标数据库中现有名称相同，否则无法创建该函数。如果函数名称相同但参数不同，则创建成功。 |
| arg_type      | 是       | 函数的参数类型。添加的参数可以用 `, ...` 表示。有关支持的数据类型，请参见 [SQL 数据类型与 Python 数据类型的映射](#sql-数据类型与-python-数据类型的映射)。 |
| return_type   | 是       | 函数的返回类型。有关支持的数据类型，请参见 [SQL 数据类型与 Python 数据类型的映射](#sql-数据类型与-python-数据类型的映射)。 |
| PROPERTIES    | 是       | 函数的属性，具体取决于要创建的 UDF 类型。                               |
| AS $$ $$      | 否       | 在 `$$` 标记之间指定内联 UDF 代码。                                      |

属性包括：

| **属性**  | **必需** | **描述**                                                              |
| --------- | -------- | -------------------------------------------------------------------- |
| type      | 是       | UDF 的类型。将其设置为 `Python` 表示创建基于 Python 的 UDF。          |
| symbol    | 是       | UDF 所属的 Python 项目的类名。此参数的值格式为 `<package_name>.<class_name>`。 |
| input     | 否       | 输入类型。有效值：`scalar`（默认）和 `arrow`（矢量化输入）。         |
| file      | 否       | 可从中下载包含 UDF 代码的 Python 包文件的 HTTP URL。此参数的值格式为 `http://<http_server_ip>:<http_server_port>/<python_package_name>`。注意包名必须以 `.py.zip` 结尾。默认值：`inline`，表示创建内联 UDF。 |
| service_url | 否     | 运行 UDF 的外部 Arrow Flight worker 服务的 URL，例如 `grpc+tcp://<host>:<port>`。设置后，BE 将连接该 worker 服务，而不是在本地启动 Python worker，此时需由您自行运行和隔离该 worker 服务。不设置（默认）则使用内置的本地 worker。参见[使用外部 worker 服务](#使用外部-worker-服务)。 |

### 使用 Python 创建内联标量输入 UDF

以下示例使用 Python 创建一个具有标量输入的内联 `echo` 函数。

```SQL
CREATE FUNCTION python_echo(INT)
RETURNS INT
type = 'Python'
symbol = 'echo'
file = 'inline'
AS
$$
def echo(x):
    return x
$$
;
```

### 使用 Python 创建内联矢量化输入 UDF

支持矢量化输入以增强 UDF 处理性能。

以下示例使用 Python 创建一个具有矢量化输入的内联 `add` 函数。

```SQL
CREATE FUNCTION python_add(INT) 
RETURNS INT
type = 'Python'
symbol = 'add'
input = "arrow"
AS
$$
import pyarrow.compute as pc
def add(x):
    return pc.add(x, 1)
$$
;
```

### 使用 Python 创建打包的 UDF

在创建 Python 包时，必须将模块打包成符合 [zipimport 格式](https://docs.python.org/3/library/zipimport.html) 的 `.py.zip` 文件。

```Plain
> tree .
.
├── main.py
└── yaml
    ├── composer.py
    ├── constructor.py
    ├── cyaml.py
    ├── dumper.py
    ├── emitter.py
    ├── error.py
    ├── events.py
    ├── __init__.py
    ├── loader.py
    ├── nodes.py
    ├── parser.py
```

```Plain
> cat main.py 
import numpy
import yaml

def echo(a):
    return yaml.__version__
```

```SQL
CREATE FUNCTION py_pack(string) 
RETURNS  string 
symbol = "add"
type = "Python"
file = "http://HTTP_IP:HTTP_PORT/m1.py.zip"
symbol = "main.echo"
;
```

### 使用外部 worker 服务

默认情况下,BE 会启动并管理一个本地 Python worker 进程来运行 Python UDF。您也可以自行以外部 Arrow Flight 服务的形式运行 worker,并通过设置 `service_url` 属性让 BE 连接到它。这样您就可以独立于 BE 运行和隔离该 worker(例如放在一个单独的、带沙箱的容器中)。

设置 `service_url` 后:

- BE 连接到该 URL 的 worker,而不是在本地启动 worker。
- 对于内联 UDF,函数代码通过连接发送给 worker。
- 对于打包(`file`)UDF,worker 会自行从 `file` URL 下载包,并根据函数的 checksum 校验,因此该 worker 服务必须能访问到该 URL。
- 运行、扩缩容、安全加固以及隔离该 worker 服务由您负责。连接默认为明文,请自行限制到 worker 的网络访问,并按需启用 TLS/鉴权。
- 为避免 worker 不可达或卡住时查询一直挂起,可设置 BE 配置项 `python_udf_rpc_timeout_ms`(单位:毫秒)来限制对 worker 的调用。该配置对本地和外部 worker 均生效,默认值为 `0`(不超时)。由于它限制的是整个调用流的时长,请将其设置为大于最长的 UDF 查询耗时。

以下示例创建一个运行在外部 worker 服务上的内联 UDF:

```SQL
CREATE FUNCTION py_echo(string)
RETURNS string
type = "Python"
symbol = "echo"
service_url = "grpc+tcp://192.168.1.10:8815"
AS
$$
def echo(x):
    return x
$$
;
```

## 沙箱化 Python UDF 执行

默认情况下,Python UDF Worker **没有任何运行时隔离**:它是 BE 拉起的一个 `python3` 进程,以 BE 所属的操作系统用户身份执行 UDF 代码,拥有完整的文件系统和网络访问权限。有效的安全边界就是"谁有权创建/运行函数",因此应把 `CREATE FUNCTION` / `CREATE GLOBAL FUNCTION` 权限视同"以 BE 操作系统用户身份运行任意代码"来授予。

如需限制 UDF 代码的能力,可通过 BE 配置项 `python_udf_sandbox` 开启沙箱:

| 取值 | 隔离能力 | 前提 |
| --- | --- | --- |
| `off`(默认) | 无 | — |
| `seccomp` | 通过 nsjail 施加 seccomp 系统调用过滤(拦截 `ptrace`、`mount`、内核模块加载等),不使用 namespace | 无需额外权限,任意容器可用 |
| `nsjail` | 完整的 Linux namespace 隔离(mount / network / PID / user namespace):最小只读文件系统视图、无外网、映射到非特权 UID | 宿主须允许所选的 namespace 模式(见下) |

沙箱是**部署侧**的控制项,在 `be.conf` 中设置,UDF 作者或查询都无法更改。隔离策略本身位于运维可编辑的配置文件中(`conf/pyudf-nsjail.conf`、`conf/pyudf-nsjail-seccomp.conf`、`conf/pyudf.kafel`),BE 仅在启动时把部署路径渲染进去。

### 示例:开启沙箱

1. 在每个 BE 的 `be.conf` 中加入(需要一个装了 `pyarrow` 的 Python 环境,见[前提条件](#前提条件)):

   ```Properties
   # 一个 Python 环境(使用其 bin/python3),需已安装 pyarrow
   python_envs = /usr

   # 沙箱级别:off | seccomp | nsjail
   python_udf_sandbox = nsjail
   # nsjail 的 namespace 策略:auto | rootless | privileged
   python_udf_sandbox_mode = auto
   # 为 true 时,无法建立所请求的沙箱则拒绝运行 UDF
   python_udf_sandbox_required = false
   ```

2. 重启 BE,然后照常创建并运行 UDF:

   ```SQL
   CREATE FUNCTION py_add(INT, INT)
   RETURNS INT
   type = 'Python'
   symbol = 'add'
   file = 'inline'
   AS
   $$
   def add(a, b):
       return a + b
   $$
   ;

   SELECT py_add(1, 2);   -- 返回 3,在沙箱内计算
   ```

BE 会记录为 Worker 选定的沙箱级别;在 BE 日志中搜索 `python UDF nsjail sandbox` 即可看到。

### namespace 模式与部署前提

`python_udf_sandbox_mode` 仅在 `python_udf_sandbox = nsjail` 时生效:

- `auto`(默认):宿主允许时使用免特权 user namespace;否则若 BE 拥有 `CAP_SYS_ADMIN` 则使用之;都不行则降级为仅 `seccomp`(除非 `python_udf_sandbox_required = true`)。
- `rootless`:强制使用免特权 user namespace。要求宿主允许免特权 user namespace,**且** BE 容器的 seccomp/AppArmor 放行 `unshare`、`mount` 系统调用。
- `privileged`:强制使用宿主 `CAP_SYS_ADMIN`(不建 user namespace)。

在 Kubernetes 上,容器默认的 seccomp 与 AppArmor profile 会拦掉建 namespace / mount 的系统调用,因此 `nsjail` 模式需要放宽 BE Pod 的这两项。对于 rootless 路径,让 BE 容器以 unconfined 的 seccomp/AppArmor 运行,且节点允许免特权 user namespace:

```yaml
securityContext:
  seccompProfile:
    type: Unconfined
  # 另需该容器 AppArmor 'unconfined',以及节点上
  # kernel.unprivileged_userns_clone=1(且无 AppArmor 限制)
```

若环境无法允许 user namespace,请使用 `python_udf_sandbox = seccomp`(无需额外权限):它过滤系统调用,但**不**隔离文件系统和网络——这两者请依赖容器自身的手段(例如 Kubernetes `NetworkPolicy`)。

:::note
`nsjail`/`seccomp` 级别都需要 `python_udf_nsjail_path` 指向的 `nsjail` 二进制(默认 `${STARROCKS_HOME}/lib/nsjail`)。若缺失且 `python_udf_sandbox_required = false`,Worker 将在无沙箱下运行并记录一条告警。
:::

## 附录

### SQL 数据类型与 Python 数据类型的映射

| SQL 类型                             | Python 3 类型           |
| ------------------------------------ | ----------------------- |
| **SCALAR**                           |                         |
| TINYINT/SMALLINT/INT/BIGINT/LARGEINT | INT                     |
| STRING                               | string                  |
| DOUBLE                               | FLOAT                   |
| BOOLEAN                              | BOOL                    |
| DATETIME                             | DATETIME.DATETIME       |
| FLOAT                                | FLOAT                   |
| CHAR                                 | STRING                  |
| VARCHAR                              | STRING                  |
| DATE                                 | DATETIME.DATE           |
| DECIMAL                              | DECIMAL.DECIMAL         |
| ARRAY                                | list                    |
| MAP                                  | dict                    |
| STRUCT                               | dict（按字段名作为键）     |
| JSON                                 | str（使用 `json.loads` 解析） |
| **VECTORIZED**                       |                         |
| TYPE_BOOLEAN                         | pyarrow.lib.BoolArray   |
| TYPE_TINYINT                         | pyarrow.lib.Int8Array   |
| TYPE_SMALLINT                        | pyarrow.lib.Int15Array  |
| TYPE_INT                             | pyarrow.lib.Int32Array  |
| TYPE_BIGINT                          | pyarrow.lib.Int64Array  |
| TYPE_FLOAT                           | pyarrow.FloatArray      |
| TYPE_DOUBLE                          | pyarrow.DoubleArray     |
| VARCHAR                              | pyarrow.StringArray     |
| DECIMAL                              | pyarrow.Decimal128Array |
| DATE                                 | pyarrow.Date32Array     |
| TYPE_TIME                            | pyarrow.TimeArray       |
| ARRAY                                | pyarrow.ListArray       |
| MAP                                  | pyarrow.MapArray        |
| STRUCT                               | pyarrow.StructArray     |

嵌套类型（`ARRAY`、`MAP`、`STRUCT`）可任意组合，例如
`array<map<string,int>>`、`map<string,array<int>>`、
`struct<a array<int>, b map<string,int>>` 或 `array<struct<...>>`。在 `scalar`
模式下，每一行会被递归地转换为 Python `list` / `dict`。返回嵌套类型时，返回相应
的 Python `list` / `dict`（`dict` 的键即为 struct 字段名）即可，`pyarrow` 会
自动完成递归转换。

### 编译 Python

按照以下步骤编译 Python：

1. 获取 OpenSSL 包。

   ```Bash
   wget 'https://github.com/openssl/openssl/archive/OpenSSL_1_1_1m.tar.gz'
   ```

2. 解压缩包。

   ```Bash
   tar -zxf OpenSSL_1_1_1m.tar.gz
   ```

3. 进入解压后的文件夹。

   ```Bash
   cd openssl-OpenSSL_1_1_1m
   ```

4. 设置环境变量 `OPENSSL_DIR`。

   ```Bash
   export OPENSSL_DIR=`pwd`/install
   ```

5. 准备源代码以进行编译。

   ```Bash
   ./Configure --prefix=`pwd`/install
   ./config --prefix=`pwd`/install
   ```

6. 编译 OpenSSL。

   ```Bash
   make -j 16 && make install
   ```

7. 设置环境变量 `LD_LIBRARY_PATH`。

   ```Bash
   LD_LIBRARY_PATH=$OPENSSL_DIR/lib:$LD_LIBRARY_PATH
   ```

8. 返回工作目录并获取 Python 包。

   ```Bash
   wget 'https://www.python.org/ftp/python/3.12.9/Python-3.12.9.tgz'
   ```

9. 解压缩包。

   ```Bash
   tar -zxf ./Python-3.12.9.tgz 
   ```

10. 进入解压后的文件夹。

   ```Bash
   cd Python-3.12.9
   ```

11. 创建并进入目录 `build`。

   ```Bash
   mkdir build && cd build
   ```

12. 准备源代码以进行编译。

   ```Bash
   ../configure --prefix=`pwd`/install --with-openssl=$OPENSSL_DIR
   ```

13. 编译 Python。

   ```Bash
   make -j 16 && make install
   ```

14. 安装 PyArrow 和 grpcio。

   ```Bash
   ./install/bin/pip3 install pyarrow grpcio
   ```

15. 将文件压缩到包中。

   ```Bash
   tar -zcf ./Python-3.12.9.tar.gz install
   ```

16. 将包分发到目标 BE 服务器，并解压缩包。

   ```Bash
   tar -zxf ./Python-3.12.9.tar.gz
   ```

17. 修改 BE 配置文件 **be.conf**，添加以下配置项。

   ```Properties
   python_envs=/home/disk1/sr/install
   ```
