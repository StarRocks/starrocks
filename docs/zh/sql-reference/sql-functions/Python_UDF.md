---
displayed_sidebar: docs
sidebar_position: 0.91
---

# Python UDF

本文介绍如何使用 Python 语言编写用户定义函数（User Defined Function，简称 UDF）。

自 3.4.0 版本起，StarRocks 支持使用 Python UDF。

目前 StarRocks 仅支持基于 Python 创建用户自定义标量函数（Scalar UDF）。

## 前提条件

在开始之前，请确保满足以下要求：

- 安装 [Python 3.8](https://www.python.org/downloads/release/python-380/) 或以上版本。
- 开启 UDF 功能。在 FE 配置文件 **fe/conf/fe.conf** 中设置配置项 `enable_udf` 为 `true`，并重启 FE 节点使配置项生效。详细信息，参考 [FE 参数 - enable_udf](../../administration/management/FE_configuration.md#enable_udf)。
- 使用环境变量在 BE 实例中设置 Python 解释器环境的位置。添加变量项 `python_envs`，并将其设置为 Python 解释器的安装位置，例如 `/opt/Python-3.8/`。

## 开发并使用 Python UDF

语法：

```SQL
CREATE [GLOBAL] FUNCTION function_name(arg_type [, ...])
RETURNS return_type
{PROPERTIES ("key" = "value" [, ...]) | key="value" [...] }
[AS $$ $$]
```

| **参数**      | **必选** | **说明**                                                     |
| ------------- | -------- | -----------------------------------------------------------|
| GLOBAL        | 否       | 如需创建全局 UDF，需指定该关键字。                              |
| function_name | 是       | 函数名，可以包含数据库名称，比如，`db1.my_func`。如果 `function_name` 中包含了数据库名称，那么该 UDF 会创建在对应的数据库中，否则该 UDF 会创建在当前数据库。新函数名和参数不能与目标数据库中已有的函数相同，否则会创建失败；如只有函数名相同，参数不同，则可以创建成功。 |
| arg_type      | 是       | 函数的参数类型。具体支持的数据类型，请参见[类型映射关系](#类型映射关系)。 |
| return_type   | 是       | 函数的返回值类型。具体支持的数据类型，请参见[类型映射关系](#类型映射关系)。 |
| PROPERTIES    | 是       | 函数相关属性。创建不同类型的 UDF 需配置不同的属性，详情和示例请参考以下示例。 |
| AS $$ $$      | 否       | 在 `$$` 标记之间指定内联 UDF 代码。                            |

Properties 包括以下参数：

| **Property**  | **必选** | **说明**                                                      |
| ------------- | ------- | ------------------------------------------------------------- |
| type          | 是      | 用于标记所创建的 UDF 类型。取值为 `Python`，表示基于 Python 的 UDF。 |
| symbol        | 是      | UDF 所在项目的类名。格式为 `<package_name>.<class_name>`。        |
| input         | 否      | 输入类型。有效值：`scalar`（默认）和 `arrow` (向量输入)。                     |
| file          | 否      | UDF 所在 Python 包的 HTTP 路径。格式为 `http://<http_server_ip>:<http_server_port>/<jar_package_name>`。注意 Python 包名必须以 `.py.zip` 结尾。默认值：`inline`，表示创建内联 UDF。 |

### 创建标量输入的内联函数

以下示例使用 Python 创建带有标量输入的内联函数 `echo`。

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

#### 创建向量输入的内联函数

为了提升 UDF 处理速度，支持向量输入。

以下示例使用 Python 创建带有向量输入的内联函数 `add`。

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

#### 创建封装函数

创建 Python 包时，必须将模块打包成 `.py.zip` 文件，需要满足 [zipimport 格式](https://docs.python.org/3/library/zipimport.html)。

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

## 类型映射关系

| SQL Type                             | Python 3 Type           |
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
| ARRAY                                | List                    |
| MAP                                  | Dict                    |
| STRUCT                               | COLLECTIONS.NAMEDTUPLE  |
| JSON                                 | dict                    |
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