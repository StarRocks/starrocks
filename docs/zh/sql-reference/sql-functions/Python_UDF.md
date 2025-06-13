---
displayed_sidebar: docs
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
| arg_type      | 是       | 函数的参数类型。添加的参数可以用 `, ...` 表示。有关支持的数据类型，请参见 [Mapping between SQL data types and Python data types](#mapping-between-sql-data-types-and-python-data-types)。 |
| return_type   | 是       | 函数的返回类型。有关支持的数据类型，请参见 [Mapping between SQL data types and Python data types](#mapping-between-sql-data-types-and-python-data-types)。 |
| PROPERTIES    | 是       | 函数的属性，具体取决于要创建的 UDF 类型。                               |
| AS $$ $$      | 否       | 在 `$$` 标记之间指定内联 UDF 代码。                                      |

属性包括：

| **属性**  | **必需** | **描述**                                                              |
| --------- | -------- | -------------------------------------------------------------------- |
| type      | 是       | UDF 的类型。将其设置为 `Python` 表示创建基于 Python 的 UDF。          |
| symbol    | 是       | UDF 所属的 Python 项目的类名。此参数的值格式为 `<package_name>.<class_name>`。 |
| input     | 否       | 输入类型。有效值：`scalar`（默认）和 `arrow`（矢量化输入）。         |
| file      | 否       | 可从中下载包含 UDF 代码的 Python 包文件的 HTTP URL。此参数的值格式为 `http://<http_server_ip>:<http_server_port>/<python_package_name>`。注意包名必须以 `.py.zip` 结尾。默认值：`inline`，表示创建内联 UDF。 |

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

## SQL 数据类型与 Python 数据类型之间的映射

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