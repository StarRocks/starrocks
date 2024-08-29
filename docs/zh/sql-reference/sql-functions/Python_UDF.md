---
displayed_sidebar: docs
sidebar_position: 0.9
---

# Python UDF

自 3.4.0 版本起，StarRocks 支持使用 Python 语言编写用户定义函数（User Defined Function，简称 UDF）。

本文介绍如何编写和使用 StarRocks Python UDF。

目前 StarRocks Python UDF 仅支持用户自定义标量函数（Scalar UDF）。

## 前提条件

使用 StarRocks 的 Python UDF 功能前，您需要:

- [安装 Python3.8+](https://www.python.org/downloads/release/python-380/) 以运行Python。
- 开启 UDF 功能。在 FE 配置文件 **fe/conf/fe.conf** 中设置配置项 `enable_udf` 为 `true`，并重启 FE 节点使配置项生效。详细操作以及配置项列表参考[配置参数](../../administration/management/FE_configuration.md)。
- BE 设置Python解释器环境变量位置。添加配置项 `python_envs` 设置为 Python解释器安装位置例如 `/opt/Python-3.8/`

## 开发并使用 Python UDF
### 开发 Scalar UDF
#### 语法

```SQL
CREATE [GLOBAL] FUNCTION function_name(arg_type [, ...])
RETURNS return_type
[PROPERTIES ("key" = "value" [, ...]) | key="value" [...] ]
[AS $$ $$]
```
#### 创建 Python inline Scalar input UDF 
echo 示例

```SQL
CREATE FUNCTION python_echo(INT) RETURNS
INT
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

|参数|描述|
|---|----|
|symbol|UDF 执行函数。|
|type|用于标记所创建的 UDF 类型，在Python UDF中。取值为 `Python`，表示基于 Python 的 UDF。|
|input|输入类型，取值为"scalar"和"arrow", 默认值为 "scalar"|
#### 创建 Python inline vectorized input UDF 

为了提升UDF处理速度，提供了vectorized input

```SQL
CREATE FUNCTION python_add(INT) RETURNS
INT
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
#### 创建 Python packaged input UDF 
打包创建:
首先把module 打包到 xxx.zip，需要满足 [zipimport 格式](https://docs.python.org/3/library/zipimport.html) 
```
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
```
> cat main.py 
import numpy
import yaml

def echo(a):
    return yaml.__version__
```

```
CREATE FUNCTION py_pack(string) 
RETURNS  string 
symbol = "add"
type = "Python"
file = "http://HTTP_IP:HTTP_PORT/m1.py.zip"
symbol = "main.echo"
;
```
注意URL这里一定是要以 .py.zip 结尾

## 类型映射关系

| SQL Type                             | Python 3 Type           |
| ------------------------------------ | ----------------------- |
| SCALAR:                              |                         |
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
| VECTORIZED:                          |                         |
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